"""
Service Layer para Ingestão de Dados — versão otimizada.

Novidades:
- Campo turno salvo em cada fato_atendimento (calculado pelo horário)
- Turno predominante do colaborador atualizado automaticamente após cada batch
- Cache de colaboradores usa chave normalizada (sem acento, maiúsculo, sem ramal)
  para garantir que variações do mesmo nome não criem registros duplicados
- Suporte a dim_colaborador_alias: nomes alternativos de sistemas externos
  são resolvidos para o colaborador canônico antes de qualquer operação
- normalizar_nome remove ramal automáticamente (ex: "WELLINGTON - 6373" → "WELLINGTON")
- Filtro SAC:
    * Ligações/Omnichannel → filtro aplicado no ingestion_controller (fila/equipe == SAC)
    * Voalle → não tem coluna de setor; apenas registros cujo colaborador
      já exista no banco com equipe='SAC' são importados. Os demais são ignorados.
"""

from typing import Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from src.infrastructure.database import models
from src.application.dto.ingestion_schema import (
    AtendimentoTransacionalImportSchema,
    VoalleAgregadoImportSchema
)
import unicodedata
import re


def normalizar_nome(nome: str) -> str:
    """
    Chave canônica de busca. Aplica em sequência:
      1. Remove ramal — sufixo "- XXXXX" ou " XXXXX" com 4-5 dígitos no final
         Ex: "Wellington Silva de Souza - 6373" → "Wellington Silva de Souza"
             "KLEBER ALVES JARENKO- 6372"       → "KLEBER ALVES JARENKO"
      2. NFKD sem acentos
      3. Maiúsculo + espaços colapsados

    Deve ser idêntica à função homônima no ingestion_controller.py.
    """
    nome = re.sub(r'\s*-?\s*\d{4,5}\s*$', '', nome).strip()
    nfkd = unicodedata.normalize("NFKD", nome)
    sem_acento = "".join(c for c in nfkd if not unicodedata.combining(c))
    return " ".join(sem_acento.upper().split())


class IngestionService:
    def __init__(self, db: Session):
        self.db = db

    # =====================================================
    # HELPERS DE DIMENSÕES
    # =====================================================

    def _build_dim_cache(self) -> Dict[str, Dict[str, Any]]:
        """
        Constrói o cache de dimensões.

        O cache de colaboradores é indexado por chave normalizada e contempla
        duas fontes de resolução de nomes:

        1. Nome canônico: cada colaborador em dim_colaboradores.
        2. Aliases: registros em dim_colaborador_alias apontam para o
           colaborador canônico. Cobre casos como:
             - Nomes truncados:   "MARCIA REGINA VENTURA RODRIGUE" → (completo)
             - Nomes distintos:   "PLACIDO PORTAL DE SOUSA JUNIOR" → "PLACIDO JUNIOR"
        """
        cache_colaboradores: Dict[str, Any] = {
            normalizar_nome(str(c.nome)): c
            for c in self.db.query(models.DimColaborador).all()
        }

        # Aliases têm precedência — representam mapeamento explícito do admin
        for alias_obj in self.db.query(models.DimColaboradorAlias).all():
            chave = normalizar_nome(str(alias_obj.alias))
            cache_colaboradores[chave] = alias_obj.colaborador

        return {
            "colaboradores": cache_colaboradores,
            "canais": {str(c.nome): c for c in self.db.query(models.DimCanal).all()},
            "status": {str(s.nome): s for s in self.db.query(models.DimStatus).all()},
        }

    def _flush_new_dims(self, cache, new_colaboradores, new_canais, new_status):
        if new_colaboradores:
            self.db.execute(
                insert(models.DimColaborador)
                .values(new_colaboradores)
                .on_conflict_do_nothing()
            )
            self.db.flush()
            for c in self.db.query(models.DimColaborador).filter(
                models.DimColaborador.nome.in_([r["nome"] for r in new_colaboradores])
            ).all():
                cache["colaboradores"][normalizar_nome(str(c.nome))] = c

        if new_canais:
            self.db.execute(
                insert(models.DimCanal)
                .values(new_canais)
                .on_conflict_do_nothing()
            )
            self.db.flush()
            for c in self.db.query(models.DimCanal).filter(
                models.DimCanal.nome.in_([r["nome"] for r in new_canais])
            ).all():
                cache["canais"][str(c.nome)] = c

        if new_status:
            self.db.execute(
                insert(models.DimStatus)
                .values(new_status)
                .on_conflict_do_nothing()
            )
            self.db.flush()
            for s in self.db.query(models.DimStatus).filter(
                models.DimStatus.nome.in_([r["nome"] for r in new_status])
            ).all():
                cache["status"][str(s.nome)] = s

    # =====================================================
    # ATUALIZAÇÃO AUTOMÁTICA DO TURNO DO COLABORADOR
    # =====================================================

    def _atualizar_turno_colaboradores(self, colaborador_ids: list[int]):
        if not colaborador_ids:
            return

        resultado = (
            self.db.query(
                models.FatoAtendimento.colaborador_id,
                models.FatoAtendimento.turno,
                func.count(models.FatoAtendimento.id).label("total")
            )
            .filter(models.FatoAtendimento.colaborador_id.in_(colaborador_ids))
            .group_by(
                models.FatoAtendimento.colaborador_id,
                models.FatoAtendimento.turno
            )
            .all()
        )

        turno_por_colaborador: dict[int, tuple[str, int]] = {}
        for col_id, turno, total in resultado:
            atual = turno_por_colaborador.get(col_id)
            if atual is None or total > atual[1]:
                turno_por_colaborador[col_id] = (turno, total)

        for col_id, (turno, _) in turno_por_colaborador.items():
            self.db.query(models.DimColaborador).filter(
                models.DimColaborador.id == col_id
            ).update({models.DimColaborador.turno: turno}, synchronize_session=False)

    # =====================================================
    # BATCH TRANSACIONAL (Ligações + Omnichannel)
    # O filtro SAC (fila/equipe == 'SAC') é aplicado no ingestion_controller
    # antes de os registros chegarem aqui.
    # =====================================================

    def process_transacional_batch(
        self,
        registros: list[AtendimentoTransacionalImportSchema]
    ) -> dict:

        success_count = 0
        error_count = 0
        erros = []
        nomes_sem_match: list[str] = []

        cache = self._build_dim_cache()

        new_colaboradores = []
        new_canais = []
        new_status = []

        for data in registros:
            nome_key = normalizar_nome(data.colaborador_nome)

            if nome_key not in cache["colaboradores"]:
                if data.colaborador_nome not in nomes_sem_match:
                    nomes_sem_match.append(data.colaborador_nome)
                if not any(normalizar_nome(r["nome"]) == nome_key for r in new_colaboradores):
                    # Salva sem ramal e com equipe SAC já definida
                    new_colaboradores.append({"nome": nome_key.title(), "equipe": data.equipe or "SAC"})

            if data.canal_nome not in cache["canais"]:
                if not any(r["nome"] == data.canal_nome for r in new_canais):
                    new_canais.append({"nome": data.canal_nome})

            if data.status_nome not in cache["status"]:
                if not any(r["nome"] == data.status_nome for r in new_status):
                    new_status.append({"nome": data.status_nome})

        self._flush_new_dims(cache, new_colaboradores, new_canais, new_status)

        fatos_para_inserir = []
        colaborador_ids_afetados = set()

        for i, data in enumerate(registros, start=1):
            try:
                colaborador = cache["colaboradores"].get(normalizar_nome(data.colaborador_nome))
                canal = cache["canais"].get(data.canal_nome)
                status = cache["status"].get(data.status_nome)

                if not colaborador or not canal or not status:
                    raise Exception("Dimensão não encontrada no cache após inserção.")

                fatos_para_inserir.append({
                    "data_referencia": data.data_referencia,
                    "turno": data.turno,
                    "protocolo": data.protocolo,
                    "sentido_interacao": data.sentido_interacao,
                    "tempo_espera_segundos": data.tempo_espera_segundos,
                    "tempo_atendimento_segundos": data.tempo_atendimento_segundos,
                    "nota_solucao": data.nota_solucao,
                    "nota_atendimento": data.nota_atendimento,
                    "colaborador_id": colaborador.id,
                    "canal_id": canal.id,
                    "status_id": status.id,
                })

                colaborador_ids_afetados.add(colaborador.id)
                success_count += 1

            except Exception as e:
                error_count += 1
                erros.append(f"Linha {i}: {str(e)}")

        try:
            if fatos_para_inserir:
                self.db.execute(
                    insert(models.FatoAtendimento).values(fatos_para_inserir)
                )
            self._atualizar_turno_colaboradores(list(colaborador_ids_afetados))
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise Exception(f"Erro ao inserir lote: {str(e)}")

        return {
            "success_count": success_count,
            "error_count": error_count,
            "errors": erros[:10],
            "nomes_sem_match": nomes_sem_match,
        }

    # =====================================================
    # BATCH VOALLE (Agregado)
    # O Voalle não tem coluna de setor. A estratégia é:
    #   - Importar SOMENTE colaboradores já cadastrados no banco com equipe='SAC'
    #     (ou resolvíveis via alias cujo canônico seja SAC)
    #   - Ignorar silenciosamente os demais (outros setores)
    #   - Retornar em 'ignorados' os nomes pulados, para auditoria
    # =====================================================

    def process_voalle_batch(
        self,
        registros: list[VoalleAgregadoImportSchema]
    ) -> dict:

        success_count = 0
        error_count = 0
        ignorados_count = 0
        erros = []
        nomes_sem_match: list[str] = []
        nomes_ignorados: list[str] = []

        # Cache inclui apenas colaboradores SAC + aliases
        cache_colaboradores: Dict[str, Any] = {
            normalizar_nome(str(c.nome)): c
            for c in self.db.query(models.DimColaborador)
                            .filter(models.DimColaborador.equipe == "SAC")
                            .all()
        }
        # Aliases apontam para o canônico — se o canônico for SAC, o alias também é válido
        for alias_obj in self.db.query(models.DimColaboradorAlias).all():
            if alias_obj.colaborador.equipe == "SAC":
                cache_colaboradores[normalizar_nome(str(alias_obj.alias))] = alias_obj.colaborador

        fatos_para_inserir = []

        for i, data in enumerate(registros, start=1):
            nome_key = normalizar_nome(data.colaborador_nome)

            # Pula entradas de sistema (bots, totais)
            if any(skip in nome_key for skip in ("SYNTESIS", "TOTAL GERAL", "OLIVIA BOT")):
                ignorados_count += 1
                continue

            colaborador = cache_colaboradores.get(nome_key)

            if not colaborador:
                # Não encontrado no cache SAC — pode ser outro setor ou nome sem alias
                nomes_ignorados.append(data.colaborador_nome)
                ignorados_count += 1
                continue

            try:
                fatos_para_inserir.append({
                    "data_referencia": data.data_referencia,
                    "clientes_atendidos": data.clientes_atendidos,
                    "numero_atendimentos": data.numero_atendimentos,
                    "solicitacao_finalizada": data.solicitacao_finalizada,
                    "colaborador_id": colaborador.id,
                })
                success_count += 1

            except Exception as e:
                error_count += 1
                erros.append(f"Linha {i}: {str(e)}")

        try:
            if fatos_para_inserir:
                self.db.execute(
                    insert(models.FatoVoalleDiario).values(fatos_para_inserir)
                )
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise Exception(f"Erro ao inserir lote Voalle: {str(e)}")

        return {
            "success_count": success_count,
            "error_count": error_count,
            "ignorados_count": ignorados_count,
            "errors": erros[:10],
            # Nomes que não estavam no cache SAC — podem ser outros setores
            # ou SAC sem alias cadastrado. Revisar se necessário.
            "nomes_ignorados": nomes_ignorados,
        }