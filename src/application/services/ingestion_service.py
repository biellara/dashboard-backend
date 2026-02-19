"""
Service Layer para Ingestão de Dados — versão otimizada.

Novidades:
- Campo turno salvo em cada fato_atendimento (calculado pelo horário)
- Turno predominante do colaborador atualizado automaticamente após cada batch
- Filtro SAC já aplicado no controller antes de chegar aqui
- Cache de colaboradores usa chave normalizada (sem acento, maiúsculo)
  para garantir que variações do mesmo nome não criem registros duplicados
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


def normalizar_nome(nome: str) -> str:
    """
    Chave canônica de busca: sem acentos, maiúsculo, espaços colapsados.
    Deve ser idêntica à função homônima no ingestion_controller.py.
    """
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
        return {
            # Chave normalizada: garante que "Ana Carolina" e "ANA CAROLINA"
            # apontem para o mesmo colaborador_id no banco.
            "colaboradores": {
                normalizar_nome(str(c.nome)): c
                for c in self.db.query(models.DimColaborador).all()
            },
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
                # Indexar com chave normalizada para consistência com _build_dim_cache
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
        """
        Para cada colaborador_id informado, calcula o turno predominante
        (o turno com mais atendimentos no histórico) e atualiza dim_colaboradores.
        Executado em lote após cada commit.
        """
        if not colaborador_ids:
            return

        # Conta atendimentos por colaborador e turno
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

        # Agrupa por colaborador e pega o turno de maior contagem
        turno_por_colaborador: dict[int, tuple[str, int]] = {}
        for col_id, turno, total in resultado:
            atual = turno_por_colaborador.get(col_id)
            if atual is None or total > atual[1]:
                turno_por_colaborador[col_id] = (turno, total)

        # Atualiza em lote
        for col_id, (turno, _) in turno_por_colaborador.items():
            self.db.query(models.DimColaborador).filter(
                models.DimColaborador.id == col_id
            ).update({models.DimColaborador.turno: turno}, synchronize_session=False)

    # =====================================================
    # BATCH TRANSACIONAL (Ligações + Omnichannel)
    # =====================================================

    def process_transacional_batch(
        self,
        registros: list[AtendimentoTransacionalImportSchema]
    ) -> dict:

        success_count = 0
        error_count = 0
        erros = []

        cache = self._build_dim_cache()

        new_colaboradores = []
        new_canais = []
        new_status = []

        for data in registros:
            if data.colaborador_nome not in cache["colaboradores"]:
                if not any(r["nome"] == data.colaborador_nome for r in new_colaboradores):
                    new_colaboradores.append({"nome": data.colaborador_nome, "equipe": data.equipe})

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
                colaborador = cache["colaboradores"].get(data.colaborador_nome)
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

            # Atualiza turno predominante dos colaboradores afetados
            self._atualizar_turno_colaboradores(list(colaborador_ids_afetados))

            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise Exception(f"Erro ao inserir lote: {str(e)}")

        return {"success_count": success_count, "error_count": error_count, "errors": erros[:10]}

    # =====================================================
    # BATCH VOALLE (Agregado)
    # =====================================================

    def process_voalle_batch(
        self,
        registros: list[VoalleAgregadoImportSchema]
    ) -> dict:

        success_count = 0
        error_count = 0
        erros = []

        cache = {
            "colaboradores": {
                normalizar_nome(str(c.nome)): c
                for c in self.db.query(models.DimColaborador).all()
            }
        }

        new_colaboradores = []
        for data in registros:
            if data.colaborador_nome not in cache["colaboradores"]:
                if not any(r["nome"] == data.colaborador_nome for r in new_colaboradores):
                    new_colaboradores.append({"nome": data.colaborador_nome, "equipe": None})

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

        fatos_para_inserir = []

        for i, data in enumerate(registros, start=1):
            try:
                colaborador = cache["colaboradores"].get(data.colaborador_nome)
                if not colaborador:
                    raise Exception("Colaborador não encontrado.")

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

        return {"success_count": success_count, "error_count": error_count, "errors": erros[:10]}