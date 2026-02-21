"""
Service Layer para Ingestão de Dados.

Deduplicação:
  - Transacional: protocolos existentes no banco são carregados antes da inserção (Python-level)
  - Voalle: registros existentes (colaborador_id + data) carregados antes da inserção (Python-level)
  - Hash SHA-256 do arquivo impede re-upload do mesmo arquivo idêntico
"""

from typing import Dict, Any, Set, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from src.infrastructure.database import models
from src.application.dto.ingestion_schema import (
    AtendimentoTransacionalImportSchema,
    VoalleAgregadoImportSchema,
)
from src.shared.utils.name_resolver import resolver_nome, is_sac
from datetime import date


class IngestionService:
    def __init__(self, db: Session):
        self.db = db

    # =====================================================
    # HELPERS DE DIMENSÕES
    # =====================================================

    def _build_dim_cache(self) -> Dict[str, Any]:
        return {
            "colaboradores": {
                resolver_nome(str(c.nome)): c
                for c in self.db.query(models.DimColaborador).all()
            },
            "canais": {
                str(c.nome): c for c in self.db.query(models.DimCanal).all()
            },
            "status": {
                str(s.nome): s for s in self.db.query(models.DimStatus).all()
            },
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
                cache["colaboradores"][resolver_nome(str(c.nome))] = c

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
                func.count(models.FatoAtendimento.id).label("total"),
            )
            .filter(models.FatoAtendimento.colaborador_id.in_(colaborador_ids))
            .group_by(
                models.FatoAtendimento.colaborador_id,
                models.FatoAtendimento.turno,
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
    # DEDUPLICAÇÃO
    # =====================================================

    def carregar_protocolos_existentes(self, protocolos: list[str]) -> Set[str]:
        """
        Consulta protocolos no banco em lotes de 500.
        Retorna set dos que já existem.
        """
        existentes: Set[str] = set()
        if not protocolos:
            return existentes

        protocolos_limpos = [p for p in protocolos if p and p.strip()]
        if not protocolos_limpos:
            return existentes

        BATCH = 500
        for i in range(0, len(protocolos_limpos), BATCH):
            lote = protocolos_limpos[i : i + BATCH]
            rows = (
                self.db.query(models.FatoAtendimento.protocolo)
                .filter(models.FatoAtendimento.protocolo.in_(lote))
                .all()
            )
            existentes.update(r[0] for r in rows if r[0])

        return existentes

    def carregar_voalle_existentes(self, data_ref: date) -> Set[int]:
        """
        Retorna set de colaborador_ids que já têm registro no Voalle
        para a data_referencia informada.
        """
        rows = (
            self.db.query(models.FatoVoalleDiario.colaborador_id)
            .filter(models.FatoVoalleDiario.data_referencia == data_ref)
            .all()
        )
        return {r[0] for r in rows}

    def verificar_hash_duplicado(self, file_hash: str) -> bool:
        """Retorna True se já existe um upload com sucesso/warning para este hash."""
        existe = (
            self.db.query(models.Upload.id)
            .filter(
                models.Upload.file_hash == file_hash,
                models.Upload.status.in_(["success", "warning"]),
            )
            .first()
        )
        return existe is not None

    # =====================================================
    # BATCH TRANSACIONAL (Ligações + Omnichannel)
    # =====================================================

    def process_transacional_batch(
        self,
        registros: list[AtendimentoTransacionalImportSchema],
        protocolos_existentes_banco: Set[str] | None = None,
    ) -> dict:

        success_count = 0
        error_count = 0
        duplicate_count = 0
        erros: list[str] = []
        nomes_sem_match: list[str] = []

        protocolos_no_banco: Set[str] = protocolos_existentes_banco or set()

        cache = self._build_dim_cache()
        new_colaboradores: list[dict] = []
        new_canais: list[dict] = []
        new_status: list[dict] = []

        for data in registros:
            nome_key = resolver_nome(data.colaborador_nome)

            if nome_key not in cache["colaboradores"]:
                if data.colaborador_nome not in nomes_sem_match:
                    nomes_sem_match.append(data.colaborador_nome)
                if not any(resolver_nome(r["nome"]) == nome_key for r in new_colaboradores):
                    new_colaboradores.append({
                        "nome": nome_key.title(),
                        "equipe": data.equipe or "SAC",
                    })

            if data.canal_nome not in cache["canais"]:
                if not any(r["nome"] == data.canal_nome for r in new_canais):
                    new_canais.append({"nome": data.canal_nome})

            if data.status_nome not in cache["status"]:
                if not any(r["nome"] == data.status_nome for r in new_status):
                    new_status.append({"nome": data.status_nome})

        self._flush_new_dims(cache, new_colaboradores, new_canais, new_status)

        fatos_para_inserir: list[dict] = []
        colaborador_ids_afetados: set[int] = set()

        for i, data in enumerate(registros, start=1):
            try:
                # ── Deduplicação contra o banco (Python-level) ──
                if data.protocolo and data.protocolo in protocolos_no_banco:
                    duplicate_count += 1
                    continue

                colaborador = cache["colaboradores"].get(resolver_nome(data.colaborador_nome))
                canal = cache["canais"].get(data.canal_nome)
                status = cache["status"].get(data.status_nome)

                if not colaborador or not canal or not status:
                    raise ValueError("Dimensão não encontrada no cache após inserção.")

                colab_id = int(colaborador.id)
                canal_id = int(canal.id)
                status_id = int(status.id)

                fatos_para_inserir.append({
                    "data_referencia": data.data_referencia,
                    "turno": data.turno,
                    "protocolo": data.protocolo,
                    "sentido_interacao": data.sentido_interacao,
                    "tempo_espera_segundos": data.tempo_espera_segundos,
                    "tempo_atendimento_segundos": data.tempo_atendimento_segundos,
                    "nota_solucao": data.nota_solucao,
                    "nota_atendimento": data.nota_atendimento,
                    "colaborador_id": colab_id,
                    "canal_id": canal_id,
                    "status_id": status_id,
                })

                colaborador_ids_afetados.add(colab_id)
                success_count += 1

            except Exception as e:
                error_count += 1
                erros.append(f"Linha {i}: {str(e)}")

        try:
            if fatos_para_inserir:
                # INSERT simples — dedup já foi feita em Python
                self.db.execute(
                    insert(models.FatoAtendimento).values(fatos_para_inserir)
                )
            self._atualizar_turno_colaboradores(list(colaborador_ids_afetados))
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise RuntimeError(f"Erro ao inserir lote no banco de dados.") from e

        return {
            "success_count": success_count,
            "error_count": error_count,
            "duplicate_count": duplicate_count,
            "errors": erros[:10],
            "nomes_sem_match": nomes_sem_match,
        }

    # =====================================================
    # BATCH VOALLE (Agregado)
    # Dedup feita em Python: carrega colaborador_ids existentes
    # para a data, e ignora quem já está no banco.
    # =====================================================

    def process_voalle_batch(
        self,
        registros: list[VoalleAgregadoImportSchema],
        voalle_existentes: Set[int] | None = None,
    ) -> dict:

        success_count = 0
        error_count = 0
        duplicate_count = 0
        ignorados_count = 0
        erros: list[str] = []
        nomes_ignorados: list[str] = []

        # Set de colaborador_ids que já têm registro para esta data
        ids_ja_no_banco: Set[int] = voalle_existentes or set()

        cache_colaboradores: Dict[str, Any] = {
            resolver_nome(str(c.nome)): c
            for c in self.db.query(models.DimColaborador)
                            .filter(models.DimColaborador.equipe == "SAC")
                            .all()
        }

        fatos_para_inserir: list[dict] = []

        for i, data in enumerate(registros, start=1):
            nome_raw: str = data.colaborador_nome

            if any(skip in nome_raw.upper() for skip in ("SYNTESIS", "TOTAL GERAL", "OLIVIA BOT")):
                ignorados_count += 1
                continue

            if not is_sac(nome_raw):
                nomes_ignorados.append(nome_raw)
                ignorados_count += 1
                continue

            nome_key = resolver_nome(nome_raw)
            colaborador = cache_colaboradores.get(nome_key)

            if not colaborador:
                try:
                    self.db.execute(
                        insert(models.DimColaborador)
                        .values({"nome": nome_key.title(), "equipe": "SAC"})
                        .on_conflict_do_nothing()
                    )
                    self.db.flush()
                    colaborador = (
                        self.db.query(models.DimColaborador)
                        .filter(models.DimColaborador.nome == nome_key.title())
                        .first()
                    )
                    if colaborador:
                        cache_colaboradores[nome_key] = colaborador
                except Exception as e:
                    error_count += 1
                    erros.append(f"Linha {i} (criar colaborador): {str(e)}")
                    continue

            if not colaborador:
                error_count += 1
                erros.append(f"Linha {i}: colaborador não pode ser criado.")
                continue

            # ── Deduplicação Voalle (Python-level) ──
            # Se este colaborador já tem dados para esta data, pula
            colab_id = int(colaborador.id)  # pyright: ignore[reportArgumentType]
            if colab_id in ids_ja_no_banco:
                duplicate_count += 1
                continue

            try:
                fatos_para_inserir.append({
                    "data_referencia": data.data_referencia,
                    "clientes_atendidos": data.clientes_atendidos,
                    "numero_atendimentos": data.numero_atendimentos,
                    "solicitacao_finalizada": data.solicitacao_finalizada,
                    "colaborador_id": colab_id,
                })
                # Marca como "visto" para não duplicar dentro do mesmo batch
                ids_ja_no_banco.add(colab_id)
                success_count += 1

            except Exception as e:
                error_count += 1
                erros.append(f"Linha {i}: {str(e)}")

        try:
            if fatos_para_inserir:
                # INSERT simples — dedup já foi feita em Python
                self.db.execute(
                    insert(models.FatoVoalleDiario).values(fatos_para_inserir)
                )
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise RuntimeError(f"Erro ao inserir lote Voalle no banco de dados.") from e

        return {
            "success_count": success_count,
            "error_count": error_count,
            "duplicate_count": duplicate_count,
            "ignorados_count": ignorados_count,
            "errors": erros[:10],
            "nomes_ignorados": nomes_ignorados,
        }