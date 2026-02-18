"""
Service Layer para Ingestão de Dados — versão otimizada.
"""

from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from typing import Optional
from src.infrastructure.database import models
from src.application.dto.ingestion_schema import (
    AtendimentoTransacionalImportSchema,
    VoalleAgregadoImportSchema
)


class IngestionService:
    def __init__(self, db: Session):
        self.db = db

    def _build_dim_cache(self):
        """Carrega todas as dimensões existentes em memória de uma vez."""
        return {
            "colaboradores": {c.nome: c for c in self.db.query(models.DimColaborador).all()},
            "canais": {c.nome: c for c in self.db.query(models.DimCanal).all()},
            "status": {s.nome: s for s in self.db.query(models.DimStatus).all()},
        }

    def _flush_new_dims(self, cache, new_colaboradores, new_canais, new_status):
        """
        Insere dimensões novas em lote (INSERT ... ON CONFLICT DO NOTHING)
        e atualiza o cache com os IDs gerados.
        """
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
                cache["colaboradores"][c.nome] = c

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
                cache["canais"][c.nome] = c

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
                cache["status"][s.nome] = s

    def process_transacional_batch(
        self,
        registros: list[AtendimentoTransacionalImportSchema]
    ) -> dict:

        success_count = 0
        error_count = 0
        erros = []

        cache = self._build_dim_cache()

        # Coleta dimensões novas em lote (sem flush por linha)
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

        # Um único flush para todas as dimensões novas
        self._flush_new_dims(cache, new_colaboradores, new_canais, new_status)

        fatos_para_inserir = []

        for i, data in enumerate(registros, start=1):
            try:
                colaborador = cache["colaboradores"].get(data.colaborador_nome)
                canal = cache["canais"].get(data.canal_nome)
                status = cache["status"].get(data.status_nome)

                if not colaborador or not canal or not status:
                    raise Exception("Dimensão não encontrada no cache após inserção.")

                fatos_para_inserir.append({
                    "data_referencia": data.data_referencia,
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

                success_count += 1

            except Exception as e:
                error_count += 1
                erros.append(f"Linha {i}: {str(e)}")

        try:
            if fatos_para_inserir:
                self.db.execute(
                    insert(models.FatoAtendimento).values(fatos_para_inserir)
                )
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise Exception(f"Erro ao inserir lote: {str(e)}")

        return {"success_count": success_count, "error_count": error_count, "errors": erros[:10]}

    def process_voalle_batch(
        self,
        registros: list[VoalleAgregadoImportSchema]
    ) -> dict:

        success_count = 0
        error_count = 0
        erros = []

        cache = {"colaboradores": {c.nome: c for c in self.db.query(models.DimColaborador).all()}}

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
                cache["colaboradores"][c.nome] = c

        fatos_para_inserir = []

        for i, data in enumerate(registros, start=1):
            try:
                colaborador = cache["colaboradores"].get(data.colaborador_nome)  # type: ignore
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