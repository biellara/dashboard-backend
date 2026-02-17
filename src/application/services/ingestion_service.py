"""
Service Layer para Ingestão de Dados.

Responsável por orquestrar a criação/recuperação de dimensões
e a inserção eficiente de registos nas tabelas de factos (Star Schema v2.0).
Versão otimizada para grandes volumes (até 50k+ linhas).
"""

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import Optional
from src.infrastructure.database import models
from src.application.dto.ingestion_schema import (
    AtendimentoTransacionalImportSchema,
    VoalleAgregadoImportSchema
)


class IngestionService:
    def __init__(self, db: Session):
        self.db = db

    # ==========================================================
    # PROCESSAMENTO TRANSAÇÕES (OMNICHANNEL + LIGAÇÕES)
    # ==========================================================

    def process_transacional_batch(
        self,
        registros: list[AtendimentoTransacionalImportSchema]
    ) -> dict:
        """
        Processa lote transacional usando:
        - Cache de dimensões
        - Bulk insert para fatos
        """

        success_count = 0
        error_count = 0
        erros = []

        fatos_para_inserir = []

        # 🔥 Cache de dimensões (evita milhares de queries repetidas)
        colaboradores_cache: dict[str, models.DimColaborador] = {}
        for c in self.db.query(models.DimColaborador).all():
            colaboradores_cache[c.nome] = c  # type: ignore
        
        canais_cache: dict[str, models.DimCanal] = {}
        for c in self.db.query(models.DimCanal).all():
            canais_cache[c.nome] = c  # type: ignore
        
        status_cache: dict[str, models.DimStatus] = {}
        for s in self.db.query(models.DimStatus).all():
            status_cache[s.nome] = s  # type: ignore

        for i, data in enumerate(registros, start=1):
            try:
                # ========================
                # Colaborador
                # ========================
                colaborador = colaboradores_cache.get(str(data.colaborador_nome))

                if not colaborador:
                    colaborador = models.DimColaborador(
                        nome=data.colaborador_nome,
                        equipe=data.equipe
                    )
                    self.db.add(colaborador)
                    self.db.flush()  # necessário para obter ID
                    colaboradores_cache[data.colaborador_nome] = colaborador

                # Atualiza equipe se vier nova informação
                elif data.equipe and colaborador.equipe is None:
                    setattr(colaborador, "equipe", data.equipe)
                    self.db.flush()

                # ========================
                # Canal
                # ========================
                canal = canais_cache.get(data.canal_nome)

                if not canal:
                    canal = models.DimCanal(nome=data.canal_nome)
                    self.db.add(canal)
                    self.db.flush()
                    canais_cache[data.canal_nome] = canal

                # ========================
                # Status
                # ========================
                status = status_cache.get(data.status_nome)

                if not status:
                    status = models.DimStatus(nome=data.status_nome)
                    self.db.add(status)
                    self.db.flush()
                    status_cache[data.status_nome] = status

                # ========================
                # Fato (apenas preparar dict)
                # ========================
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
                    "status_id": status.id
                })

                success_count += 1

            except Exception as e:
                error_count += 1
                erros.append(f"Linha {i}: {str(e)}")

        # 🔥 BULK INSERT FINAL
        try:
            if fatos_para_inserir:
                self.db.bulk_insert_mappings(
                    models.FatoAtendimento,  # type: ignore
                    fatos_para_inserir
                )
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise Exception(f"Erro ao inserir lote: {str(e)}")

        return {
            "success_count": success_count,
            "error_count": error_count,
            "errors": erros[:10]
        }

    # ==========================================================
    # PROCESSAMENTO VOALLE (AGREGADO)
    # ==========================================================

    def process_voalle_batch(
        self,
        registros: list[VoalleAgregadoImportSchema]
    ) -> dict:
        """
        Processa dados agregados do Voalle usando bulk insert.
        """

        success_count = 0
        error_count = 0
        erros = []

        fatos_para_inserir = []

        # Cache colaboradores
        colaboradores_cache: dict[str, models.DimColaborador] = {
            str(c.nome): c for c in self.db.query(models.DimColaborador).all()
        }

        for i, data in enumerate(registros, start=1):
            try:
                colaborador = colaboradores_cache.get(str(data.colaborador_nome))

                if not colaborador:
                    colaborador = models.DimColaborador(
                        nome=data.colaborador_nome,
                        equipe=None
                    )
                    self.db.add(colaborador)
                    self.db.flush()
                    colaboradores_cache[data.colaborador_nome] = colaborador

                fatos_para_inserir.append({
                    "data_referencia": data.data_referencia,
                    "clientes_atendidos": data.clientes_atendidos,
                    "numero_atendimentos": data.numero_atendimentos,
                    "solicitacao_finalizada": data.solicitacao_finalizada,
                    "colaborador_id": colaborador.id
                })

                success_count += 1

            except Exception as e:
                error_count += 1
                erros.append(f"Linha {i}: {str(e)}")

        try:
            if fatos_para_inserir:
                self.db.bulk_insert_mappings(
                    models.FatoVoalleDiario,  # type: ignore
                    fatos_para_inserir
                )
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise Exception(f"Erro ao inserir lote Voalle: {str(e)}")

        return {
            "success_count": success_count,
            "error_count": error_count,
            "errors": erros[:10]
        }
