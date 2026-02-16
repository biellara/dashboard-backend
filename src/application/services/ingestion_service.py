"""
Service Layer para Ingestão de Dados.

Responsável por orquestrar a criação/recuperação de dimensões
e a inserção de registos nas tabelas de factos (Star Schema v2.0).
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

    def _get_or_create_dimension(self, model, **kwargs):
        """
        Busca um registo na tabela de dimensão (Canais ou Status).
        Se não existir, cria um novo registo.
        """
        instance = self.db.query(model).filter_by(**kwargs).first()

        if not instance:
            try:
                instance = model(**kwargs)
                self.db.add(instance)
                self.db.flush()
            except IntegrityError:
                self.db.rollback()
                instance = self.db.query(model).filter_by(**kwargs).first()
                if not instance:
                    raise ValueError(f"Falha ao criar/recuperar {model.__tablename__}: {kwargs}")

        return instance

    def _get_or_create_colaborador(self, nome: str, equipe: Optional[str] = None) -> models.DimColaborador:
        """
        Lógica inteligente para cruzar dados de Omnichannel e Voalle.
        Busca sempre pelo nome. Se receber a equipa e o banco estiver nulo, atualiza.
        """
        colaborador = self.db.query(models.DimColaborador).filter(models.DimColaborador.nome == nome).first()

        if colaborador:
            # Se a importação atual tem equipa e o banco não tinha (ex: primeiro veio Voalle, depois Omni)
            if equipe and colaborador.equipe is None:
                setattr(colaborador, 'equipe', equipe)
                self.db.add(colaborador)
                self.db.flush()
            return colaborador
        
        # Se não existe, cria um novo
        try:
            colaborador = models.DimColaborador(nome=nome, equipe=equipe)
            self.db.add(colaborador)
            self.db.flush()
        except IntegrityError:
            self.db.rollback()
            colaborador = self.db.query(models.DimColaborador).filter(models.DimColaborador.nome == nome).first()

        return colaborador


    def process_transacional_batch(self, registros: list[AtendimentoTransacionalImportSchema]) -> dict:
        """
        Processa lote de dados de WhatsApp (Omnichannel) e Ligações.
        Salva na tabela fato_atendimentos.
        """
        success_count = 0
        error_count = 0
        erros = []

        for i, data in enumerate(registros, start=1):
            try:
                # 1. Resolve as Dimensões
                colaborador = self._get_or_create_colaborador(
                    nome=data.colaborador_nome,
                    equipe=data.equipe
                )
                canal = self._get_or_create_dimension(models.DimCanal, nome=data.canal_nome)
                status = self._get_or_create_dimension(models.DimStatus, nome=data.status_nome)

                # 2. Cria o Fato
                novo_fato = models.FatoAtendimento(
                    data_referencia=data.data_referencia,
                    protocolo=data.protocolo,
                    sentido_interacao=data.sentido_interacao,
                    tempo_espera_segundos=data.tempo_espera_segundos,
                    tempo_atendimento_segundos=data.tempo_atendimento_segundos,
                    nota_solucao=data.nota_solucao,
                    nota_atendimento=data.nota_atendimento,
                    colaborador_id=colaborador.id,
                    canal_id=canal.id,
                    status_id=status.id
                )
                self.db.add(novo_fato)
                self.db.flush()
                success_count += 1

            except Exception as e:
                error_count += 1
                erros.append(f"Linha {i}: {str(e)}")
                self.db.rollback()

        try:
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise Exception(f"Erro ao commitar transação: {str(e)}")

        return {
            "success_count": success_count,
            "error_count": error_count,
            "errors": erros[:10]
        }

    def process_voalle_batch(self, registros: list[VoalleAgregadoImportSchema]) -> dict:
        """
        Processa lote de dados agregados do Voalle.
        Salva na tabela fato_voalle_diario.
        """
        success_count = 0
        error_count = 0
        erros = []

        for i, data in enumerate(registros, start=1):
            try:
                # Resolve o colaborador (sem equipe, pois Voalle não fornece)
                colaborador = self._get_or_create_colaborador(nome=data.colaborador_nome, equipe=None)

                # Cria o Fato Voalle
                novo_fato = models.FatoVoalleDiario(
                    data_referencia=data.data_referencia,
                    clientes_atendidos=data.clientes_atendidos,
                    numero_atendimentos=data.numero_atendimentos,
                    solicitacao_finalizada=data.solicitacao_finalizada,
                    colaborador_id=colaborador.id
                )
                self.db.add(novo_fato)
                self.db.flush()
                success_count += 1

            except Exception as e:
                error_count += 1
                erros.append(f"Linha {i}: {str(e)}")
                self.db.rollback()

        try:
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise Exception(f"Erro ao commitar transação: {str(e)}")

        return {
            "success_count": success_count,
            "error_count": error_count,
            "errors": erros[:10]
        }