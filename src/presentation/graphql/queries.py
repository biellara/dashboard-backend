import strawberry
from typing import List, Optional, cast
from datetime import datetime
from src.infrastructure.database.config import SessionLocal
from src.infrastructure.database import models
from src.application.services.dashboard_service import DashboardService
from .schema import (
    AtendimentoType, 
    ColaboradorType,
    MetricasConsolidadasType,
    AtendimentoPorCanalType,
    RankingColaboradorType
)

@strawberry.type
class Query:
    
    @strawberry.field
    def atendimentos(
        self,
        limite: int = 100,
        offset: int = 0,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None
    ) -> List[AtendimentoType]:
        """
        Retorna lista de atendimentos com filtros opcionais.
        """
        db = SessionLocal()
        try:
            query = db.query(models.FatoAtendimento)
            
            # Aplicar filtros
            if data_inicio:
                query = query.filter(models.FatoAtendimento.data_referencia >= data_inicio)
            if data_fim:
                query = query.filter(models.FatoAtendimento.data_referencia <= data_fim)
            
            # Ordenar por data mais recente
            query = query.order_by(models.FatoAtendimento.data_referencia.desc())
            
            # Paginação
            result = query.limit(limite).offset(offset).all()
            
            return [
                AtendimentoType(
                    id=strawberry.ID(str(a.id)),
                    data_referencia=cast(datetime, a.data_referencia),
                    tempo_resposta_segundos=cast(int, a.tempo_resposta_segundos),
                    tempo_atendimento_segundos=cast(int, a.tempo_atendimento_segundos),
                    satisfeito=cast(bool, a.satisfeito),
                    colaborador=ColaboradorType(
                        id=strawberry.ID(str(a.colaborador.id)),
                        nome=a.colaborador.nome,
                        equipe=a.colaborador.equipe
                    )
                )
                for a in result
            ]
        finally:
            db.close()
    
    @strawberry.field
    def kpi_total_atendimentos(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None
    ) -> int:
        """
        Retorna o total de atendimentos no período.
        """
        db = SessionLocal()
        try:
            service = DashboardService(db)
            return service.get_total_atendimentos(data_inicio, data_fim)
        finally:
            db.close()
    
    @strawberry.field
    def kpi_sla_percentual(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None
    ) -> float:
        """
        Retorna o percentual de atendimentos dentro do SLA.
        """
        db = SessionLocal()
        try:
            service = DashboardService(db)
            return service.get_sla_percentage(data_inicio, data_fim)
        finally:
            db.close()
    
    @strawberry.field
    def kpi_taxa_satisfacao(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None
    ) -> float:
        """
        Retorna a taxa de satisfação (%).
        """
        db = SessionLocal()
        try:
            service = DashboardService(db)
            return service.get_taxa_satisfacao(data_inicio, data_fim)
        finally:
            db.close()
    
    @strawberry.field
    def kpi_tempo_medio_atendimento(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None
    ) -> int:
        """
        Retorna o tempo médio de atendimento em segundos.
        """
        db = SessionLocal()
        try:
            service = DashboardService(db)
            return service.get_tempo_medio_atendimento(data_inicio, data_fim)
        finally:
            db.close()
    
    @strawberry.field
    def atendimentos_por_canal(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None
    ) -> List[AtendimentoPorCanalType]:
        """
        Retorna a distribuição de atendimentos por canal.
        """
        db = SessionLocal()
        try:
            service = DashboardService(db)
            resultados = service.get_atendimentos_por_canal(data_inicio, data_fim)
            
            return [
                AtendimentoPorCanalType(
                    canal=r["canal"],
                    total=r["total"]
                )
                for r in resultados
            ]
        finally:
            db.close()
    
    @strawberry.field
    def ranking_colaboradores(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        limite: int = 10
    ) -> List[RankingColaboradorType]:
        """
        Retorna o ranking de colaboradores por volume.
        """
        db = SessionLocal()
        try:
            service = DashboardService(db)
            resultados = service.get_ranking_colaboradores(data_inicio, data_fim, limite)
            
            return [
                RankingColaboradorType(
                    nome=r["nome"],
                    equipe=r["equipe"],
                    total_atendimentos=r["total_atendimentos"],
                    tempo_medio_segundos=r["tempo_medio_segundos"],
                    taxa_satisfacao=r["taxa_satisfacao"]
                )
                for r in resultados
            ]
        finally:
            db.close()
    
    @strawberry.field
    def metricas_consolidadas(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None
    ) -> MetricasConsolidadasType:
        """
        Retorna todas as métricas principais em uma única query (otimizado).
        """
        db = SessionLocal()
        try:
            service = DashboardService(db)
            metricas = service.get_metricas_consolidadas(data_inicio, data_fim)
            
            return MetricasConsolidadasType(
                total_atendimentos=metricas["total_atendimentos"],
                sla_percentual=metricas["sla_percentual"],
                tempo_medio_atendimento_segundos=metricas["tempo_medio_atendimento_segundos"],
                tempo_medio_resposta_segundos=metricas["tempo_medio_resposta_segundos"],
                taxa_satisfacao=metricas["taxa_satisfacao"],
                atendimentos_por_canal=[
                    AtendimentoPorCanalType(canal=c["canal"], total=c["total"])
                    for c in metricas["atendimentos_por_canal"]
                ],
                ranking_colaboradores=[
                    RankingColaboradorType(
                        nome=r["nome"],
                        equipe=r["equipe"],
                        total_atendimentos=r["total_atendimentos"],
                        tempo_medio_segundos=r["tempo_medio_segundos"],
                        taxa_satisfacao=r["taxa_satisfacao"]
                    )
                    for r in metricas["ranking_colaboradores"]
                ]
            )
        finally:
            db.close()