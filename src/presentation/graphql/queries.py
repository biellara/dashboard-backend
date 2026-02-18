import strawberry
from typing import List, Optional
from datetime import datetime
from src.infrastructure.database.config import SessionLocal
from src.application.services.dashboard_service import DashboardService
from .schema import (
    MetricasConsolidadasType,
    AtendimentoPorCanalType,
    RankingColaboradorType,
)


@strawberry.type
class Query:

    @strawberry.field
    def metricas_consolidadas(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        turno: Optional[str] = None,
    ) -> MetricasConsolidadasType:
        """
        Retorna todas as métricas principais em uma única query.
        Filtros opcionais: data_inicio, data_fim, turno (Madrugada|Manhã|Tarde|Noite)
        """
        db = SessionLocal()
        try:
            service = DashboardService(db)
            m = service.get_metricas_consolidadas(data_inicio, data_fim, turno)

            return MetricasConsolidadasType(
                total_atendimentos=m["total_atendimentos"],
                total_perdidas=m["total_perdidas"],
                taxa_abandono=m["taxa_abandono"],
                sla_percentual=m["sla_percentual"],
                tme_ligacao_segundos=m["tme_ligacao_segundos"],
                nota_media_ligacao=m["nota_media_ligacao"],
                tme_omni_segundos=m["tme_omni_segundos"],
                nota_media_omni=m["nota_media_omni"],
                atendimentos_por_canal=[
                    AtendimentoPorCanalType(canal=c["canal"], total=c["total"])
                    for c in m["atendimentos_por_canal"]
                ],
            )
        finally:
            db.close()

    @strawberry.field
    def ranking_colaboradores(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        turno: Optional[str] = None,
        limite: int = 50,
    ) -> List[RankingColaboradorType]:
        """
        Ranking de colaboradores com métricas separadas por canal e Nota Final.
        Filtros opcionais: data_inicio, data_fim, turno, limite
        """
        db = SessionLocal()
        try:
            service = DashboardService(db)
            resultados = service.get_ranking_colaboradores(data_inicio, data_fim, turno, limite)

            return [
                RankingColaboradorType(
                    posicao=r["posicao"],
                    colaborador_id=r["colaborador_id"],
                    nome=r["nome"],
                    equipe=r["equipe"],
                    turno=r["turno"],
                    ligacoes_atendidas=r["ligacoes_atendidas"],
                    ligacoes_perdidas=r["ligacoes_perdidas"],
                    tme_ligacao_segundos=r["tme_ligacao_segundos"],
                    nota_ligacao=r["nota_ligacao"],
                    atendimentos_omni=r["atendimentos_omni"],
                    tme_omni_segundos=r["tme_omni_segundos"],
                    nota_omni=r["nota_omni"],
                    total_atendimentos=r["total_atendimentos"],
                    nota_final=r["nota_final"],
                )
                for r in resultados
            ]
        finally:
            db.close()

    @strawberry.field
    def atendimentos_por_canal(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        turno: Optional[str] = None,
    ) -> List[AtendimentoPorCanalType]:
        db = SessionLocal()
        try:
            service = DashboardService(db)
            return [
                AtendimentoPorCanalType(canal=c["canal"], total=c["total"])
                for c in service.get_atendimentos_por_canal(data_inicio, data_fim, turno)
            ]
        finally:
            db.close()