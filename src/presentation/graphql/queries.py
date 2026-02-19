import strawberry
from typing import List, Optional
from datetime import datetime, date
from src.infrastructure.database.config import SessionLocal
from src.infrastructure.database import models
from src.application.services.dashboard_service import DashboardService
from .schema import (
    MetricasConsolidadasType,
    AtendimentoPorCanalType,
    RankingColaboradorType,
    VoalleDiarioType,
    ResumoVoalleType,
    UploadHistoricoType,
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
                # TME — espera na fila
                tme_ligacao_segundos=m["tme_ligacao_segundos"],
                tme_omni_segundos=m["tme_omni_segundos"],
                # TMA — duração da conversa
                tma_ligacao_segundos=m["tma_ligacao_segundos"],
                tma_omni_segundos=m["tma_omni_segundos"],
                # Notas
                nota_media_ligacao=m["nota_media_ligacao"],
                nota_media_omni=m["nota_media_omni"],
                nota_media_solucao_omni=m["nota_media_solucao_omni"],
                # Distribuição
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
                    tma_ligacao_segundos=r["tma_ligacao_segundos"],
                    nota_ligacao=r["nota_ligacao"],
                    atendimentos_omni=r["atendimentos_omni"],
                    tme_omni_segundos=r["tme_omni_segundos"],
                    tma_omni_segundos=r["tma_omni_segundos"],
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

    @strawberry.field
    def dados_voalle(
        self,
        data_inicio: Optional[date] = None,
        data_fim: Optional[date] = None,
        colaborador_id: Optional[int] = None,
    ) -> ResumoVoalleType:
        """
        Dados de produção do sistema Voalle (ISP).
        Retorna resumo agregado + registros individuais por colaborador/dia.
        Filtros opcionais: data_inicio, data_fim, colaborador_id
        """
        db = SessionLocal()
        try:
            service = DashboardService(db)
            dados = service.get_dados_voalle(data_inicio, data_fim, colaborador_id)

            return ResumoVoalleType(
                total_clientes_atendidos=dados["total_clientes_atendidos"],
                total_atendimentos=dados["total_atendimentos"],
                total_finalizados=dados["total_finalizados"],
                taxa_finalizacao=dados["taxa_finalizacao"],
                registros=[
                    VoalleDiarioType(
                        data_referencia=r["data_referencia"],
                        colaborador_id=r["colaborador_id"],
                        nome=r["nome"],
                        equipe=r["equipe"],
                        clientes_atendidos=r["clientes_atendidos"],
                        numero_atendimentos=r["numero_atendimentos"],
                        solicitacao_finalizada=r["solicitacao_finalizada"],
                    )
                    for r in dados["registros"]
                ],
            )
        finally:
            db.close()

    @strawberry.field
    def historico_uploads(self) -> List[UploadHistoricoType]:
        """
        Lista os arquivos importados com status e possíveis erros.
        Útil para auditoria e diagnóstico de importações anteriores.
        """
        db = SessionLocal()
        try:
            uploads = db.query(models.Upload).order_by(
                models.Upload.created_at.desc()
            ).limit(100).all()

            return [
                UploadHistoricoType(
                    id=str(u.id),
                    file_path=u.file_path,
                    status=u.status,
                    created_at=u.created_at.isoformat() if u.created_at is not None else None,
                    processed_at=u.processed_at.isoformat() if u.processed_at is not None else None,
                    error=u.error,
                )
                for u in uploads
            ]
        finally:
            db.close()