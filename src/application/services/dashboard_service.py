"""
Service Layer para cálculos de KPIs do Dashboard.
Centraliza toda a lógica de agregação e métricas de negócio.
"""

from sqlalchemy.orm import Session
from sqlalchemy import func, case
from src.infrastructure.database import models
from datetime import datetime, timedelta
from typing import Optional, Dict, List

class DashboardService:
    """
    Serviço responsável por calcular KPIs e métricas do SAC.
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.SLA_LIMITE_SEGUNDOS = 300  # 5 minutos
        self.CSAT_LIMITE = 4.0
    
    def get_total_atendimentos(
        self, 
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        equipe: Optional[str] = None
    ) -> int:
        """
        Retorna o total de atendimentos no período.
        """
        query = self.db.query(func.count(models.FatoAtendimento.id))
        
        if data_inicio:
            query = query.filter(models.FatoAtendimento.data_referencia >= data_inicio)
        if data_fim:
            query = query.filter(models.FatoAtendimento.data_referencia <= data_fim)
        if equipe:
            query = query.join(models.DimColaborador).filter(
                models.DimColaborador.equipe == equipe
            )
        
        return query.scalar() or 0
    
    def get_sla_percentage(self, data_inicio=None, data_fim=None) -> float:
        query = self.db.query(
            func.count(models.FatoAtendimento.id).label('total'),
            func.sum(
                case(
                    (models.FatoAtendimento.tempo_espera_segundos <= self.SLA_LIMITE_SEGUNDOS, 1),
                    else_=0
                )
            ).label('dentro_sla')
        )

        if data_inicio:
            query = query.filter(models.FatoAtendimento.data_referencia >= data_inicio)
        if data_fim:
            query = query.filter(models.FatoAtendimento.data_referencia <= data_fim)

        result = query.first()

        if not result or result.total == 0:
            return 0.0

        return round((result.dentro_sla / result.total) * 100, 1)

    
    def get_tempo_medio_atendimento(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None
    ) -> int:
        """
        Retorna o tempo médio de atendimento em segundos.
        """
        query = self.db.query(
            func.avg(models.FatoAtendimento.tempo_atendimento_segundos)
        )
        
        if data_inicio:
            query = query.filter(models.FatoAtendimento.data_referencia >= data_inicio)
        if data_fim:
            query = query.filter(models.FatoAtendimento.data_referencia <= data_fim)
        
        result = query.scalar()
        return int(result) if result else 0
    
    def get_tempo_medio_espera(self, data_inicio=None, data_fim=None) -> int:
        query = self.db.query(
            func.avg(models.FatoAtendimento.tempo_espera_segundos)
        )

        if data_inicio:
            query = query.filter(models.FatoAtendimento.data_referencia >= data_inicio)
        if data_fim:
            query = query.filter(models.FatoAtendimento.data_referencia <= data_fim)

        result = query.scalar()
        return int(result) if result else 0

    def get_taxa_satisfacao(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None
    ) -> float:
        """
        Retorna a taxa de satisfação baseada na nota_atendimento.
        Considera satisfeito quando nota >= CSAT_LIMITE.
        """

        query = self.db.query(
            func.count(models.FatoAtendimento.id).label('total'),
            func.sum(
                case(
                    (models.FatoAtendimento.nota_atendimento >= self.CSAT_LIMITE, 1),
                    else_=0
                )
            ).label('satisfeitos')
        )

        # Ignorar registros sem nota
        query = query.filter(models.FatoAtendimento.nota_atendimento.isnot(None))

        if data_inicio:
            query = query.filter(models.FatoAtendimento.data_referencia >= data_inicio)
        if data_fim:
            query = query.filter(models.FatoAtendimento.data_referencia <= data_fim)

        result = query.first()

        if not result or result.total == 0:
            return 0.0

        taxa = (result.satisfeitos / result.total) * 100
        return round(taxa, 1)
    
    def get_atendimentos_por_canal(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None
    ) -> List[Dict]:
        """
        Retorna a distribuição de atendimentos por canal.
        """
        query = self.db.query(
            models.DimCanal.nome,
            func.count(models.FatoAtendimento.id).label('total')
        ).join(
            models.FatoAtendimento
        ).group_by(
            models.DimCanal.nome
        )
        
        if data_inicio:
            query = query.filter(models.FatoAtendimento.data_referencia >= data_inicio)
        if data_fim:
            query = query.filter(models.FatoAtendimento.data_referencia <= data_fim)
        
        results = query.all()
        
        return [
            {"canal": r.nome, "total": r.total}
            for r in results
        ]
    
    def get_ranking_colaboradores(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        limite: int = 10
    ) -> List[Dict]:
        """
        Retorna o ranking de colaboradores por volume de atendimentos.
        """
        query = self.db.query(
            models.DimColaborador.nome,
            models.DimColaborador.equipe,
            func.count(models.FatoAtendimento.id).label('total_atendimentos'),
            func.avg(models.FatoAtendimento.tempo_atendimento_segundos).label('tempo_medio'),
            func.sum(
                case(
                    (models.FatoAtendimento.nota_atendimento >= self.CSAT_LIMITE, 1),
                    else_=0
                )
            ).label('satisfeitos')
        ).join(
            models.FatoAtendimento
        ).group_by(
            models.DimColaborador.id,
            models.DimColaborador.nome,
            models.DimColaborador.equipe
        ).order_by(
            func.count(models.FatoAtendimento.id).desc()
        )
        
        if data_inicio:
            query = query.filter(models.FatoAtendimento.data_referencia >= data_inicio)
        if data_fim:
            query = query.filter(models.FatoAtendimento.data_referencia <= data_fim)
        
        results = query.limit(limite).all()
        
        return [
            {
                "nome": r.nome,
                "equipe": r.equipe,
                "total_atendimentos": r.total_atendimentos,
                "tempo_medio_segundos": int(r.tempo_medio) if r.tempo_medio else 0,
                "taxa_satisfacao": round((r.satisfeitos / r.total_atendimentos) * 100, 1) if r.total_atendimentos > 0 else 0
            }
            for r in results
        ]
    
    def get_metricas_consolidadas(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None
    ) -> Dict:
        """
        Retorna todas as métricas principais consolidadas em um único objeto.
        Otimizado para reduzir queries ao banco.
        """
        return {
            "total_atendimentos": self.get_total_atendimentos(data_inicio, data_fim),
            "sla_percentual": self.get_sla_percentage(data_inicio, data_fim),
            "tempo_medio_atendimento_segundos": self.get_tempo_medio_atendimento(data_inicio, data_fim),
            "tempo_medio_resposta_segundos": self.get_tempo_medio_espera(data_inicio, data_fim),
            "taxa_satisfacao": self.get_taxa_satisfacao(data_inicio, data_fim),
            "atendimentos_por_canal": self.get_atendimentos_por_canal(data_inicio, data_fim),
            "ranking_colaboradores": self.get_ranking_colaboradores(data_inicio, data_fim, limite=10)
        }