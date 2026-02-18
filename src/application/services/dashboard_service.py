"""
Service Layer para cálculos de KPIs do Dashboard SAC.
Centraliza toda a lógica de agregação e métricas de negócio.

Canais suportados:
  - Ligação  → nota_atendimento (Avaliação 1)
  - WhatsApp → nota_solucao + nota_atendimento (Média Omni)

Nota Final por colaborador:
  - Média Ligação  = avg(nota_atendimento) onde canal = 'Ligação' e status != 'Perdida'
  - Média Omni     = avg((nota_solucao + nota_atendimento) / 2) onde canal = 'WhatsApp'
  - Nota Final     = média ponderada pelo volume de cada canal
"""

from sqlalchemy.orm import Session
from sqlalchemy import func, case, and_
from src.infrastructure.database import models
from datetime import datetime
from typing import Optional, Dict, List


class DashboardService:

    TURNOS = ["Madrugada", "Manhã", "Tarde", "Noite"]
    SLA_LIMITE_SEGUNDOS = 300   # 5 minutos
    CSAT_LIMITE = 4.0

    def __init__(self, db: Session):
        self.db = db

    # =====================================================
    # HELPERS INTERNOS
    # =====================================================

    def _filtro_periodo(self, query, data_inicio, data_fim):
        if data_inicio:
            query = query.filter(models.FatoAtendimento.data_referencia >= data_inicio)
        if data_fim:
            query = query.filter(models.FatoAtendimento.data_referencia <= data_fim)
        return query

    def _filtro_turno(self, query, turno: Optional[str]):
        if turno:
            query = query.join(
                models.DimColaborador,
                models.FatoAtendimento.colaborador_id == models.DimColaborador.id
            ).filter(models.DimColaborador.turno == turno)
        return query

    def _filtro_canal(self, query, canal_nome: str):
        return query.join(
            models.DimCanal,
            models.FatoAtendimento.canal_id == models.DimCanal.id
        ).filter(models.DimCanal.nome == canal_nome)

    # =====================================================
    # KPIs GERAIS
    # =====================================================

    def get_total_atendimentos(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        turno: Optional[str] = None,
    ) -> int:
        """Total de atendimentos no período (exclui ligações perdidas)."""
        query = self.db.query(func.count(models.FatoAtendimento.id))
        query = query.join(
            models.DimStatus,
            models.FatoAtendimento.status_id == models.DimStatus.id
        ).filter(models.DimStatus.nome != "Perdida")
        query = self._filtro_periodo(query, data_inicio, data_fim)
        query = self._filtro_turno(query, turno)
        return query.scalar() or 0

    def get_total_perdidas(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        turno: Optional[str] = None,
    ) -> int:
        """Total de ligações perdidas no período."""
        query = self.db.query(func.count(models.FatoAtendimento.id))
        query = query.join(
            models.DimStatus,
            models.FatoAtendimento.status_id == models.DimStatus.id
        ).filter(models.DimStatus.nome == "Perdida")
        query = self._filtro_periodo(query, data_inicio, data_fim)
        query = self._filtro_turno(query, turno)
        return query.scalar() or 0

    def get_taxa_abandono(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        turno: Optional[str] = None,
    ) -> float:
        """Taxa de abandono = perdidas / (atendidas + perdidas) * 100."""
        atendidas = self.get_total_atendimentos(data_inicio, data_fim, turno)
        perdidas = self.get_total_perdidas(data_inicio, data_fim, turno)
        total = atendidas + perdidas
        if total == 0:
            return 0.0
        return round((perdidas / total) * 100, 1)

    def get_sla_percentual(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        turno: Optional[str] = None,
    ) -> float:
        """Percentual de atendimentos dentro do SLA (espera <= 5 min)."""
        base = self.db.query(
            func.count(models.FatoAtendimento.id).label("total"),
            func.sum(case(
                (models.FatoAtendimento.tempo_espera_segundos <= self.SLA_LIMITE_SEGUNDOS, 1),
                else_=0
            )).label("dentro_sla")
        ).join(
            models.DimStatus,
            models.FatoAtendimento.status_id == models.DimStatus.id
        ).filter(models.DimStatus.nome != "Perdida")

        base = self._filtro_periodo(base, data_inicio, data_fim)
        base = self._filtro_turno(base, turno)
        result = base.first()

        if not result or not result.total:
            return 0.0
        return round((result.dentro_sla / result.total) * 100, 1)

    def get_tme_ligacao(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        turno: Optional[str] = None,
    ) -> int:
        """Tempo médio de espera (segundos) — somente Ligações atendidas."""
        query = self.db.query(func.avg(models.FatoAtendimento.tempo_espera_segundos))
        query = self._filtro_canal(query, "Ligação")
        query = query.join(
            models.DimStatus,
            models.FatoAtendimento.status_id == models.DimStatus.id
        ).filter(models.DimStatus.nome != "Perdida")
        query = self._filtro_periodo(query, data_inicio, data_fim)
        query = self._filtro_turno(query, turno)
        result = query.scalar()
        return int(result) if result else 0

    def get_tme_omni(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        turno: Optional[str] = None,
    ) -> int:
        """Tempo médio de espera (segundos) — somente WhatsApp."""
        query = self.db.query(func.avg(models.FatoAtendimento.tempo_espera_segundos))
        query = self._filtro_canal(query, "WhatsApp")
        query = self._filtro_periodo(query, data_inicio, data_fim)
        query = self._filtro_turno(query, turno)
        result = query.scalar()
        return int(result) if result else 0

    def get_nota_media_ligacao(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        turno: Optional[str] = None,
    ) -> float:
        """Nota média de atendimento — somente Ligações com avaliação."""
        query = self.db.query(func.avg(models.FatoAtendimento.nota_atendimento))
        query = self._filtro_canal(query, "Ligação")
        query = query.filter(models.FatoAtendimento.nota_atendimento.isnot(None))
        query = self._filtro_periodo(query, data_inicio, data_fim)
        query = self._filtro_turno(query, turno)
        result = query.scalar()
        return round(float(result), 2) if result else 0.0

    def get_nota_media_omni(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        turno: Optional[str] = None,
    ) -> float:
        """
        Média Omni = avg((nota_solucao + nota_atendimento) / 2)
        Considera apenas registros com ambas as notas preenchidas.
        """
        query = self.db.query(
            func.avg(
                (models.FatoAtendimento.nota_solucao + models.FatoAtendimento.nota_atendimento) / 2
            )
        )
        query = self._filtro_canal(query, "WhatsApp")
        query = query.filter(
            models.FatoAtendimento.nota_solucao.isnot(None),
            models.FatoAtendimento.nota_atendimento.isnot(None)
        )
        query = self._filtro_periodo(query, data_inicio, data_fim)
        query = self._filtro_turno(query, turno)
        result = query.scalar()
        return round(float(result), 2) if result else 0.0

    def get_atendimentos_por_canal(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        turno: Optional[str] = None,
    ) -> List[Dict]:
        """Distribuição de atendimentos por canal."""
        query = self.db.query(
            models.DimCanal.nome,
            func.count(models.FatoAtendimento.id).label("total")
        ).join(
            models.DimCanal,
            models.FatoAtendimento.canal_id == models.DimCanal.id
        ).join(
            models.DimStatus,
            models.FatoAtendimento.status_id == models.DimStatus.id
        ).filter(
            models.DimStatus.nome != "Perdida"
        ).group_by(models.DimCanal.nome)

        query = self._filtro_periodo(query, data_inicio, data_fim)
        query = self._filtro_turno(query, turno)

        return [{"canal": r.nome, "total": r.total} for r in query.all()]

    # =====================================================
    # RANKING DE COLABORADORES COM NOTA FINAL
    # =====================================================

    def get_ranking_colaboradores(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        turno: Optional[str] = None,
        limite: int = 50,
    ) -> List[Dict]:
        """
        Ranking completo de colaboradores com:
        - Métricas de Ligação e Omni separadas
        - Nota Final = média ponderada pelo volume de cada canal
        - Ordenado por Nota Final decrescente
        """

        # --- Sub-query: métricas de Ligação por colaborador ---
        ligacao_sq = self.db.query(
            models.FatoAtendimento.colaborador_id.label("col_id"),
            func.count(models.FatoAtendimento.id).label("total_ligacao"),
            func.sum(case(
                (models.DimStatus.nome == "Perdida", 1), else_=0
            )).label("total_perdida"),
            func.avg(models.FatoAtendimento.tempo_espera_segundos).label("tme_ligacao"),
            func.avg(
                case(
                    (models.FatoAtendimento.nota_atendimento.isnot(None),
                     models.FatoAtendimento.nota_atendimento),
                    else_=None
                )
            ).label("nota_ligacao"),
        ).join(
            models.DimCanal,
            models.FatoAtendimento.canal_id == models.DimCanal.id
        ).join(
            models.DimStatus,
            models.FatoAtendimento.status_id == models.DimStatus.id
        ).filter(
            models.DimCanal.nome == "Ligação"
        )

        if data_inicio:
            ligacao_sq = ligacao_sq.filter(models.FatoAtendimento.data_referencia >= data_inicio)
        if data_fim:
            ligacao_sq = ligacao_sq.filter(models.FatoAtendimento.data_referencia <= data_fim)

        ligacao_sq = ligacao_sq.group_by(
            models.FatoAtendimento.colaborador_id
        ).subquery()

        # --- Sub-query: métricas de Omni por colaborador ---
        omni_sq = self.db.query(
            models.FatoAtendimento.colaborador_id.label("col_id"),
            func.count(models.FatoAtendimento.id).label("total_omni"),
            func.avg(models.FatoAtendimento.tempo_espera_segundos).label("tme_omni"),
            func.avg(
                case(
                    (and_(
                        models.FatoAtendimento.nota_solucao.isnot(None),
                        models.FatoAtendimento.nota_atendimento.isnot(None)
                    ),
                    (models.FatoAtendimento.nota_solucao + models.FatoAtendimento.nota_atendimento) / 2),
                    else_=None
                )
            ).label("nota_omni"),
        ).join(
            models.DimCanal,
            models.FatoAtendimento.canal_id == models.DimCanal.id
        ).filter(
            models.DimCanal.nome == "WhatsApp"
        )

        if data_inicio:
            omni_sq = omni_sq.filter(models.FatoAtendimento.data_referencia >= data_inicio)
        if data_fim:
            omni_sq = omni_sq.filter(models.FatoAtendimento.data_referencia <= data_fim)

        omni_sq = omni_sq.group_by(
            models.FatoAtendimento.colaborador_id
        ).subquery()

        # --- Query principal: join colaborador + subqueries ---
        query = self.db.query(
            models.DimColaborador.id,
            models.DimColaborador.nome,
            models.DimColaborador.equipe,
            models.DimColaborador.turno,
            ligacao_sq.c.total_ligacao,
            ligacao_sq.c.total_perdida,
            ligacao_sq.c.tme_ligacao,
            ligacao_sq.c.nota_ligacao,
            omni_sq.c.total_omni,
            omni_sq.c.tme_omni,
            omni_sq.c.nota_omni,
        ).outerjoin(
            ligacao_sq, models.DimColaborador.id == ligacao_sq.c.col_id
        ).outerjoin(
            omni_sq, models.DimColaborador.id == omni_sq.c.col_id
        ).filter(
            (ligacao_sq.c.total_ligacao.isnot(None)) |
            (omni_sq.c.total_omni.isnot(None))
        )

        if turno:
            query = query.filter(models.DimColaborador.turno == turno)

        results = query.limit(limite).all()

        ranking = []
        for r in results:
            total_ligacao = int(r.total_ligacao or 0)
            total_perdida = int(r.total_perdida or 0)
            total_omni = int(r.total_omni or 0)
            nota_ligacao = round(float(r.nota_ligacao), 2) if r.nota_ligacao else None
            nota_omni = round(float(r.nota_omni), 2) if r.nota_omni else None

            # Nota Final: média ponderada pelo volume com avaliação
            vol_lig = total_ligacao - total_perdida  # atendidas
            vol_omni = total_omni
            nota_final = None

            if nota_ligacao is not None and nota_omni is not None:
                total_vol = vol_lig + vol_omni
                if total_vol > 0:
                    nota_final = round(
                        (nota_ligacao * vol_lig + nota_omni * vol_omni) / total_vol, 2
                    )
            elif nota_ligacao is not None:
                nota_final = nota_ligacao
            elif nota_omni is not None:
                nota_final = nota_omni

            ranking.append({
                "colaborador_id": r.id,
                "nome": r.nome,
                "equipe": r.equipe,
                "turno": r.turno,
                # Ligação
                "ligacoes_atendidas": vol_lig,
                "ligacoes_perdidas": total_perdida,
                "tme_ligacao_segundos": int(r.tme_ligacao) if r.tme_ligacao else 0,
                "nota_ligacao": nota_ligacao,
                # Omni
                "atendimentos_omni": total_omni,
                "tme_omni_segundos": int(r.tme_omni) if r.tme_omni else 0,
                "nota_omni": nota_omni,
                # Consolidado
                "total_atendimentos": vol_lig + total_omni,
                "nota_final": nota_final,
            })

        # Ordena por nota_final decrescente (None vai para o final)
        ranking.sort(key=lambda x: x["nota_final"] if x["nota_final"] is not None else -1, reverse=True)

        # Adiciona posição no ranking
        for i, item in enumerate(ranking, start=1):
            item["posicao"] = i

        return ranking

    # =====================================================
    # MÉTRICAS CONSOLIDADAS
    # =====================================================

    def get_metricas_consolidadas(
        self,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        turno: Optional[str] = None,
    ) -> Dict:
        """Retorna todas as métricas principais em um único objeto."""
        return {
            "total_atendimentos": self.get_total_atendimentos(data_inicio, data_fim, turno),
            "total_perdidas": self.get_total_perdidas(data_inicio, data_fim, turno),
            "taxa_abandono": self.get_taxa_abandono(data_inicio, data_fim, turno),
            "sla_percentual": self.get_sla_percentual(data_inicio, data_fim, turno),
            # Ligação
            "tme_ligacao_segundos": self.get_tme_ligacao(data_inicio, data_fim, turno),
            "nota_media_ligacao": self.get_nota_media_ligacao(data_inicio, data_fim, turno),
            # Omni
            "tme_omni_segundos": self.get_tme_omni(data_inicio, data_fim, turno),
            "nota_media_omni": self.get_nota_media_omni(data_inicio, data_fim, turno),
            # Distribuição
            "atendimentos_por_canal": self.get_atendimentos_por_canal(data_inicio, data_fim, turno),
        }