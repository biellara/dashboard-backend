"""
Service Layer para cálculos de KPIs do Dashboard SAC.

Filtro de turno aplicado diretamente em fato_atendimentos.turno
(calculado automaticamente pelo horário do atendimento na ingestão).
"""

from sqlalchemy.orm import Session
from sqlalchemy import func, case, and_
from src.infrastructure.database import models
from datetime import datetime
from typing import Optional, Dict, List


class DashboardService:

    SLA_LIMITE_SEGUNDOS = 300  # 5 minutos

    def __init__(self, db: Session):
        self.db = db

    # =====================================================
    # HELPERS
    # =====================================================

    def _filtro_base(self, query, data_inicio, data_fim, turno):
        """Aplica filtros de período e turno diretamente no fato."""
        if data_inicio:
            query = query.filter(models.FatoAtendimento.data_referencia >= data_inicio)
        if data_fim:
            query = query.filter(models.FatoAtendimento.data_referencia <= data_fim)
        if turno:
            query = query.filter(models.FatoAtendimento.turno == turno)
        return query

    def _filtro_canal(self, query, canal_nome: str):
        return query.join(
            models.DimCanal,
            models.FatoAtendimento.canal_id == models.DimCanal.id
        ).filter(models.DimCanal.nome == canal_nome)

    # =====================================================
    # KPIs GERAIS
    # =====================================================

    def get_total_atendimentos(self, data_inicio=None, data_fim=None, turno=None) -> int:
        query = self.db.query(func.count(models.FatoAtendimento.id))
        query = query.join(
            models.DimStatus, models.FatoAtendimento.status_id == models.DimStatus.id
        ).filter(models.DimStatus.nome != "Perdida")
        query = self._filtro_base(query, data_inicio, data_fim, turno)
        return query.scalar() or 0

    def get_total_perdidas(self, data_inicio=None, data_fim=None, turno=None) -> int:
        query = self.db.query(func.count(models.FatoAtendimento.id))
        query = query.join(
            models.DimStatus, models.FatoAtendimento.status_id == models.DimStatus.id
        ).filter(models.DimStatus.nome == "Perdida")
        query = self._filtro_base(query, data_inicio, data_fim, turno)
        return query.scalar() or 0

    def get_taxa_abandono(self, data_inicio=None, data_fim=None, turno=None) -> float:
        atendidas = self.get_total_atendimentos(data_inicio, data_fim, turno)
        perdidas = self.get_total_perdidas(data_inicio, data_fim, turno)
        total = atendidas + perdidas
        if total == 0:
            return 0.0
        return round((perdidas / total) * 100, 1)

    def get_sla_percentual(self, data_inicio=None, data_fim=None, turno=None) -> float:
        query = self.db.query(
            func.count(models.FatoAtendimento.id).label("total"),
            func.sum(case(
                (models.FatoAtendimento.tempo_espera_segundos <= self.SLA_LIMITE_SEGUNDOS, 1),
                else_=0
            )).label("dentro_sla")
        ).join(
            models.DimStatus, models.FatoAtendimento.status_id == models.DimStatus.id
        ).filter(models.DimStatus.nome != "Perdida")
        query = self._filtro_base(query, data_inicio, data_fim, turno)
        result = query.first()
        if not result or not result.total:
            return 0.0
        return round((result.dentro_sla / result.total) * 100, 1)

    # =====================================================
    # TME — Tempo Médio de Espera (fila)
    # =====================================================

    def get_tme_ligacao(self, data_inicio=None, data_fim=None, turno=None) -> int:
        """Tempo médio que o cliente aguardou na fila antes de ser atendido (Ligação)."""
        query = self.db.query(func.avg(models.FatoAtendimento.tempo_espera_segundos))
        query = self._filtro_canal(query, "Ligação")
        query = query.join(
            models.DimStatus, models.FatoAtendimento.status_id == models.DimStatus.id
        ).filter(models.DimStatus.nome != "Perdida")
        query = self._filtro_base(query, data_inicio, data_fim, turno)
        result = query.scalar()
        return int(result) if result else 0

    def get_tme_omni(self, data_inicio=None, data_fim=None, turno=None) -> int:
        """Tempo médio que o cliente aguardou na fila antes de ser atendido (WhatsApp)."""
        query = self.db.query(func.avg(models.FatoAtendimento.tempo_espera_segundos))
        query = self._filtro_canal(query, "WhatsApp")
        query = self._filtro_base(query, data_inicio, data_fim, turno)
        result = query.scalar()
        return int(result) if result else 0

    # =====================================================
    # TMA — Tempo Médio de Atendimento (conversa)  ← NOVO
    # =====================================================

    def get_tma_ligacao(self, data_inicio=None, data_fim=None, turno=None) -> int:
        """Tempo médio de duração da conversa/atendimento (Ligação)."""
        query = self.db.query(func.avg(models.FatoAtendimento.tempo_atendimento_segundos))
        query = self._filtro_canal(query, "Ligação")
        query = query.join(
            models.DimStatus, models.FatoAtendimento.status_id == models.DimStatus.id
        ).filter(
            models.DimStatus.nome != "Perdida",
            models.FatoAtendimento.tempo_atendimento_segundos > 0
        )
        query = self._filtro_base(query, data_inicio, data_fim, turno)
        result = query.scalar()
        return int(result) if result else 0

    def get_tma_omni(self, data_inicio=None, data_fim=None, turno=None) -> int:
        """Tempo médio de duração da conversa/atendimento (WhatsApp)."""
        query = self.db.query(func.avg(models.FatoAtendimento.tempo_atendimento_segundos))
        query = self._filtro_canal(query, "WhatsApp")
        query = query.filter(models.FatoAtendimento.tempo_atendimento_segundos > 0)
        query = self._filtro_base(query, data_inicio, data_fim, turno)
        result = query.scalar()
        return int(result) if result else 0

    # =====================================================
    # NOTAS DE SATISFAÇÃO
    # =====================================================

    def get_nota_media_ligacao(self, data_inicio=None, data_fim=None, turno=None) -> float:
        query = self.db.query(func.avg(models.FatoAtendimento.nota_atendimento))
        query = self._filtro_canal(query, "Ligação")
        query = query.filter(models.FatoAtendimento.nota_atendimento.isnot(None))
        query = self._filtro_base(query, data_inicio, data_fim, turno)
        result = query.scalar()
        return round(float(result), 2) if result else 0.0

    def get_nota_media_omni(self, data_inicio=None, data_fim=None, turno=None) -> float:
        """Nota média consolidada do omni: média de nota_solucao + nota_atendimento."""
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
        query = self._filtro_base(query, data_inicio, data_fim, turno)
        result = query.scalar()
        return round(float(result), 2) if result else 0.0

    def get_nota_media_solucao_omni(self, data_inicio=None, data_fim=None, turno=None) -> float:
        """Nota média da SOLUÇÃO oferecida (WhatsApp) — avalia o que foi resolvido, não o atendente."""
        query = self.db.query(func.avg(models.FatoAtendimento.nota_solucao))
        query = self._filtro_canal(query, "WhatsApp")
        query = query.filter(models.FatoAtendimento.nota_solucao.isnot(None))
        query = self._filtro_base(query, data_inicio, data_fim, turno)
        result = query.scalar()
        return round(float(result), 2) if result else 0.0

    def get_atendimentos_por_canal(self, data_inicio=None, data_fim=None, turno=None) -> List[Dict]:
        query = self.db.query(
            models.DimCanal.nome,
            func.count(models.FatoAtendimento.id).label("total")
        ).join(
            models.DimCanal, models.FatoAtendimento.canal_id == models.DimCanal.id
        ).join(
            models.DimStatus, models.FatoAtendimento.status_id == models.DimStatus.id
        ).filter(models.DimStatus.nome != "Perdida").group_by(models.DimCanal.nome)
        query = self._filtro_base(query, data_inicio, data_fim, turno)
        return [{"canal": r.nome, "total": r.total} for r in query.all()]

    # =====================================================
    # RANKING COM NOTA FINAL
    # =====================================================

    def get_ranking_colaboradores(
        self,
        data_inicio=None,
        data_fim=None,
        turno=None,
        limite: int = 50,
    ) -> List[Dict]:

        # Sub-query Ligação
        lig = self.db.query(
            models.FatoAtendimento.colaborador_id.label("col_id"),
            func.count(models.FatoAtendimento.id).label("total_ligacao"),
            func.sum(case((models.DimStatus.nome == "Perdida", 1), else_=0)).label("total_perdida"),
            func.avg(models.FatoAtendimento.tempo_espera_segundos).label("tme_ligacao"),
            func.avg(case(
                (models.FatoAtendimento.tempo_atendimento_segundos > 0,
                 models.FatoAtendimento.tempo_atendimento_segundos),
                else_=None
            )).label("tma_ligacao"),
            func.avg(case(
                (models.FatoAtendimento.nota_atendimento.isnot(None), models.FatoAtendimento.nota_atendimento),
                else_=None
            )).label("nota_ligacao"),
        ).join(
            models.DimCanal, models.FatoAtendimento.canal_id == models.DimCanal.id
        ).join(
            models.DimStatus, models.FatoAtendimento.status_id == models.DimStatus.id
        ).filter(models.DimCanal.nome == "Ligação")

        if data_inicio:
            lig = lig.filter(models.FatoAtendimento.data_referencia >= data_inicio)
        if data_fim:
            lig = lig.filter(models.FatoAtendimento.data_referencia <= data_fim)
        if turno:
            lig = lig.filter(models.FatoAtendimento.turno == turno)

        lig = lig.group_by(models.FatoAtendimento.colaborador_id).subquery()

        # Sub-query Omni
        omni = self.db.query(
            models.FatoAtendimento.colaborador_id.label("col_id"),
            func.count(models.FatoAtendimento.id).label("total_omni"),
            func.avg(models.FatoAtendimento.tempo_espera_segundos).label("tme_omni"),
            func.avg(case(
                (models.FatoAtendimento.tempo_atendimento_segundos > 0,
                 models.FatoAtendimento.tempo_atendimento_segundos),
                else_=None
            )).label("tma_omni"),
            func.avg(case(
                (and_(
                    models.FatoAtendimento.nota_solucao.isnot(None),
                    models.FatoAtendimento.nota_atendimento.isnot(None)
                ),
                (models.FatoAtendimento.nota_solucao + models.FatoAtendimento.nota_atendimento) / 2),
                else_=None
            )).label("nota_omni"),
        ).join(
            models.DimCanal, models.FatoAtendimento.canal_id == models.DimCanal.id
        ).filter(models.DimCanal.nome == "WhatsApp")

        if data_inicio:
            omni = omni.filter(models.FatoAtendimento.data_referencia >= data_inicio)
        if data_fim:
            omni = omni.filter(models.FatoAtendimento.data_referencia <= data_fim)
        if turno:
            omni = omni.filter(models.FatoAtendimento.turno == turno)

        omni = omni.group_by(models.FatoAtendimento.colaborador_id).subquery()

        # Sub-query Voalle — produtividade ISP, sem filtro de turno
        voalle = self.db.query(
            models.FatoVoalleDiario.colaborador_id.label("col_id"),
            func.sum(models.FatoVoalleDiario.clientes_atendidos).label("voalle_clientes"),
            func.sum(models.FatoVoalleDiario.numero_atendimentos).label("voalle_atendimentos"),
            func.sum(models.FatoVoalleDiario.solicitacao_finalizada).label("voalle_finalizados"),
        )

        if data_inicio:
            voalle = voalle.filter(models.FatoVoalleDiario.data_referencia >= data_inicio)
        if data_fim:
            voalle = voalle.filter(models.FatoVoalleDiario.data_referencia <= data_fim)

        voalle = voalle.group_by(models.FatoVoalleDiario.colaborador_id).subquery()

        # Query principal — apenas colaboradores SAC
        query = self.db.query(
            models.DimColaborador.id,
            models.DimColaborador.nome,
            models.DimColaborador.equipe,
            models.DimColaborador.turno,
            lig.c.total_ligacao,
            lig.c.total_perdida,
            lig.c.tme_ligacao,
            lig.c.tma_ligacao,
            lig.c.nota_ligacao,
            omni.c.total_omni,
            omni.c.tme_omni,
            omni.c.tma_omni,
            omni.c.nota_omni,
            voalle.c.voalle_clientes,
            voalle.c.voalle_atendimentos,
            voalle.c.voalle_finalizados,
        ).outerjoin(
            lig, models.DimColaborador.id == lig.c.col_id
        ).outerjoin(
            omni, models.DimColaborador.id == omni.c.col_id
        ).outerjoin(
            voalle, models.DimColaborador.id == voalle.c.col_id
        ).filter(
            models.DimColaborador.equipe == "SAC",
            (lig.c.total_ligacao.isnot(None))
            | (omni.c.total_omni.isnot(None))
            | (voalle.c.voalle_atendimentos.isnot(None))
        )

        results = query.limit(limite).all()

        ranking = []
        for r in results:
            total_ligacao = int(r.total_ligacao or 0)
            total_perdida = int(r.total_perdida or 0)
            total_omni = int(r.total_omni or 0)
            ligacoes_atendidas = total_ligacao - total_perdida
            nota_ligacao = round(float(r.nota_ligacao), 2) if r.nota_ligacao else None
            nota_omni = round(float(r.nota_omni), 2) if r.nota_omni else None

            # Nota final ponderada pelo volume de atendimentos
            nota_final = None
            if nota_ligacao is not None and nota_omni is not None:
                total_vol = ligacoes_atendidas + total_omni
                if total_vol > 0:
                    nota_final = round(
                        (nota_ligacao * ligacoes_atendidas + nota_omni * total_omni) / total_vol, 2
                    )
            elif nota_ligacao is not None:
                nota_final = nota_ligacao
            elif nota_omni is not None:
                nota_final = nota_omni

            voalle_atend = int(r.voalle_atendimentos or 0)
            voalle_final = int(r.voalle_finalizados or 0)

            ranking.append({
                "colaborador_id": r.id,
                "nome": r.nome,
                "equipe": r.equipe,
                "turno": r.turno,
                # Ligação
                "ligacoes_atendidas": ligacoes_atendidas,
                "ligacoes_perdidas": total_perdida,
                "tme_ligacao_segundos": int(r.tme_ligacao) if r.tme_ligacao else 0,
                "tma_ligacao_segundos": int(r.tma_ligacao) if r.tma_ligacao else 0,
                "nota_ligacao": nota_ligacao,
                # Omnichannel
                "atendimentos_omni": total_omni,
                "tme_omni_segundos": int(r.tme_omni) if r.tme_omni else 0,
                "tma_omni_segundos": int(r.tma_omni) if r.tma_omni else 0,
                "nota_omni": nota_omni,
                # Voalle (produtividade ISP)
                "voalle_clientes_atendidos": int(r.voalle_clientes or 0),
                "voalle_atendimentos": voalle_atend,
                "voalle_finalizados": voalle_final,
                "voalle_taxa_finalizacao": round((voalle_final / voalle_atend) * 100, 1) if voalle_atend > 0 else None,
                # Consolidado
                "total_atendimentos": ligacoes_atendidas + total_omni,
                "nota_final": nota_final,
            })

        ranking.sort(key=lambda x: x["nota_final"] if x["nota_final"] is not None else -1, reverse=True)
        for i, item in enumerate(ranking, start=1):
            item["posicao"] = i

        return ranking

    # =====================================================
    # MÉTRICAS CONSOLIDADAS
    # =====================================================

    def get_metricas_consolidadas(self, data_inicio=None, data_fim=None, turno=None) -> Dict:
        return {
            "total_atendimentos": self.get_total_atendimentos(data_inicio, data_fim, turno),
            "total_perdidas": self.get_total_perdidas(data_inicio, data_fim, turno),
            "taxa_abandono": self.get_taxa_abandono(data_inicio, data_fim, turno),
            "sla_percentual": self.get_sla_percentual(data_inicio, data_fim, turno),
            # TME — espera na fila
            "tme_ligacao_segundos": self.get_tme_ligacao(data_inicio, data_fim, turno),
            "tme_omni_segundos": self.get_tme_omni(data_inicio, data_fim, turno),
            # TMA — duração da conversa (NOVO)
            "tma_ligacao_segundos": self.get_tma_ligacao(data_inicio, data_fim, turno),
            "tma_omni_segundos": self.get_tma_omni(data_inicio, data_fim, turno),
            # Notas
            "nota_media_ligacao": self.get_nota_media_ligacao(data_inicio, data_fim, turno),
            "nota_media_omni": self.get_nota_media_omni(data_inicio, data_fim, turno),
            "nota_media_solucao_omni": self.get_nota_media_solucao_omni(data_inicio, data_fim, turno),
            # Distribuição
            "atendimentos_por_canal": self.get_atendimentos_por_canal(data_inicio, data_fim, turno),
        }

    # =====================================================
    # VOALLE DIÁRIO — NOVO
    # =====================================================

    def get_dados_voalle(
        self,
        data_inicio=None,
        data_fim=None,
        colaborador_id: Optional[int] = None,
    ) -> Dict:
        """
        Retorna dados do Voalle (ISP) com resumo agregado e registros individuais.
        Sem filtro de turno — Voalle não tem granularidade horária.
        """
        query = self.db.query(
            models.FatoVoalleDiario,
            models.DimColaborador.nome,
            models.DimColaborador.equipe,
        ).join(
            models.DimColaborador,
            models.FatoVoalleDiario.colaborador_id == models.DimColaborador.id
        )

        if data_inicio:
            query = query.filter(models.FatoVoalleDiario.data_referencia >= data_inicio)
        if data_fim:
            query = query.filter(models.FatoVoalleDiario.data_referencia <= data_fim)
        if colaborador_id:
            query = query.filter(models.FatoVoalleDiario.colaborador_id == colaborador_id)

        query = query.order_by(
            models.FatoVoalleDiario.data_referencia.desc(),
            models.DimColaborador.nome
        )

        rows = query.all()

        registros = [
            {
                "data_referencia": r.FatoVoalleDiario.data_referencia,
                "colaborador_id": r.FatoVoalleDiario.colaborador_id,
                "nome": r.nome,
                "equipe": r.equipe,
                "clientes_atendidos": r.FatoVoalleDiario.clientes_atendidos,
                "numero_atendimentos": r.FatoVoalleDiario.numero_atendimentos,
                "solicitacao_finalizada": r.FatoVoalleDiario.solicitacao_finalizada,
            }
            for r in rows
        ]

        total_clientes = sum(r["clientes_atendidos"] for r in registros)
        total_atend = sum(r["numero_atendimentos"] for r in registros)
        total_final = sum(r["solicitacao_finalizada"] for r in registros)
        taxa_final = round((total_final / total_atend) * 100, 1) if total_atend > 0 else 0.0

        return {
            "total_clientes_atendidos": total_clientes,
            "total_atendimentos": total_atend,
            "total_finalizados": total_final,
            "taxa_finalizacao": taxa_final,
            "registros": registros,
        }