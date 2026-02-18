import strawberry
from typing import List, Optional


# ==========================================
# TIPOS DE DIMENSÃO
# ==========================================

@strawberry.type
class ColaboradorType:
    id: strawberry.ID
    nome: str
    equipe: Optional[str]
    turno: Optional[str]


@strawberry.type
class AtendimentoPorCanalType:
    canal: str
    total: int


# ==========================================
# RANKING COLABORADOR (novo — separado por canal)
# ==========================================

@strawberry.type
class RankingColaboradorType:
    posicao: int
    colaborador_id: int
    nome: str
    equipe: Optional[str]
    turno: Optional[str]

    # Ligação
    ligacoes_atendidas: int
    ligacoes_perdidas: int
    tme_ligacao_segundos: int
    nota_ligacao: Optional[float]

    # Omnichannel (WhatsApp)
    atendimentos_omni: int
    tme_omni_segundos: int
    nota_omni: Optional[float]

    # Consolidado
    total_atendimentos: int
    nota_final: Optional[float]


# ==========================================
# MÉTRICAS CONSOLIDADAS (novo)
# ==========================================

@strawberry.type
class MetricasConsolidadasType:
    total_atendimentos: int
    total_perdidas: int
    taxa_abandono: float
    sla_percentual: float

    # Ligação
    tme_ligacao_segundos: int
    nota_media_ligacao: float

    # Omni
    tme_omni_segundos: int
    nota_media_omni: float

    # Distribuição
    atendimentos_por_canal: List[AtendimentoPorCanalType]