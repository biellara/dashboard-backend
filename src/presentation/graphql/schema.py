import strawberry
from typing import List, Optional
from datetime import date


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
# RANKING COLABORADOR
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
    tme_ligacao_segundos: int   # Tempo Médio de Espera (fila)
    tma_ligacao_segundos: int   # Tempo Médio de Atendimento (conversa) — NOVO
    nota_ligacao: Optional[float]

    # Omnichannel (WhatsApp)
    atendimentos_omni: int
    tme_omni_segundos: int      # Tempo Médio de Espera (fila)
    tma_omni_segundos: int      # Tempo Médio de Atendimento (conversa) — NOVO
    nota_omni: Optional[float]

    # Consolidado
    total_atendimentos: int
    nota_final: Optional[float]


# ==========================================
# MÉTRICAS CONSOLIDADAS
# ==========================================

@strawberry.type
class MetricasConsolidadasType:
    total_atendimentos: int
    total_perdidas: int
    taxa_abandono: float
    sla_percentual: float

    # Ligação
    tme_ligacao_segundos: int   # Tempo Médio de Espera (fila)
    tma_ligacao_segundos: int   # Tempo Médio de Atendimento (conversa) — NOVO
    nota_media_ligacao: float

    # Omni
    tme_omni_segundos: int      # Tempo Médio de Espera (fila)
    tma_omni_segundos: int      # Tempo Médio de Atendimento (conversa) — NOVO
    nota_media_omni: float
    nota_media_solucao_omni: float  # Nota da solução (separada do atendente) — NOVO

    # Distribuição
    atendimentos_por_canal: List[AtendimentoPorCanalType]


# ==========================================
# VOALLE DIÁRIO — NOVO
# ==========================================

@strawberry.type
class VoalleDiarioType:
    """Produção diária de um colaborador no sistema Voalle (ISP)."""
    data_referencia: date
    colaborador_id: int
    nome: str
    equipe: Optional[str]
    clientes_atendidos: int
    numero_atendimentos: int
    solicitacao_finalizada: int


@strawberry.type
class ResumoVoalleType:
    """Agregado do período com lista de registros individuais."""
    total_clientes_atendidos: int
    total_atendimentos: int
    total_finalizados: int
    taxa_finalizacao: float          # (total_finalizados / total_atendimentos) * 100
    registros: List[VoalleDiarioType]


# ==========================================
# HISTÓRICO DE UPLOADS — NOVO
# ==========================================

@strawberry.type
class UploadHistoricoType:
    """Registro de um arquivo importado."""
    id: str
    file_path: str
    status: Optional[str]
    created_at: Optional[str]
    processed_at: Optional[str]
    error: Optional[str]