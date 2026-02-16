import strawberry
from typing import List
from datetime import datetime

@strawberry.type
class ColaboradorType:
    id: strawberry.ID
    nome: str
    equipe: str

@strawberry.type
class AtendimentoType:
    id: strawberry.ID
    data_referencia: datetime
    tempo_resposta_segundos: int
    tempo_atendimento_segundos: int
    satisfeito: bool
    colaborador: ColaboradorType

@strawberry.type
class AtendimentoPorCanalType:
    canal: str
    total: int

@strawberry.type
class RankingColaboradorType:
    nome: str
    equipe: str
    total_atendimentos: int
    tempo_medio_segundos: int
    taxa_satisfacao: float

@strawberry.type
class MetricasConsolidadasType:
    """
    Tipo que agrega todas as métricas principais em uma única query.
    Otimiza o carregamento do dashboard reduzindo round-trips.
    """
    total_atendimentos: int
    sla_percentual: float
    tempo_medio_atendimento_segundos: int
    tempo_medio_resposta_segundos: int
    taxa_satisfacao: float
    atendimentos_por_canal: List[AtendimentoPorCanalType]
    ranking_colaboradores: List[RankingColaboradorType]