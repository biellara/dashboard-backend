from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional

@dataclass
class AtendimentoTransacional:
    """Entidade para interações linha a linha (WhatsApp e Telefonia)"""
    id: Optional[int]
    data_referencia: datetime
    tempo_espera_segundos: int
    tempo_atendimento_segundos: int
    
    # Novos campos opcionais dependendo do canal
    protocolo: Optional[str]
    sentido_interacao: Optional[str]
    nota_solucao: Optional[float]
    nota_atendimento: Optional[float]
    
    # Chaves Estrangeiras para as Dimensões
    colaborador_id: int
    canal_id: int
    status_id: int

    @property
    def dentro_do_sla_espera(self) -> bool:
        """Exemplo de regra: SLA de espera na fila de 5 minutos (300 segundos)"""
        return self.tempo_espera_segundos <= 300

@dataclass
class ProdutividadeVoalle:
    """Entidade para fechamento diário do Voalle"""
    id: Optional[int]
    data_referencia: date
    clientes_atendidos: int
    numero_atendimentos: int
    solicitacao_finalizada: int
    colaborador_id: int