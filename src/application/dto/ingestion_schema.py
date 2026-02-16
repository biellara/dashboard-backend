from pydantic import BaseModel, Field
from datetime import datetime, date
from typing import Optional

class AtendimentoTransacionalImportSchema(BaseModel):
    """
    Esquema de Validação para os CSVs de Omnichannel e Ligações.
    O Controller deve preparar os dados brutos para se encaixarem aqui.
    """
    data_referencia: datetime
    colaborador_nome: str
    equipe: Optional[str] = None # Opcional para não quebrar integrações
    canal_nome: str
    status_nome: str
    
    # Campos que podem ser nulos dependendo de onde vem a origem
    protocolo: Optional[str] = None
    sentido_interacao: Optional[str] = None
    
    # Tempos devem chegar aqui já convertidos em segundos pelo Controller
    tempo_espera_segundos: int = Field(default=0, ge=0)
    tempo_atendimento_segundos: int = Field(default=0, ge=0)
    
    # Notas de avaliação
    nota_solucao: Optional[float] = None
    nota_atendimento: Optional[float] = None


class VoalleAgregadoImportSchema(BaseModel):
    """
    Esquema de Validação para o CSV do Voalle.
    """
    data_referencia: date
    colaborador_nome: str
    clientes_atendidos: int = Field(default=0, ge=0)
    numero_atendimentos: int = Field(default=0, ge=0)
    solicitacao_finalizada: int = Field(default=0, ge=0)