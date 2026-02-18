from pydantic import BaseModel, Field
from datetime import datetime, date
from typing import Optional


class AtendimentoTransacionalImportSchema(BaseModel):
    data_referencia: datetime
    turno: str  # Calculado automaticamente pelo horário
    colaborador_nome: str
    equipe: Optional[str] = None
    canal_nome: str
    status_nome: str
    protocolo: Optional[str] = None
    sentido_interacao: Optional[str] = None
    tempo_espera_segundos: int = Field(default=0, ge=0)
    tempo_atendimento_segundos: int = Field(default=0, ge=0)
    nota_solucao: Optional[float] = None
    nota_atendimento: Optional[float] = None


class VoalleAgregadoImportSchema(BaseModel):
    data_referencia: date
    colaborador_nome: str
    clientes_atendidos: int = Field(default=0, ge=0)
    numero_atendimentos: int = Field(default=0, ge=0)
    solicitacao_finalizada: int = Field(default=0, ge=0)