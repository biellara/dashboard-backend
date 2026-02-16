from dataclasses import dataclass
from typing import Optional

@dataclass
class Colaborador:
    id: Optional[int]
    nome: str
    equipe: str

@dataclass
class Canal:
    id: Optional[int]
    nome: str  # Ex: WhatsApp, Telefone, E-mail

@dataclass
class Status:
    id: Optional[int]
    nome: str  # Ex: Concluído, Pendente, Cancelado