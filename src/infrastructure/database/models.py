from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Date, Numeric
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# ==========================================
# DIMENSÕES
# ==========================================

class DimColaborador(Base):
    __tablename__ = "dim_colaboradores"

    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String, nullable=False)
    equipe = Column(String, nullable=True)
    turno = Column(String, nullable=True)  # Madrugada | Manhã | Tarde | Noite


class DimCanal(Base):
    __tablename__ = "dim_canais"

    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String, unique=True, nullable=False)


class DimStatus(Base):
    __tablename__ = "dim_status"

    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String, unique=True, nullable=False)


# ==========================================
# FATOS
# ==========================================

class FatoAtendimento(Base):
    """Tabela transacional para Omnichannel (WhatsApp) e Ligações."""
    __tablename__ = "fato_atendimentos"

    id = Column(Integer, primary_key=True, index=True)
    data_referencia = Column(DateTime, nullable=False, index=True)

    protocolo = Column(String(100), nullable=True)
    sentido_interacao = Column(String(50), nullable=True)

    tempo_espera_segundos = Column(Integer, nullable=False, default=0)
    tempo_atendimento_segundos = Column(Integer, nullable=False, default=0)

    nota_solucao = Column(Numeric(5, 2), nullable=True)
    nota_atendimento = Column(Numeric(5, 2), nullable=True)

    # Chaves Estrangeiras
    colaborador_id = Column(Integer, ForeignKey("dim_colaboradores.id"), nullable=False, index=True)
    canal_id = Column(Integer, ForeignKey("dim_canais.id"), nullable=False, index=True)
    status_id = Column(Integer, ForeignKey("dim_status.id"), nullable=False, index=True)

    # Relacionamentos
    colaborador = relationship("DimColaborador")
    canal = relationship("DimCanal")
    status = relationship("DimStatus")


class FatoVoalleDiario(Base):
    """Tabela de dados agregados de produtividade (Voalle)."""
    __tablename__ = "fato_voalle_diario"

    id = Column(Integer, primary_key=True, index=True)
    data_referencia = Column(Date, nullable=False, index=True)

    clientes_atendidos = Column(Integer, nullable=False, default=0)
    numero_atendimentos = Column(Integer, nullable=False, default=0)
    solicitacao_finalizada = Column(Integer, nullable=False, default=0)

    # Chave Estrangeira
    colaborador_id = Column(Integer, ForeignKey("dim_colaboradores.id"), nullable=False, index=True)

    colaborador = relationship("DimColaborador")