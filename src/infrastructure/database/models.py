from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Date, Numeric, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy.ext.declarative import declarative_base
import uuid
from datetime import datetime
from typing import Optional

Base = declarative_base()


class DimColaborador(Base):
    __tablename__ = "dim_colaboradores"
    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String, nullable=False)
    equipe = Column(String, nullable=True)
    turno = Column(String, nullable=True)
    aliases = relationship("DimColaboradorAlias", back_populates="colaborador")


class DimColaboradorAlias(Base):
    __tablename__ = "dim_colaborador_alias"
    id = Column(Integer, primary_key=True, index=True)
    alias = Column(Text, nullable=False, unique=True)
    colaborador_id = Column(Integer, ForeignKey("dim_colaboradores.id"), nullable=False, index=True)
    colaborador = relationship("DimColaborador", back_populates="aliases")


class DimCanal(Base):
    __tablename__ = "dim_canais"
    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String, unique=True, nullable=False)


class DimStatus(Base):
    __tablename__ = "dim_status"
    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String, unique=True, nullable=False)


class FatoAtendimento(Base):
    __tablename__ = "fato_atendimentos"
    id = Column(Integer, primary_key=True, index=True)
    data_referencia = Column(DateTime, nullable=False, index=True)
    turno = Column(String, nullable=False, index=True)
    protocolo = Column(String(100), nullable=True)
    sentido_interacao = Column(String(50), nullable=True)
    tempo_espera_segundos = Column(Integer, nullable=False, default=0)
    tempo_atendimento_segundos = Column(Integer, nullable=False, default=0)
    nota_solucao = Column(Numeric(5, 2), nullable=True)
    nota_atendimento = Column(Numeric(5, 2), nullable=True)
    colaborador_id = Column(Integer, ForeignKey("dim_colaboradores.id"), nullable=False, index=True)
    canal_id = Column(Integer, ForeignKey("dim_canais.id"), nullable=False, index=True)
    status_id = Column(Integer, ForeignKey("dim_status.id"), nullable=False, index=True)
    colaborador = relationship("DimColaborador")
    canal = relationship("DimCanal")
    status = relationship("DimStatus")


class FatoVoalleDiario(Base):
    __tablename__ = "fato_voalle_diario"
    id = Column(Integer, primary_key=True, index=True)
    data_referencia = Column(Date, nullable=False, index=True)
    clientes_atendidos = Column(Integer, nullable=False, default=0)
    numero_atendimentos = Column(Integer, nullable=False, default=0)
    solicitacao_finalizada = Column(Integer, nullable=False, default=0)
    colaborador_id = Column(Integer, ForeignKey("dim_colaboradores.id"), nullable=False, index=True)
    colaborador = relationship("DimColaborador")


class Upload(Base):
    __tablename__ = "uploads"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    file_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    status: Mapped[Optional[str]] = mapped_column(String, default="pending")
    total_registros: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    total_duplicados: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)