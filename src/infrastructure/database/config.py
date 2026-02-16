import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
from sqlalchemy.pool import NullPool

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Validação da variável de ambiente
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError(
        "\n❌ ERRO: DATABASE_URL não configurada!\n"
        "\n📝 Passos para corrigir:"
        "\n1. Copie o arquivo .env.example para .env"
        "\n2. Edite o arquivo .env com suas credenciais do banco"
        "\n3. Reinicie a aplicação\n"
        "\nExemplo de DATABASE_URL:"
        "\nDATABASE_URL=postgresql://usuario:senha@localhost:5432/dashboard_sac\n"
    )

# Criação do engine SQLAlchemy
engine = create_engine(
    DATABASE_URL,
    poolclass=NullPool,
    pool_pre_ping=True,
)

# Criação da SessionLocal
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# Base para os modelos
Base = declarative_base()

def get_db():
    """
    Dependency para FastAPI que fornece uma sessão do banco de dados.
    Garante que a conexão seja fechada após o uso.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()