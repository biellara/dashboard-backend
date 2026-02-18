# Dashboard SAC - Backend

API robusta para controle de desempenho e KPIs do SAC, integrando dados de múltiplas plataformas de atendimento.

## 🚀 Estrutura do Projeto

O projeto segue uma arquitetura modularizada:

- **sql/**: Scripts de definição de esquema (Star Schema).
- **src/**:
    - **application/**: Camada de serviço e lógica de negócio. Contém `DashboardService` para KPIs e `IngestionService` para ETL.
    - **domain/**: Entidades de domínio e esquemas de dados (DTOs).
    - **infrastructure/**: Configurações de banco de dados, modelos SQLAlchemy e processadores de massa.
    - **presentation/**: Controladores REST (Ingestão) e resolvers GraphQL (Consultas).
- **worker.py**: Serviço em segundo plano para processamento de uploads pendentes.

## 🛠️ Tecnologias Principais
- **FastAPI**: Framework web de alta performance.
- **Strawberry GraphQL**: API de consulta tipada e eficiente.
- **SQLAlchemy & PostgreSQL**: Gerenciamento de banco de dados relacional.
- **Pandas**: Manipulação e sanitização de dados de entrada.

## 📡 Endpoints Principais
- `GET /`: Status da API.
- `POST /ingestion/upload-csv`: Upload e processamento de dados.
- `ANY /graphql`: Interface para consultas complexas de KPIs.

## ☁️ Deploy
Configurado para execução em ambiente Serverless via Vercel (`vercel.json`).