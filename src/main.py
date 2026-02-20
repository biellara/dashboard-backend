import os
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from strawberry.fastapi import GraphQLRouter
from fastapi.middleware.cors import CORSMiddleware
import strawberry

from src.presentation.graphql.queries import Query
from src.presentation.controllers import ingestion_controller

# Criação do Schema GraphQL
schema = strawberry.Schema(query=Query)
graphql_app = GraphQLRouter(schema)

app = FastAPI(
    title="Dashboard SAC API",
    description="API para controle de desempenho e KPIs do SAC",
    version="1.0.0"
)

# CORS dinâmico – aceita localhost (dev) e domínio Vercel (prod)
origins = [
    "http://localhost:5173",
    "http://localhost:3000",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:3000",
    "http://localhost:8000",
    "https://dashboard-frontend-ten-theta.vercel.app",
    "https://dashboardsac.vercel.app"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Handler explícito para preflight CORS no GraphQL
@app.options("/graphql")
async def graphql_options(request: Request):
    return JSONResponse(
        content={},
        headers={
            "Access-Control-Allow-Origin": request.headers.get("origin", "*"),
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Credentials": "true",
        }
    )

# Rotas REST para Ingestão
app.include_router(ingestion_controller.router)

# Rota GraphQL para Leitura
app.include_router(graphql_app, prefix="/graphql")

@app.get("/")
def read_root():
    return {
        "status": "Dashboard SAC API Running",
        "version": "1.0.0",
        "endpoints": {
            "graphql": "/graphql",
            "ingestion": "/ingestion/upload-csv",
            "docs": "/docs"
        }
    }

@app.get("/health")
def health_check():
    """Endpoint de health check para monitoramento"""
    return {"status": "healthy"}