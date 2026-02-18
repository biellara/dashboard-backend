-- =====================================================
-- Schema do Dashboard SAC (Star Schema)
-- =====================================================
-- Criado manualmente via Supabase SQL Editor
-- Este arquivo serve como documentação e backup
-- =====================================================

-- Tabela de Dimensão: Colaboradores
CREATE TABLE IF NOT EXISTS dim_colaboradores (
    id SERIAL PRIMARY KEY,
    nome VARCHAR NOT NULL,
    equipe VARCHAR,
    turno VARCHAR  -- Madrugada | Manhã | Tarde | Noite
);

-- Tabela de Dimensão: Canais
CREATE TABLE IF NOT EXISTS dim_canais (
    id SERIAL PRIMARY KEY,
    nome VARCHAR UNIQUE NOT NULL
);

-- Tabela de Dimensão: Status
CREATE TABLE IF NOT EXISTS dim_status (
    id SERIAL PRIMARY KEY,
    nome VARCHAR UNIQUE NOT NULL
);

-- Tabela de Fato: Atendimentos (Omnichannel + Ligações)
CREATE TABLE IF NOT EXISTS fato_atendimentos (
    id SERIAL PRIMARY KEY,
    data_referencia TIMESTAMP NOT NULL,
    protocolo VARCHAR(100),
    sentido_interacao VARCHAR(50),
    tempo_espera_segundos INTEGER NOT NULL DEFAULT 0,
    tempo_atendimento_segundos INTEGER NOT NULL DEFAULT 0,
    nota_solucao NUMERIC(5, 2),
    nota_atendimento NUMERIC(5, 2),

    -- Chaves Estrangeiras (Star Schema)
    colaborador_id INTEGER NOT NULL REFERENCES dim_colaboradores(id),
    canal_id INTEGER NOT NULL REFERENCES dim_canais(id),
    status_id INTEGER NOT NULL REFERENCES dim_status(id)
);

-- Tabela de Fato: Produtividade Diária (Voalle Agregado)
CREATE TABLE IF NOT EXISTS fato_voalle_diario (
    id SERIAL PRIMARY KEY,
    data_referencia DATE NOT NULL,
    clientes_atendidos INTEGER NOT NULL DEFAULT 0,
    numero_atendimentos INTEGER NOT NULL DEFAULT 0,
    solicitacao_finalizada INTEGER NOT NULL DEFAULT 0,

    -- Chave Estrangeira
    colaborador_id INTEGER NOT NULL REFERENCES dim_colaboradores(id)
);

-- =====================================================
-- ÍNDICES
-- =====================================================

CREATE INDEX IF NOT EXISTS idx_fato_data_referencia
ON fato_atendimentos(data_referencia);

CREATE INDEX IF NOT EXISTS idx_fato_colaborador
ON fato_atendimentos(colaborador_id);

CREATE INDEX IF NOT EXISTS idx_fato_canal
ON fato_atendimentos(canal_id);

CREATE INDEX IF NOT EXISTS idx_fato_status
ON fato_atendimentos(status_id);

CREATE INDEX IF NOT EXISTS idx_voalle_data
ON fato_voalle_diario(data_referencia);

CREATE INDEX IF NOT EXISTS idx_voalle_colaborador
ON fato_voalle_diario(colaborador_id);

-- =====================================================
-- MIGRAÇÃO (rodar se o banco já existir)
-- =====================================================

-- ALTER TABLE dim_colaboradores ADD COLUMN IF NOT EXISTS turno VARCHAR;