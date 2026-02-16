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
    equipe VARCHAR NOT NULL
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

-- Tabela de Fato: Atendimentos
CREATE TABLE IF NOT EXISTS fato_atendimentos (
    id SERIAL PRIMARY KEY,
    data_referencia TIMESTAMP NOT NULL,
    tempo_resposta_segundos INTEGER NOT NULL,
    tempo_atendimento_segundos INTEGER NOT NULL,
    satisfeito BOOLEAN DEFAULT TRUE,
    
    -- Chaves Estrangeiras (Star Schema)
    colaborador_id INTEGER NOT NULL REFERENCES dim_colaboradores(id),
    canal_id INTEGER NOT NULL REFERENCES dim_canais(id),
    status_id INTEGER NOT NULL REFERENCES dim_status(id)
);

-- Índice para melhorar performance em queries por data
CREATE INDEX IF NOT EXISTS idx_data_referencia 
ON fato_atendimentos(data_referencia);

-- Índices adicionais para otimização
CREATE INDEX IF NOT EXISTS idx_colaborador 
ON fato_atendimentos(colaborador_id);

CREATE INDEX IF NOT EXISTS idx_canal 
ON fato_atendimentos(canal_id);