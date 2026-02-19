from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
import csv
import openpyxl
import io
import re
from datetime import datetime, date
from sqlalchemy.orm import Session
from src.infrastructure.database.config import get_db
from src.infrastructure.database import models
from src.application.services.ingestion_service import IngestionService
from src.application.dto.ingestion_schema import (
    AtendimentoTransacionalImportSchema,
    VoalleAgregadoImportSchema
)

router = APIRouter(prefix="/ingestion", tags=["Ingestão"])

CHUNK_SIZE = 6000
TURNOS_VALIDOS = ["Madrugada", "Manhã", "Tarde", "Noite"]

# Apenas registros cujo setor/fila começa com este prefixo são importados
SETOR_PREFIXO = "SAC"


# ==========================================
# FUNÇÕES AUXILIARES
# ==========================================

def calcular_turno(dt: datetime) -> str:
    """Calcula o turno com base no horário do atendimento."""
    hora = dt.hour
    if 0 <= hora <= 5:
        return "Madrugada"
    elif 6 <= hora <= 11:
        return "Manhã"
    elif 12 <= hora <= 17:
        return "Tarde"
    else:
        return "Noite"

def detect_encoding(raw: bytes) -> str:
    try:
        import chardet  # type: ignore
        result = chardet.detect(raw)
        enc = (result.get("encoding") or "utf-8").lower().replace("-", "_")
        return "utf-8" if "ascii" in enc else enc
    except Exception:
        return "utf-8"

def detect_separator(text: str) -> str:
    first_line = text.split("\n")[0]
    candidates = {",": first_line.count(","), ";": first_line.count(";")}
    max_count = max(candidates.values())
    return max(candidates, key=lambda k: candidates[k]) if max_count > 0 else ";"

def parse_time_to_seconds(time_str: str) -> int:
    if not time_str or time_str.strip() in ("-", ""):
        return 0
    try:
        parts = time_str.strip().split(":")
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        return 0
    except Exception:
        return 0

def clean_agent_name(name: str) -> str:
    if not name:
        return "Desconhecido"
    return name.split(" - ")[0].strip()

def safe_int(value) -> int:
    if not value or str(value).strip() == "-":
        return 0
    try:
        return int(float(str(value).replace(",", ".").strip()))
    except ValueError:
        return 0

def safe_float_or_none(value) -> float | None:
    if not value or str(value).strip() == "-":
        return None
    try:
        return float(str(value).replace(",", ".").strip())
    except ValueError:
        return None

def extract_date_from_filename(filename: str) -> date:
    match = re.search(r"(\d{2,4}[-_]?\d{2}[-_]?\d{2,4})", filename)
    if match:
        date_str = match.group(1).replace("_", "").replace("-", "")
        try:
            if len(date_str) == 8:
                if date_str.startswith("20"):
                    return datetime.strptime(date_str, "%Y%m%d").date()
                else:
                    return datetime.strptime(date_str, "%d%m%Y").date()
        except Exception:
            pass
    return date.today()

def is_setor_permitido(valor: str) -> bool:
    """
    Aceita qualquer fila/equipe que comece com 'SAC' (case-insensitive).
    Ex: "SAC", "SAC RETENÇÃO", "SAC - ATENDIMENTO", "Sac Vendas" → aceito.
    Se o campo vier vazio, também aceita (não há como filtrar).
    """
    if not valor or not valor.strip():
        return True
    return valor.strip().upper().startswith(SETOR_PREFIXO)

def detect_format(headers: list[str]):
    headers_upper = [h.upper() for h in headers]
    is_voalle_agregado = all(h in headers_upper for h in ["CA", "NA", "NSF"])
    is_omnichannel = "Nome do Atendente" in headers or "Tempo em Espera na Fila" in headers
    is_ligacao = "Data de início" in headers and "Sentido" in headers
    return is_voalle_agregado, is_omnichannel, is_ligacao

def parse_row_to_dto(row: dict, is_voalle, is_omnichannel, is_ligacao, voalle_data_ref=None):
    """
    Converte uma linha para DTO.
    Retorna None se o registro não pertencer ao SAC (filtro de setor).
    """

    if is_voalle:
        # Voalle agregado não tem coluna de setor — importa tudo
        assert voalle_data_ref is not None
        return VoalleAgregadoImportSchema(
            data_referencia=voalle_data_ref,
            colaborador_nome=clean_agent_name((row.get("Atendente") or "Desconhecido").strip()),
            clientes_atendidos=safe_int(row.get("CA")),
            numero_atendimentos=safe_int(row.get("NA")),
            solicitacao_finalizada=safe_int(row.get("NSF")),
        )

    elif is_omnichannel:
        equipe_raw = (row.get("Nome da Equipe") or "").strip()
        if not is_setor_permitido(equipe_raw):
            return None

        data_inicial = (row.get("Data Inicial") or "").strip()
        hora_inicial = (row.get("Hora Inicial") or "").strip()
        data_str = f"{data_inicial} {hora_inicial}".strip()
        data_ref = datetime.strptime(data_str, "%d/%m/%Y %H:%M:%S") if data_str else datetime.now()

        return AtendimentoTransacionalImportSchema(
            data_referencia=data_ref,
            turno=calcular_turno(data_ref),
            colaborador_nome=clean_agent_name((row.get("Nome do Atendente") or "Desconhecido").strip()),
            equipe=(equipe_raw or None),
            canal_nome="WhatsApp",
            status_nome=(row.get("Status") or "Desconhecido").strip(),
            protocolo=(row.get("Número do Protocolo") or None),
            sentido_interacao=None,
            tempo_espera_segundos=parse_time_to_seconds(row.get("Tempo em Espera na Fila") or ""),
            tempo_atendimento_segundos=parse_time_to_seconds(row.get("Tempo em Atendimento") or ""),
            nota_solucao=safe_float_or_none(row.get("Avaliação - Nota da Solução Oferecida")),
            nota_atendimento=safe_float_or_none(row.get("Avaliação - Nota do Atendimento Prestado")),
        )

    elif is_ligacao:
        fila_raw = (row.get("Fila") or "").strip()
        if not is_setor_permitido(fila_raw):
            return None

        data_inicio = (row.get("Data de início") or "").strip()
        data_ref = datetime.strptime(data_inicio, "%d/%m/%Y %H:%M:%S")

        return AtendimentoTransacionalImportSchema(
            data_referencia=data_ref,
            turno=calcular_turno(data_ref),
            colaborador_nome=clean_agent_name((row.get("Agente") or "Desconhecido").strip()),
            equipe=(fila_raw or None),
            canal_nome="Ligação",
            status_nome=(row.get("Status") or "Desconhecido").strip(),
            protocolo=(row.get("Protocolo") or None),
            sentido_interacao=(row.get("Sentido") or None),
            tempo_espera_segundos=parse_time_to_seconds(row.get("Espera") or ""),
            tempo_atendimento_segundos=parse_time_to_seconds(row.get("Atendimento") or ""),
            nota_solucao=None,
            nota_atendimento=safe_float_or_none(row.get("Avaliação 1")),
        )

    raise ValueError("Formato não reconhecido")


# ==========================================
# ENDPOINT: UPLOAD DE PLANILHA
# ==========================================

@router.post("/upload-csv")
async def upload_csv(
    file: UploadFile = File(...),
    data_voalle: str = Form(None),
    db: Session = Depends(get_db)
):
    if not file.filename or not file.filename.endswith((".csv", ".xlsx")):
        raise HTTPException(status_code=400, detail="Apenas arquivos CSV ou XLSX são permitidos.")

    service = IngestionService(db)

    try:
        raw_content = await file.read()
        rows: list[dict] = []
        headers: list[str] = []

        if file.filename.endswith(".xlsx"):
            wb = openpyxl.load_workbook(io.BytesIO(raw_content), data_only=True, read_only=True)
            sheet = wb.active
            if sheet is None:
                raise HTTPException(status_code=400, detail="Planilha Excel vazia ou inválida.")

            raw_headers = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True))
            headers = [str(h).strip() for h in raw_headers if h is not None]

            for row_values in sheet.iter_rows(min_row=2, values_only=True):
                if all(v is None for v in row_values):
                    continue
                rows.append({
                    headers[i]: str(row_values[i]).strip() if row_values[i] is not None else ""
                    for i in range(len(headers))
                })
            wb.close()

        else:
            encoding = detect_encoding(raw_content)
            try:
                decoded_content = raw_content.decode(encoding)
            except Exception:
                decoded_content = raw_content.decode("iso-8859-1", errors="replace")

            if decoded_content.startswith("\ufeff"):
                decoded_content = decoded_content[1:]

            separator = detect_separator(decoded_content)
            reader = csv.DictReader(io.StringIO(decoded_content), delimiter=separator)
            headers = [str(h).strip() for h in (reader.fieldnames or [])]
            rows = [
                {k.strip(): (v.strip() if v else "") for k, v in row.items()}
                for row in reader
            ]

        is_voalle, is_omnichannel, is_ligacao = detect_format(headers)

        if not (is_voalle or is_omnichannel or is_ligacao):
            raise HTTPException(
                status_code=400,
                detail="Formato não reconhecido. Use exportações do Voalle, Omnichannel ou Telefonia."
            )

        voalle_data_ref = None
        if is_voalle:
            if data_voalle:
                voalle_data_ref = datetime.strptime(data_voalle, "%Y-%m-%d").date()
            else:
                voalle_data_ref = extract_date_from_filename(file.filename)

            if voalle_data_ref is None:
                raise HTTPException(
                    status_code=400,
                    detail="Data de referência não fornecida para arquivo Voalle."
                )

        total_success = 0
        total_error = 0
        total_ignorados = 0
        total_duplicados = 0
        all_errors = []
        chunk = []

        # Coleta protocolos já vistos neste upload para deduplicação em memória.
        # Evita que subir o mesmo arquivo duas vezes dobre os números do dashboard.
        protocolos_vistos: set[str] = set()

        def flush_chunk():
            nonlocal total_success, total_error, all_errors, chunk
            if not chunk:
                return
            res = service.process_voalle_batch(chunk) if is_voalle else service.process_transacional_batch(chunk)
            total_success += res["success_count"]
            total_error += res["error_count"]
            all_errors.extend(res.get("errors", []))
            chunk = []

        for index, row in enumerate(rows, start=1):
            try:
                dto = parse_row_to_dto(row, is_voalle, is_omnichannel, is_ligacao, voalle_data_ref)

                if dto is None:
                    total_ignorados += 1
                    continue

                # Deduplicação por protocolo dentro do mesmo upload
                if not is_voalle and hasattr(dto, "protocolo"):
                    protocolo = getattr(dto, "protocolo", None)
                    if protocolo:
                        if protocolo in protocolos_vistos:
                            total_duplicados += 1
                            continue
                        protocolos_vistos.add(protocolo)

                chunk.append(dto)
                if len(chunk) >= CHUNK_SIZE:
                    flush_chunk()

            except Exception as e:
                total_error += 1
                if len(all_errors) < 20:
                    all_errors.append(f"Linha {index}: {str(e)}")

        flush_chunk()

        status = (
            "success" if total_success > 0 and total_error == 0
            else "warning" if total_success > 0
            else "error"
        )

        partes_msg = [f"{total_success} registros importados"]
        if total_ignorados:
            partes_msg.append(f"{total_ignorados} ignorados (outros setores)")
        if total_duplicados:
            partes_msg.append(f"{total_duplicados} duplicados ignorados")
        if total_error:
            partes_msg.append(f"{total_error} erros")

        return {
            "status": status,
            "message": ". ".join(partes_msg) + ".",
            "detalhes": {
                "success_count": total_success,
                "ignored_count": total_ignorados,
                "duplicate_count": total_duplicados,
                "error_count": total_error,
                "errors": all_errors[:10],
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno de processamento: {str(e)}")


# ==========================================
# ENDPOINT: LISTAR COLABORADORES
# ==========================================

@router.get("/colaboradores")
def listar_colaboradores(db: Session = Depends(get_db)):
    """Lista todos os colaboradores com seus turnos predominantes."""
    colaboradores = db.query(models.DimColaborador).order_by(
        models.DimColaborador.turno,
        models.DimColaborador.nome
    ).all()

    return [
        {
            "id": c.id,
            "nome": c.nome,
            "equipe": c.equipe,
            "turno": c.turno or "Não calculado"
        }
        for c in colaboradores
    ]