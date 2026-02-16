from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
import csv
import openpyxl
import io
import re
from datetime import datetime, date
from sqlalchemy.orm import Session
from src.infrastructure.database.config import get_db
from src.application.services.ingestion_service import IngestionService
from src.application.dto.ingestion_schema import (
    AtendimentoTransacionalImportSchema,
    VoalleAgregadoImportSchema
)

router = APIRouter(prefix="/ingestion", tags=["Ingestão"])


# ==========================================
# FUNÇÕES AUXILIARES DE LIMPEZA (SANITIZAÇÃO)
# ==========================================

def detect_encoding(raw: bytes) -> str:
    """Detecta encoding para evitar erros de acentuação."""
    try:
        import chardet
        result = chardet.detect(raw)
        enc = (result.get("encoding") or "utf-8").lower().replace("-", "_")
        return "utf-8" if "ascii" in enc else enc
    except ImportError:
        return "utf-8"

def detect_separator(text: str) -> str:
    """Detecta se o CSV usa vírgula ou ponto-e-vírgula."""
    first_line = text.split("\n")[0]
    candidates = {",": first_line.count(","), ";": first_line.count(";")}
    max_count = max(candidates.values())
    return max(candidates, key=lambda k: candidates[k]) if max_count > 0 else ";"

def parse_time_to_seconds(time_str: str) -> int:
    """Converte formato 'HH:MM:SS' para segundos inteiros."""
    if not time_str or time_str.strip() == '-' or time_str.strip() == '':
        return 0
    try:
        parts = time_str.strip().split(':')
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        return 0
    except Exception:
        return 0

def clean_agent_name(name: str) -> str:
    """Remove sufixos numéricos (ex: 'Wellington - 6373' -> 'Wellington')."""
    if not name:
        return "Desconhecido"
    return name.split(" - ")[0].strip()

def safe_int(value) -> int:
    """Garante a conversão para inteiro, tratando o hífen '-' como zero."""
    if not value or str(value).strip() == '-':
        return 0
    try:
        return int(float(str(value).replace(",", ".").strip()))
    except ValueError:
        return 0

def safe_float_or_none(value) -> float | None:
    """Processa notas de avaliação. Devolve None se estiver vazio."""
    if not value or str(value).strip() == '-':
        return None
    try:
        return float(str(value).replace(",", ".").strip())
    except ValueError:
        return None

def extract_date_from_filename(filename: str) -> date:
    """Tenta descobrir a data do Voalle pelo nome do ficheiro."""
    match = re.search(r'(\d{2,4}[-_]?\d{2}[-_]?\d{2,4})', filename)
    if match:
        date_str = match.group(1).replace('_', '').replace('-', '')
        try:
            if len(date_str) == 8:
                if date_str.startswith('20'): # Formato YYYYMMDD
                    return datetime.strptime(date_str, "%Y%m%d").date()
                else: # Formato DDMMYYYY
                    return datetime.strptime(date_str, "%d%m%Y").date()
        except Exception:
            pass
    return date.today() # Fallback para o dia de hoje


# ==========================================
# ENDPOINT DE UPLOAD
# ==========================================

@router.post("/upload-csv")
async def upload_csv(
    file: UploadFile = File(...), 
    data_voalle: str = Form(None),
    db: Session = Depends(get_db)
):
    # AGORA ACEITAMOS XLSX TAMBÉM
    if not file.filename or not file.filename.endswith((".csv", ".xlsx")):
        raise HTTPException(status_code=400, detail="Apenas ficheiros CSV ou XLSX são permitidos.")

    service = IngestionService(db)

    try:
        raw_content = await file.read()
        registros_lidos = []
        headers = []

        # --------------------------------------------------
        # ESTRATÉGIA DE LEITURA: EXCEL (.xlsx)
        # --------------------------------------------------
        if file.filename.endswith(".xlsx"):
            wb = openpyxl.load_workbook(io.BytesIO(raw_content), data_only=True)
            sheet = wb.active
            
            if sheet is None:
                raise HTTPException(status_code=400, detail="Planilha Excel vazia ou inválida.")
            
            # Lê os cabeçalhos da primeira linha
            headers_raw = [cell.value for cell in sheet[1]]
            headers = [str(h).strip() for h in headers_raw if h is not None]
            
            # Transforma as linhas num dicionário para ficar igual ao csv.DictReader
            for row in sheet.iter_rows(min_row=2, values_only=True):
                # Ignora linhas completamente vazias
                if all(cell is None for cell in row):
                    continue
                row_dict = {headers[i]: str(row[i]) if row[i] is not None else "" for i in range(len(headers))}
                registros_lidos.append(row_dict)

        # --------------------------------------------------
        # ESTRATÉGIA DE LEITURA: CSV (.csv)
        # --------------------------------------------------
        else:
            encoding = detect_encoding(raw_content)
            try:
                decoded_content = raw_content.decode(encoding)
            except Exception:
                decoded_content = raw_content.decode("iso-8859-1", errors="replace")

            if decoded_content.startswith("\ufeff"):
                decoded_content = decoded_content[1:]

            separator = detect_separator(decoded_content)
            reader = list(csv.DictReader(io.StringIO(decoded_content), delimiter=separator))
            
            if reader:
                headers = [str(h).strip() for h in reader[0].keys()]
                registros_lidos = reader

        # ==================================================
        # INTELIGÊNCIA DE ROTEAMENTO (Igual a antes)
        # ==================================================
        is_voalle = any(h.upper() in ["CA", "NA", "NSF"] for h in headers)
        is_omnichannel = "Canal de Atendimento" in headers or "Tempo em Espera na Fila" in headers
        is_ligacao = "Data de início" in headers and "Sentido" in headers

        if not (is_voalle or is_omnichannel or is_ligacao):
            raise HTTPException(status_code=400, detail="Formato não reconhecido. Use exportações do Voalle, Omnichannel ou Telefonia.")

        registros = []

        for index, row in enumerate(registros_lidos):
            row = {k.strip(): v.strip() if isinstance(v, str) else v for k, v in row.items() if k is not None}
            
            try:
                # ----------------------------------------------------
                # ROTA 1: PROCESSAMENTO DO VOALLE
                # ----------------------------------------------------
                if is_voalle:
                    data_ref = None
                    if data_voalle:
                        data_ref = datetime.strptime(data_voalle, "%Y-%m-%d").date()
                    else:
                        data_ref = extract_date_from_filename(file.filename)

                    dto = VoalleAgregadoImportSchema(
                        data_referencia=data_ref,
                        colaborador_nome=clean_agent_name(row.get("Atendente", "Desconhecido")),
                        clientes_atendidos=safe_int(row.get("CA")),
                        numero_atendimentos=safe_int(row.get("NA")),
                        solicitacao_finalizada=safe_int(row.get("NSF"))
                    )
                    registros.append(dto)

                # ----------------------------------------------------
                # ROTA 2: PROCESSAMENTO DO OMNICHANNEL (WHATSAPP)
                # ----------------------------------------------------
                elif is_omnichannel:
                    data_str = f"{row.get('Data Inicial', '')} {row.get('Hora Inicial', '')}".strip()
                    data_ref = datetime.strptime(data_str, "%d/%m/%Y %H:%M:%S") if data_str else datetime.now()

                    dto = AtendimentoTransacionalImportSchema(
                        data_referencia=data_ref,
                        colaborador_nome=clean_agent_name(row.get("Nome do Atendente", "Desconhecido")),
                        equipe=row.get("Nome da Equipe"),
                        canal_nome="WhatsApp", # Hardcoded por regra de negócio
                        status_nome=row.get("Status", "Desconhecido"),
                        protocolo=row.get("Número do Protocolo"),
                        sentido_interacao=None,
                        tempo_espera_segundos=parse_time_to_seconds(row.get("Tempo em Espera na Fila") or ""),
                        tempo_atendimento_segundos=parse_time_to_seconds(row.get("Tempo em Atendimento") or ""),
                        nota_solucao=safe_float_or_none(row.get("Avaliação - Nota da Solução Oferecida")),
                        nota_atendimento=safe_float_or_none(row.get("Avaliação - Nota do Atendimento Prestado"))
                    )
                    registros.append(dto)

                # ----------------------------------------------------
                # ROTA 3: PROCESSAMENTO DAS LIGAÇÕES
                # ----------------------------------------------------
                elif is_ligacao:
                    data_ref = datetime.strptime(row.get("Data de início", ""), "%d/%m/%Y %H:%M:%S")

                    dto = AtendimentoTransacionalImportSchema(
                        data_referencia=data_ref,
                        colaborador_nome=clean_agent_name(row.get("Agente", "Desconhecido")),
                        equipe=row.get("Fila"),
                        canal_nome="Ligação",
                        status_nome=row.get("Status", "Desconhecido"),
                        protocolo=row.get("Protocolo"),
                        sentido_interacao=row.get("Sentido"),
                        tempo_espera_segundos=parse_time_to_seconds(row.get("Espera") or ""),
                        tempo_atendimento_segundos=parse_time_to_seconds(row.get("Atendimento") or ""),
                        nota_solucao=None,
                        nota_atendimento=safe_float_or_none(row.get("Avaliação 1"))
                    )
                    registros.append(dto)

            except Exception as e:
                print(f"Erro ao processar linha {index}: {str(e)}")
                continue # Ignora a linha com erro e continua
        
        # Envia os dados higienizados para o banco de dados
        if is_voalle:
            resultado = service.process_voalle_batch(registros)
        else:
            resultado = service.process_transacional_batch(registros)

        if resultado["success_count"] > 0:
            status = "success" if resultado["error_count"] == 0 else "warning"
        else:
            status = "error"

        return {
            "status": status,
            "message": f"{resultado['success_count']} registos importados. {resultado['error_count']} erros.",
            "detalhes": resultado
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno de processamento: {str(e)}")