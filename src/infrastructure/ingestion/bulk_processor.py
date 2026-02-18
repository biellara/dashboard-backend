from sqlalchemy import text
from src.infrastructure.database.config import engine
import pandas as pd

def process_pending_uploads():
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT * FROM uploads WHERE status = 'pending'")
        )

        uploads = result.fetchall()

        for upload in uploads:
            try:
                conn.execute(
                    text("UPDATE uploads SET status='processing' WHERE id=:id"),
                    {"id": upload.id}
                )

                df = pd.read_excel(upload.file_path)

                # tratamento vetorizado
                df.columns = df.columns.str.lower()

                # bulk insert via COPY
                cursor = conn.connection.cursor()
                try:
                    from io import StringIO

                    buffer = StringIO()
                    df.to_csv(buffer, index=False, header=False)
                    buffer.seek(0)

                    cursor.copy_expert(
                        "COPY minha_tabela FROM STDIN WITH CSV",
                        buffer
                    )
                finally:
                    cursor.close()

                conn.commit()

                conn.execute(
                    text("UPDATE uploads SET status='completed' WHERE id=:id"),
                    {"id": upload.id}
                )

            except Exception as e:
                conn.execute(
                    text("UPDATE uploads SET status='error', error=:err WHERE id=:id"),
                    {"id": upload.id, "err": str(e)}
                )
                conn.commit()
