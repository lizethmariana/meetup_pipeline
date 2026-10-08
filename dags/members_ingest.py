"""Ingesta incremental de members.csv: un trozo por corrida (PUT al stage + COPY INTO)."""
import csv
import glob
import os
import re
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator

SNOWFLAKE_CONN = "snowflake_default"
SLACK_CONN = "slack_webhook"

CHUNK_DIR = "/usr/local/airflow/include/members_chunks"
STAGE = "MEETUP_DB.RAW.STG_MEMBERS"
TABLE = "MEETUP_DB.RAW.MEMBERS"
CONTROL = "MEETUP_DB.OPS.INGEST_CONTROL"
FILE_FORMAT = "MEETUP_DB.RAW.FF_CSV_MEMBERS"

EXPECTED_CHUNKS = 6
EXPECTED_TOTAL_ROWS = 5_893_886

notify_failed = SlackWebhookNotifier(
    slack_webhook_conn_id=SLACK_CONN,
    text=":x: *{{ dag.dag_id }}* | tarea `{{ ti.task_id }}` FALLO (intento {{ ti.try_number }}) "
         "| corrida `{{ run_id }}`\n```{{ exception }}```",
)


def _columns(path):
    """Nombres de columna seguros para Snowflake, a partir de la cabecera del CSV."""
    with open(path, encoding="utf-8", newline="") as f:
        header = next(csv.reader(f))
    cols, seen = [], {}
    for i, h in enumerate(header):
        name = re.sub(r"[^A-Za-z0-9_]", "_", h.strip()).upper().strip("_") or f"COL_{i + 1}"
        if name[0].isdigit():
            name = "C_" + name
        seen[name] = seen.get(name, 0) + 1
        if seen[name] > 1:
            name = f"{name}_{seen[name]}"
        cols.append(name)
    return cols


def _slack(text):
    try:
        SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN).send(text=text)
    except Exception as exc:
        print(f"No se pudo enviar el aviso a Slack: {exc}")


def ingest_next_chunk(**context):
    run_id = context["run_id"]
    files = sorted(glob.glob(os.path.join(CHUNK_DIR, "members_part_*.csv")))
    if not files:
        raise AirflowFailException(
            f"No hay trozos en {CHUNK_DIR}. Copia members_part_*.csv a include/members_chunks/"
        )

    conn = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN).get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS {CONTROL} (
                FILE_NAME VARCHAR, STATUS VARCHAR, ROWS_LOADED NUMBER, ERRORS_SEEN NUMBER,
                DAG_RUN_ID VARCHAR, STARTED_AT TIMESTAMP_NTZ, FINISHED_AT TIMESTAMP_NTZ,
                DETAIL VARCHAR)"""
        )
        cur.execute(f"SELECT FILE_NAME, ROWS_LOADED FROM {CONTROL} WHERE STATUS = 'LOADED'")
        loaded_before = cur.fetchall()
        done = {r[0] for r in loaded_before}
        rows_before = sum(int(r[1] or 0) for r in loaded_before)

        pending = [f for f in files if os.path.basename(f) not in done]
        if not pending:
            print("Ingesta completa: no hay trozos pendientes.")
            return "complete"

        path = pending[0]
        name = os.path.basename(path)
        position = len(done) + 1
        started = datetime.utcnow()

        cols = _columns(path)
        cur.execute(f"CREATE TABLE IF NOT EXISTS {TABLE} ({', '.join(c + ' TEXT' for c in cols)})")

        try:
            cur.execute(f"LIST @{STAGE}/{name}")
            if not cur.fetchall():
                print(f"Subiendo {name} al stage...")
                cur.execute(f"PUT 'file://{path}' @{STAGE} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

            cur.execute(
                f"""COPY INTO {TABLE} FROM @{STAGE}
                    FILES = ('{name}.gz')
                    FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}')
                    ON_ERROR = 'ABORT_STATEMENT'"""
            )
            names = [d[0].lower() for d in cur.description]
            result = [dict(zip(names, r)) for r in cur.fetchall()]
            ok = [r for r in result if str(r.get("status", "")).upper() == "LOADED"]
            rows_loaded = sum(int(r.get("rows_loaded") or 0) for r in ok)
            errors = sum(int(r.get("errors_seen") or 0) for r in ok)

            cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
            total = int(cur.fetchone()[0])
            if not ok:
                rows_loaded = total - rows_before
                detail = "El trozo ya estaba cargado; se reconcilio con el conteo de la tabla"
            else:
                detail = "OK"
        except Exception as exc:
            cur.execute(
                f"""INSERT INTO {CONTROL} (FILE_NAME, STATUS, ROWS_LOADED, ERRORS_SEEN, DAG_RUN_ID,
                    STARTED_AT, FINISHED_AT, DETAIL)
                    VALUES (%(f)s, 'FAILED', 0, 0, %(d)s, %(a)s, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, %(x)s)""",
                {"f": name, "d": run_id, "a": started, "x": str(exc)[:500]},
            )
            raise

        cur.execute(
            f"""INSERT INTO {CONTROL} (FILE_NAME, STATUS, ROWS_LOADED, ERRORS_SEEN, DAG_RUN_ID,
                STARTED_AT, FINISHED_AT, DETAIL)
                VALUES (%(f)s, 'LOADED', %(r)s, %(e)s, %(d)s, %(a)s, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, %(x)s)""",
            {"f": name, "r": rows_loaded, "e": errors, "d": run_id, "a": started, "x": detail},
        )
    finally:
        conn.close()

    lines = [
        ":inbox_tray: *members_ingest* | trozo "
        f"`{name}` ({position}/{len(files)}) cargado",
        f"• Filas de este trozo: *{rows_loaded:,}*  · errores: *{errors}*",
        f"• Total en `RAW.MEMBERS`: *{total:,}*",
    ]
    if position == len(files):
        status = ":white_check_mark:" if total == EXPECTED_TOTAL_ROWS else ":warning:"
        lines.append(f"{status} Carga completa. Esperadas: {EXPECTED_TOTAL_ROWS:,} filas.")
    _slack("\n".join(lines))
    return name


with DAG(
    dag_id="members_ingest",
    description="Ingesta incremental de members.csv (un trozo por corrida) a Snowflake",
    start_date=datetime(2025, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": notify_failed,
    },
    tags=["snowflake", "meetup", "ingesta"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_next_chunk",
        python_callable=ingest_next_chunk,
        execution_timeout=timedelta(minutes=30),
    )

    validate = SQLCheckOperator(
        task_id="validate_members",
        conn_id=SNOWFLAKE_CONN,
        sql=f"""
            SELECT
              (SELECT COUNT(*) FROM {TABLE})
                 = COALESCE((SELECT SUM(ROWS_LOADED) FROM {CONTROL} WHERE STATUS = 'LOADED'), 0)
              AND (
                (SELECT COUNT(*) FROM {CONTROL} WHERE STATUS = 'LOADED') < {EXPECTED_CHUNKS}
                OR (SELECT COUNT(*) FROM {TABLE}) = {EXPECTED_TOTAL_ROWS}
              )
        """,
    )

    ingest >> validate
