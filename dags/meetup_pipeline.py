"""Pipeline de eventos y miembros de Meetup en Snowflake, cada 15 minutos.

Eventos: generate -> MERGE -> metricas (grupo/ciudad) -> DQ -> snapshot de KPIs -> log.
Miembros: puerta logica -> dimension sin datos personales -> DQ -> metricas agregadas.
Al final, la exportacion a S3 y el informe a Slack corren en paralelo.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator

from include.slack_report import send_insights_report

SNOWFLAKE_CONN = "snowflake_default"
SLACK_CONN = "slack_webhook"
DIM_GROUP_TABLE = "MEETUP_DB.AUX.DIM_GROUP"

INGEST_CONTROL = "MEETUP_DB.OPS.INGEST_CONTROL"
MEMBERS_STATE_TABLE = "MEETUP_DB.OPS.MEMBER_METRICS_STATE"
MEMBERS_EXPECTED_CHUNKS = 6
MEMBERS_EXPECTED_ROWS = 5_893_886


def notify_task_ok_if_enabled(context):
    """Exito por tarea a Slack solo si la variable ALERT_TASK_SUCCESS es true (apagada por defecto).
    Los fallos siempre se avisan (on_failure_callback no cambia)."""
    from airflow.models import Variable
    try:
        enabled = Variable.get("ALERT_TASK_SUCCESS", default_var="false").strip().lower() in ("true", "1", "yes")
    except Exception:
        enabled = False
    if enabled:
        notify_task_ok(context)


def members_ready(**context):
    """ShortCircuit: las metricas de miembros solo se calculan cuando la ingesta esta completa
    (6/6 trozos y las filas esperadas en RAW.MEMBERS) y algo cambio desde la ultima vez.
    Si no, la tarea se salta y el informe mostrara 'carga de members en curso: X/6'."""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
    try:
        loaded = hook.get_first(
            f"SELECT COUNT(*) FROM {INGEST_CONTROL} WHERE STATUS = 'LOADED'")[0]
    except Exception:
        print("carga de members en curso: 0/6 (OPS.INGEST_CONTROL todavia no existe)")
        return False
    if int(loaded) < MEMBERS_EXPECTED_CHUNKS:
        print(f"carga de members en curso: {loaded}/{MEMBERS_EXPECTED_CHUNKS}")
        return False
    total = int(hook.get_first("SELECT COUNT(*) FROM MEETUP_DB.RAW.MEMBERS")[0])
    if total != MEMBERS_EXPECTED_ROWS:
        print(f"RAW.MEMBERS tiene {total} filas, se esperaban {MEMBERS_EXPECTED_ROWS:,}; "
              "no se calculan las metricas")
        return False
    try:
        last = hook.get_first(f"SELECT LAST_ROW_COUNT FROM {MEMBERS_STATE_TABLE}")
    except Exception:
        last = None
    if last and int(last[0] or 0) == total:
        print("members sin cambios desde la ultima construccion; se omite el recalculo")
        return False
    return True

notify_task_ok = SlackWebhookNotifier(
    slack_webhook_conn_id=SLACK_CONN,
    text=":white_check_mark: *{{ dag.dag_id }}* | tarea `{{ ti.task_id }}` completada "
         "(intento {{ ti.try_number }}) | corrida `{{ run_id }}`",
)
notify_task_failed = SlackWebhookNotifier(
    slack_webhook_conn_id=SLACK_CONN,
    text=":x: *{{ dag.dag_id }}* | tarea `{{ ti.task_id }}` FALLO "
         "(intento {{ ti.try_number }}) | corrida `{{ run_id }}`\n```{{ exception }}```",
)
notify_dag_ok = SlackWebhookNotifier(
    slack_webhook_conn_id=SLACK_CONN,
    text=":checkered_flag: *{{ dag.dag_id }}* | corrida completa OK | `{{ run_id }}`",
)
notify_dag_failed = SlackWebhookNotifier(
    slack_webhook_conn_id=SLACK_CONN,
    text=":rotating_light: *{{ dag.dag_id }}* | la corrida terminó con ERRORES | `{{ run_id }}`",
)

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_success_callback": notify_task_ok_if_enabled,
    "on_failure_callback": notify_task_failed,
}

with DAG(
    dag_id="meetup_pipeline",
    description="Genera eventos, MERGE en Snowflake y recalcula metricas cada 15 min",
    start_date=datetime(2025, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_success_callback=notify_dag_ok,
    on_failure_callback=notify_dag_failed,
    tags=["snowflake", "meetup"],
) as dag:

    def sql_task(task_id: str, filename: str) -> SQLExecuteQueryOperator:
        return SQLExecuteQueryOperator(
            task_id=task_id,
            conn_id=SNOWFLAKE_CONN,
            sql=f"sql/{filename}",
            split_statements=True,
        )

    generate = sql_task("generate_new_events", "01_generate_new_events.sql")
    merge = sql_task("merge_fact_event", "02_merge_fact_event.sql")
    group_metrics = sql_task("build_group_metrics", "03_group_metrics.sql")
    city_metrics = sql_task("build_city_metrics", "04_city_metrics.sql")

    data_quality = SQLCheckOperator(
        task_id="data_quality_check",
        conn_id=SNOWFLAKE_CONN,
        sql="""
            SELECT COUNT(*) > 0 AND COUNT(*) = COUNT(DISTINCT EVENT_ID)
            FROM MEETUP_DB.ANALYTICS.PROC_FACT_EVENT
        """,
    )

    log_run = sql_task("log_run", "05_log_run.sql")

    export = sql_task("export_to_s3", "06_export_s3.sql")

    kpi_snapshot = SQLExecuteQueryOperator(
        task_id="kpi_snapshot",
        conn_id=SNOWFLAKE_CONN,
        sql="sql/07_kpi_snapshot.sql",
        split_statements=True,
        params={"dim_group_table": DIM_GROUP_TABLE},
    )

    slack_insights_report = PythonOperator(
        task_id="slack_insights_report",
        python_callable=send_insights_report,
        op_kwargs={"dag_id": "{{ dag.dag_id }}", "run_id": "{{ run_id }}"},
        retries=1,
    )

    members_gate = ShortCircuitOperator(
        task_id="members_gate",
        python_callable=members_ready,
        retries=1,
    )
    build_dim_member_group = sql_task("build_dim_member_group", "08_dim_member_group.sql")
    member_dq_check = SQLCheckOperator(
        task_id="member_dq_check",
        conn_id=SNOWFLAKE_CONN,
        sql="sql/09_member_dq_check.sql",
    )
    build_member_metrics = sql_task("build_member_metrics", "10_member_metrics.sql")

    generate >> merge >> [group_metrics, city_metrics] >> data_quality >> kpi_snapshot >> log_run
    merge >> members_gate >> build_dim_member_group >> member_dq_check >> build_member_metrics >> log_run
    log_run >> [export, slack_insights_report]
