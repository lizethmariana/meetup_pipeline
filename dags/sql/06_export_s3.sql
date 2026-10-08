-- Exportacion de agregados a S3 en Parquet, particionada por fecha y hora de la corrida.
COPY INTO @MEETUP_DB.AUX.S3_EXPORT_STAGE/proc_fact_event/{{ dag_run.start_date.strftime('%Y/%m/%d/%H%M%S') }}/
FROM MEETUP_DB.ANALYTICS.PROC_FACT_EVENT
FILE_FORMAT = (TYPE = PARQUET)
HEADER = TRUE
OVERWRITE = TRUE;

COPY INTO @MEETUP_DB.AUX.S3_EXPORT_STAGE/proc_group_metrics/{{ dag_run.start_date.strftime('%Y/%m/%d/%H%M%S') }}/
FROM MEETUP_DB.ANALYTICS.PROC_GROUP_METRICS
FILE_FORMAT = (TYPE = PARQUET)
HEADER = TRUE
OVERWRITE = TRUE;

COPY INTO @MEETUP_DB.AUX.S3_EXPORT_STAGE/proc_city_metrics/{{ dag_run.start_date.strftime('%Y/%m/%d/%H%M%S') }}/
FROM MEETUP_DB.ANALYTICS.PROC_CITY_METRICS
FILE_FORMAT = (TYPE = PARQUET)
HEADER = TRUE
OVERWRITE = TRUE;

COPY INTO @MEETUP_DB.AUX.S3_EXPORT_STAGE/proc_member_metrics/{{ dag_run.start_date.strftime('%Y/%m/%d/%H%M%S') }}/
FROM MEETUP_DB.ANALYTICS.PROC_MEMBER_METRICS
FILE_FORMAT = (TYPE = PARQUET)
HEADER = TRUE
OVERWRITE = TRUE;

COPY INTO @MEETUP_DB.AUX.S3_EXPORT_STAGE/proc_member_reconciliation/{{ dag_run.start_date.strftime('%Y/%m/%d/%H%M%S') }}/
FROM MEETUP_DB.ANALYTICS.PROC_MEMBER_RECONCILIATION
FILE_FORMAT = (TYPE = PARQUET)
HEADER = TRUE
OVERWRITE = TRUE;

COPY INTO @MEETUP_DB.AUX.S3_EXPORT_STAGE/kpi_snapshot/{{ dag_run.start_date.strftime('%Y/%m/%d/%H%M%S') }}/
FROM (SELECT * FROM MEETUP_DB.OPS.KPI_SNAPSHOT WHERE DAG_RUN_ID = '{{ run_id }}')
FILE_FORMAT = (TYPE = PARQUET)
HEADER = TRUE
OVERWRITE = TRUE;
