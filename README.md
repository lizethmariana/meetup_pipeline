# Meetup Pipeline — Prueba técnica Data Engineer

Pipeline orquestado con **Apache Airflow 3 (Astro Runtime, ejecución local con Docker/Colima)** que ingiere el dataset público de Meetup (Kaggle) en **Snowflake** (`MEETUP_DB`), construye capas auxiliares y analíticas, calcula métricas de eventos y de miembros, envía **un informe único por corrida a Slack** y **exporta los agregados a S3 en Parquet**.

## Objetivo

Demostrar un ciclo completo de ingeniería de datos: ingesta incremental, modelado por capas (RAW → AUX → ANALYTICS → OPS), calidad de datos, observabilidad (Slack) y entrega a un data lake (S3), con tratamiento responsable de datos personales.

## Arquitectura

```mermaid
flowchart LR
    subgraph fuentes
        K[Dataset Kaggle<br/>meetup CSVs] --> RAW
        M[members.csv<br/>5.893.886 filas<br/>6 trozos] --> MI
    end

    subgraph snowflake[Snowflake MEETUP_DB]
        RAW[(RAW<br/>CITIES, CATEGORIES, TOPICS,<br/>GROUPS, EVENTS, VENUES,<br/>GROUPS_TOPICS, MEMBERS_TOPICS,<br/>MEMBERS)]
        AUX[(AUX<br/>DIM_CITY, DIM_CATEGORY, DIM_GROUP,<br/>DIM_VENUE, FACT_EVENT,<br/>STG_NEW_EVENTS, DIM_MEMBER_GROUP)]
        AN[(ANALYTICS<br/>PROC_FACT_EVENT, PROC_GROUP_METRICS,<br/>PROC_CITY_METRICS, PROC_MEMBER_METRICS,<br/>PROC_MEMBER_RECONCILIATION)]
        OPS[(OPS<br/>PIPELINE_RUN_LOG, KPI_SNAPSHOT,<br/>INGEST_CONTROL, MEMBER_METRICS_STATE)]
        STAGE[@AUX.S3_EXPORT_STAGE<br/>Storage Integration]
        RAW --> AUX --> AN
        AN --> OPS
    end

    subgraph airflow[Airflow 3 - cada 15 min]
        MI[members_ingest<br/>PUT + COPY INTO<br/>1 trozo/corrida]
        MP[meetup_pipeline<br/>generate > merge > metrics<br/>> dq > kpi > log > export+report]
        MB[members_gate<br/>ShortCircuit]
    end

    MI -->|PUT + COPY| RAW
    MP -->|SQL| AUX & AN & OPS
    AN --> STAGE --> S3[S3<br/>Parquet por fecha/hora]
    OPS -->|informe| SL[Slack webhook]
```

Diagramas adicionales en `docs/img/`: `arquitectura.png`, `dag.png`, `dag_members.png`, `linaje.png`, `er.png`.

## Los 2 DAGs

### `meetup_pipeline` (cada 15 min)

```
generate_new_events >> merge_fact_event >> [build_group_metrics, build_city_metrics] >> data_quality_check >> kpi_snapshot >> log_run
merge >> members_gate >> build_dim_member_group >> member_dq_check >> build_member_metrics >> log_run
log_run >> [export_to_s3, slack_insights_report]
```

- `generate_new_events` (`dags/sql/01`): crea `AUX.STG_NEW_EVENTS` con eventos nuevos (IDs `SIM_*`) y actualizaciones de eventos simulados existentes — **datos nuevos en cada corrida**.
- `merge_fact_event` (`02`): **MERGE** idempotente contra `ANALYTICS.PROC_FACT_EVENT`.
- `build_group_metrics` / `build_city_metrics` (`03`, `04`): **CREATE OR REPLACE** de agregados.
- `data_quality_check`: unicidad de `EVENT_ID`.
- `kpi_snapshot` (`07`): foto de ~55 KPIs en `OPS.KPI_SNAPSHOT` (base del informe).
- `log_run` (`05`): registro en `OPS.PIPELINE_RUN_LOG`.
- Rama de miembros: `members_gate` (**ShortCircuitOperator**) solo deja calcular si `OPS.INGEST_CONTROL` muestra los 6 trozos cargados y `RAW.MEMBERS` = 5.893.886 filas; si no, la rama se salta y el informe dice "carga de members en curso: X/6". Si nada cambió desde la última corrida (`OPS.MEMBER_METRICS_STATE`), tampoco recalcula.
- `export_to_s3` (`06`) y `slack_insights_report` corren **en paralelo** tras `log_run`: un fallo del informe no bloquea la exportación.

### `members_ingest` (cada 15 min)

Ingesta **incremental** de `members.csv` en 6 trozos de ~1M filas (`include/members_chunks/`): `PUT` al stage interno `RAW.STG_MEMBERS` + `COPY INTO RAW.MEMBERS` (columnas TEXT), control idempotente en `OPS.INGEST_CONTROL`, un trozo por corrida, aviso a Slack por trozo y validación final contra el total esperado.

## Mapeo a los requisitos del enunciado

| # | Requisito | Dónde | Evidencia |
|---|---|---|---|
| 1 | Cuenta Snowflake | Conexión `snowflake_default`; base `MEETUP_DB` | UI de Airflow → Connections; Snowsight |
| 2 | Carga del dataset de Kaggle | RAW cargado a mano; `dags/members_ingest.py` + `snowflake_setup/07_members_ingest.sql` | `OPS.INGEST_CONTROL`; `SELECT COUNT(*) FROM RAW.MEMBERS` |
| 3 | Tablas físicas auxiliares | `AUX.DIM_*`, `AUX.STG_NEW_EVENTS`, `AUX.DIM_MEMBER_GROUP` | `snowflake_setup/09_member_analytics.sql`, `dags/sql/08` |
| 4 | DAG cada 15 min con MERGE / CREATE OR REPLACE / COPY y datos nuevos | `dags/meetup_pipeline.py`, `dags/sql/01-10` | Grid del DAG en verde; `OPS.PIPELINE_RUN_LOG` |
| 5 | Alertas a Slack | Callbacks por tarea/DAG + informe único (`include/slack_report.py`) | Canal de Slack; `OPS.KPI_SNAPSHOT` |
| 6 | Exportación a S3 | `dags/sql/06_export_s3.sql` con `@AUX.S3_EXPORT_STAGE` (Storage Integration, sin claves en código) | `LIST @MEETUP_DB.AUX.S3_EXPORT_STAGE` en Snowsight |

## El informe de Slack (un mensaje por corrida)

`include/slack_report.py` construye un único mensaje mrkdwn (< 3.500 caracteres) con: estado de la corrida, movimiento de eventos (nuevos, actualizados, cancelaciones), KPIs con ▲/▼/▬ frente a la corrida anterior, distribución por estado, top 3 categorías y ciudades, sección de miembros (o progreso de carga X/6) y alertas. Si excede el límite se descartan secciones de menor prioridad (miembros → estados → rankings).

**Alertas y umbrales** (constantes al inicio de `include/slack_report.py`):

| Constante | Valor | Alerta |
|---|---|---|
| `RSVP_AVG_DROP_PCT` | 10 | el RSVP promedio cae >= 10 % |
| `CANCELLED_IN_RUN_ALERT` | 3 | >= 3 cancelaciones en una sola corrida |
| (sin constante) | | no entraron eventos nuevos |
| `MIN_CELL` | 5 | supresión: celdas con < 5 miembros se agrupan como "otros" |
| `MAX_MESSAGE_CHARS` | 3500 | límite del mensaje |

Las notificaciones de **éxito por tarea** están controladas por la variable de Airflow `ALERT_TASK_SUCCESS` (**apagada por defecto**; `airflow variables set ALERT_TASK_SUCCESS true` para activarlas). Los **fallos siempre se avisan**, por tarea y por DAG.

## Decisiones de diseño

- **Capas RAW / AUX / ANALYTICS / OPS**: crudo inmutable, dimensiones/limpieza, métricas de negocio y operativa/observabilidad. Cada capa se puede reconstruir desde la anterior.
- **MERGE incremental** para eventos (no truncate-and-load) y **CREATE OR REPLACE** para agregados (idempotente y simple a este volumen).
- **Ingesta troceada de members**: la UI/stage tiene límites prácticos (~250 MB) y el CSV original es grande; 6 trozos de ~1M filas con control idempotente en `OPS.INGEST_CONTROL` permiten reintentos sin duplicar.
- **Storage Integration** para S3: credenciales AWS viven en Snowflake, no en el código ni en conexiones de Airflow.
- **Usuario de servicio (`AIRFLOW_SVC`) con autenticación por clave** (par de claves) en lugar de usuario/contraseña.
- **Puerta lógica + estado** (`members_gate` + `OPS.MEMBER_METRICS_STATE`): las métricas de miembros solo se recalculan cuando hay datos nuevos, no en cada corrida.
- **Cuarentena de calidad**: chequeos SQL (`SQLCheckOperator`) antes de publicar; la dimensión de miembros valida claves nulas, duplicados y % de huérfanos.
- **Supresión de celdas pequeñas**: ningún agregado derivado de members publica celdas con menos de 5 miembros.

## Tratamiento de datos personales

`members.csv` contiene datos de personas reales (nombre, bio, foto, enlaces, coordenadas). Medidas:

- `RAW.MEMBERS` se conserva intacto **solo como respaldo**; nada de él sale hacia ANALYTICS, S3, Slack, logs o documentación.
- `AUX.DIM_MEMBER_GROUP` **excluye explícitamente** `member_name`, `bio`, `link`, `lat`, `lon` y `hometown`; conserva solo identificadores pseudónimos, ciudad/estado/país, fechas y estado de membresía.
- Todo lo que se publica (métricas, informe, exportación) es **agregado**, con supresión de celdas < 5 miembros.
- `.gitignore` y `.dockerignore` excluyen `archive/`, `include/members_chunks/`, `include/keys/`, `.env` y `airflow_settings.yaml`.

## Cómo reproducir

```bash
# 1. Arrancar Airflow local (Astro CLI + Docker/Colima)
astro dev start          # UI en http://localhost:8080 (u otro puerto si 8080 esta ocupado)

# 2. DDL y permisos en Snowflake (en Snowsight, con ACCOUNTADMIN, en este orden)
#    snowflake_setup/09_member_analytics.sql   -> tablas vacias de miembros (export_to_s3 las necesita)
#    snowflake_setup/07_members_ingest.sql     -> stage y file format de members
#    snowflake_setup/08_grants_role_airflow.sql -> permisos de ROLE_AIRFLOW
#    (las verificaciones de 09 que consultan RAW.MEMBERS fallan hasta el primer trozo: es normal)

# 3. Despausar los DAGs (members_ingest carga 1 trozo por corrida)
airflow dags unpause meetup_pipeline
airflow dags unpause members_ingest

# 4. Verificaciones
astro dev run dags list-import-errors
astro dev pytest tests/unit
```

## Problemas encontrados y cómo se resolvieron

- **Codificaciones mixtas**: `topics.csv` y `groups_topics.csv` vienen en cp1252 (file format `FF_CSV_MEETUP_LATIN1`); el resto en UTF-8. Sin ello, la carga fallaba o corrompía acentos.
- **Límite de ~250 MB y troceado de members**: la carga por la UI no era viable; se partió el CSV en 6 trozos y se automatizó la ingesta incremental con control propio.
- **DNS corporativos en Colima**: la red corporativa bloquea DNS externos; se configuraron los DNS internos en `~/.colima/default/colima.yaml`.
- **Port-forwarding de Colima roto por el EDR**: el agente de seguridad mata los procesos `ssh -O forward` de Lima; workaround con relay `socat` + registro del forward en gvproxy (o migrar a Docker Desktop).
- **External ID al recrear la Storage Integration**: al recrearla cambia el `EXTERNAL_ID` y hay que actualizar la trust policy del rol de AWS; documentado para no perder horas.
- **`ds` no disponible en corridas manuales (Airflow 3)**: todo el código usa `run_id` / `dag_run`, nunca `ds`.

## Limitaciones honestas

- Los eventos "nuevos" son **simulados** (IDs `SIM_*`) sobre una base real de 5.807 eventos de 2017-2018: es la forma de demostrar el MERGE con datos que cambian.
- Los 5.807 eventos reales caen solo en **3 categorías** y 341 grupos; los rankings dominan esas categorías.
- El dataset es un **snapshot de 2017-2018**; no refleja el estado actual de Meetup.
- El extracto de members está **muestreado** (~27 k miembros únicos por cada 200 k filas): la conciliación contra `GROUPS.MEMBERS` declarado muestra sesgo esperable, documentado en `PROC_MEMBER_RECONCILIATION`.
- `maybe_rsvp`, `headcount` y `rating.average` llegan a 0 en el dataset: no hay métricas posibles sobre ellos.
- La tabla `OPS.KPI_SNAPSHOT` crece ~5.000 filas/día sin purga automática.

## Cómo llevarlo a producción

- **Snowflake Tasks/Streams** en lugar del DAG para las cargas dependientes de datos nuevos, con el DAG como orquestador de alto nivel.
- **dbt** para la capa ANALYTICS (tests de calidad, documentación de columnas, lineage).
- **Secretos en un gestor** (Secret Manager / Vault) en lugar de conexiones locales; conexiones de Airflow aprovisionadas por IaC.
- **CI/CD**: deploy a Astronomer/Composer desde CI, con `astro dev pytest` y revisión de secretos como gates.
- **Particionado y purga**: retención definida para `OPS.KPI_SNAPSHOT` y logs de ejecución.

## Estructura del proyecto

```
dags/
  meetup_pipeline.py        # DAG principal (eventos + metricas + informe + export)
  members_ingest.py         # ingesta incremental de members.csv
  sql/01..10                # SQL de cada tarea (Snowflake)
include/
  slack_report.py           # informe de Slack (capa pura + capa con efectos)
  members_chunks/           # trozos de members.csv (NO versionado)
snowflake_setup/            # DDL/grants que se corren a mano en Snowsight
tests/unit/                 # pytest sin Snowflake ni Slack
docs/img/                   # diagramas (arquitectura, DAGs, linaje, ER)
```
