-- 07_kpi_snapshot.sql
-- Foto de KPIs de ANALYTICS.PROC_FACT_EVENT para esta corrida (base del informe a Slack).

CREATE TABLE IF NOT EXISTS MEETUP_DB.OPS.KPI_SNAPSHOT (
  RUN_TS      TIMESTAMP_NTZ NOT NULL,
  DAG_RUN_ID  VARCHAR(250)  NOT NULL,
  KPI_TYPE    VARCHAR(30)   NOT NULL,
  KPI_KEY     VARCHAR(200)  NOT NULL,
  KPI_VALUE   NUMBER(38,4)
);

DELETE FROM MEETUP_DB.OPS.KPI_SNAPSHOT WHERE DAG_RUN_ID = '{{ run_id }}';

INSERT INTO MEETUP_DB.OPS.KPI_SNAPSHOT (RUN_TS, DAG_RUN_ID, KPI_TYPE, KPI_KEY, KPI_VALUE)
WITH
cur AS (SELECT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS TS),

prev AS (
  SELECT MAX(s.RUN_TS) AS TS
  FROM MEETUP_DB.OPS.KPI_SNAPSHOT s CROSS JOIN cur
  WHERE s.DAG_RUN_ID <> '{{ run_id }}' AND s.RUN_TS < cur.TS
),

dim AS (
  SELECT GROUP_ID, CATEGORY_NAME, CITY
  FROM {{ params.dim_group_table | default('MEETUP_DB.AUX.DIM_GROUP') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY GROUP_ID ORDER BY CATEGORY_NAME, CITY) = 1
),

ev AS (
  SELECT f.YES_RSVP_COUNT, f.IS_SIMULATED, f.CREATED_AT, f.UPDATED_AT,
         COALESCE(LOWER(TRIM(f.EVENT_STATUS)), '(sin estado)') AS STATUS_KEY,
         COALESCE(d.CATEGORY_NAME, '(sin categoria)')          AS CATEGORY_KEY,
         COALESCE(d.CITY, '(sin ciudad)')                      AS CITY_KEY
  FROM MEETUP_DB.ANALYTICS.PROC_FACT_EVENT f
  LEFT JOIN dim d ON f.GROUP_ID = d.GROUP_ID
),

tot AS (
  SELECT
    COUNT(*)                                                                    AS N,
    COUNT_IF(COALESCE(IS_SIMULATED, FALSE))                                     AS N_SIM,
    COALESCE(SUM(YES_RSVP_COUNT), 0)                                            AS RSVP,
    AVG(YES_RSVP_COUNT)                                                         AS RSVP_AVG,
    COUNT_IF(STATUS_KEY IN ('cancelled', 'canceled'))                           AS N_CANC,
    COUNT_IF(STATUS_KEY IN ('cancelled', 'canceled')
             AND COALESCE(IS_SIMULATED, FALSE))                                 AS N_CANC_SIM
  FROM ev
),

prev_canc AS (
  SELECT MAX(s.KPI_VALUE) AS V
  FROM MEETUP_DB.OPS.KPI_SNAPSHOT s
  JOIN prev p ON s.RUN_TS = p.TS
  WHERE s.KPI_TYPE = 'TOTAL' AND s.KPI_KEY = 'N_CANC'
),

mv AS (
  SELECT
    IFF(p.TS IS NULL, NULL, COUNT_IF(e.CREATED_AT > p.TS))                              AS NEW_EVENTS,
    IFF(p.TS IS NULL, NULL, COUNT_IF(e.UPDATED_AT > p.TS AND e.CREATED_AT <= p.TS))     AS UPDATED_EVENTS,
    IFF(p.TS IS NULL, NULL, MAX(t.N_CANC) - MAX(pc.V))                                  AS CANCELLED_IN_RUN
  FROM prev p
  CROSS JOIN tot t
  CROSS JOIN prev_canc pc
  LEFT JOIN ev e ON TRUE
  GROUP BY p.TS
),

kpis AS (
  SELECT 'TOTAL' AS T, 'N'            AS K, N::NUMBER(38,4)               AS V FROM tot
  UNION ALL SELECT 'TOTAL', 'N_SIM',        N_SIM::NUMBER(38,4)             FROM tot
  UNION ALL SELECT 'TOTAL', 'N_REAL',       (N - N_SIM)::NUMBER(38,4)       FROM tot
  UNION ALL SELECT 'TOTAL', 'RSVP',         RSVP::NUMBER(38,4)              FROM tot
  UNION ALL SELECT 'TOTAL', 'RSVP_AVG',     RSVP_AVG::NUMBER(38,4)          FROM tot
  UNION ALL SELECT 'TOTAL', 'N_CANC',       N_CANC::NUMBER(38,4)            FROM tot
  UNION ALL SELECT 'TOTAL', 'N_CANC_SIM',   N_CANC_SIM::NUMBER(38,4)        FROM tot
  UNION ALL SELECT 'TOTAL', 'N_CANC_REAL',  (N_CANC - N_CANC_SIM)::NUMBER(38,4) FROM tot
  UNION ALL SELECT 'MOVEMENT', 'NEW_EVENTS',       NEW_EVENTS::NUMBER(38,4)       FROM mv
  UNION ALL SELECT 'MOVEMENT', 'UPDATED_EVENTS',   UPDATED_EVENTS::NUMBER(38,4)   FROM mv
  UNION ALL SELECT 'MOVEMENT', 'CANCELLED_IN_RUN', CANCELLED_IN_RUN::NUMBER(38,4) FROM mv
  UNION ALL SELECT 'STATUS',   STATUS_KEY,   COUNT(*)::NUMBER(38,4) FROM ev GROUP BY STATUS_KEY
  UNION ALL SELECT 'CATEGORY', CATEGORY_KEY, COUNT(*)::NUMBER(38,4) FROM ev GROUP BY CATEGORY_KEY
  UNION ALL SELECT 'CITY',     CITY_KEY,     COUNT(*)::NUMBER(38,4) FROM ev GROUP BY CITY_KEY
)

SELECT cur.TS, '{{ run_id }}', kpis.T, kpis.K, kpis.V FROM kpis CROSS JOIN cur;
