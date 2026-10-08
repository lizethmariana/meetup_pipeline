-- Genera en AUX.STG_NEW_EVENTS eventos nuevos y actualizaciones de eventos simulados.
CREATE OR REPLACE TRANSIENT TABLE MEETUP_DB.AUX.STG_NEW_EVENTS AS
WITH new_events AS (
  SELECT
    'SIM_' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS') || '_'
      || ROW_NUMBER() OVER (ORDER BY RANDOM())                  AS EVENT_ID,
    GROUP_ID,
    'Simulated meetup ' || UNIFORM(1, 9999, RANDOM())           AS EVENT_NAME,
    IFF(UNIFORM(1, 10, RANDOM()) > 2, 'upcoming', 'cancelled')  AS EVENT_STATUS,
    UNIFORM(5, 300, RANDOM())                                   AS YES_RSVP_COUNT,
    TRUE                                                        AS IS_SIMULATED,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ                          AS CREATED_AT
  FROM MEETUP_DB.AUX.DIM_GROUP SAMPLE (25 ROWS)
),
updates AS (
  SELECT EVENT_ID, GROUP_ID, EVENT_NAME, EVENT_STATUS,
         YES_RSVP_COUNT + UNIFORM(1, 20, RANDOM())              AS YES_RSVP_COUNT,
         IS_SIMULATED, CREATED_AT
  FROM MEETUP_DB.ANALYTICS.PROC_FACT_EVENT
  WHERE IS_SIMULATED
  QUALIFY ROW_NUMBER() OVER (ORDER BY RANDOM()) <= 5
)
SELECT * FROM new_events
UNION ALL
SELECT * FROM updates;
