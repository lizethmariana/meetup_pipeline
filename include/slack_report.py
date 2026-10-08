"""Informe de cambios en eventos a Slack (un mensaje por corrida).

Dos capas:
  1. Funciones PURAS (sin Airflow, sin red): calculo de deltas, alertas y formato del mensaje.
     Son las que cubren los tests de tests/unit/test_slack_report.py.
  2. Funciones con efectos (Snowflake y Slack): leen OPS.KPI_SNAPSHOT, arman el mensaje y lo envian.
     Los imports de Airflow son perezosos para que la capa pura se pueda importar y probar sin Airflow.

La foto de KPIs la genera dags/sql/07_kpi_snapshot.sql (tarea kpi_snapshot).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable

log = logging.getLogger(__name__)

CANCELLED_PCT_SIM_RISE_PP = 2.0
RSVP_AVG_DROP_PCT = 10.0
MIN_SIM_EVENTS_FOR_ALERT = 50
CANCELLED_IN_RUN_ALERT = 3
TOP_N = 3
MAX_MESSAGE_CHARS = 3500
MIN_CELL = 5

SNOWFLAKE_CONN = "snowflake_default"
SLACK_CONN = "slack_webhook"
SNAPSHOT_TABLE = "MEETUP_DB.OPS.KPI_SNAPSHOT"
MEMBER_METRICS_TABLE = "MEETUP_DB.ANALYTICS.PROC_MEMBER_METRICS"
INGEST_CONTROL = "MEETUP_DB.OPS.INGEST_CONTROL"
MEMBERS_EXPECTED_CHUNKS = 6
TASK_ID = "slack_insights_report"

Snapshot = dict[str, Any]


def to_float(value: Any) -> float | None:
    """Decimal/int/str/None -> float | None (NaN tambien da None)."""
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return None if out != out else out


def pct(part: Any, total: Any) -> float | None:
    """Porcentaje part/total*100. None si falta algun dato o total es 0."""
    p, t = to_float(part), to_float(total)
    if p is None or t is None or t == 0:
        return None
    return p / t * 100.0


@dataclass(frozen=True)
class Delta:
    cur: float | None
    prev: float | None
    abs: float | None
    pct: float | None
    arrow: str


def delta(cur: Any, prev: Any) -> Delta:
    c, p = to_float(cur), to_float(prev)
    if c is None or p is None:
        return Delta(c, p, None, None, "")
    d = c - p
    rel = None if p == 0 else d / abs(p) * 100.0
    arrow = "▲" if d > 0 else ("▼" if d < 0 else "▬")
    return Delta(c, p, d, rel, arrow)


def slack_escape(text: Any) -> str:
    """Escapa los caracteres con significado en mrkdwn de Slack."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def fmt_num(value: float | None, decimals: int = 0, signed: bool = False) -> str:
    """Formato es-CO: miles con '.', decimales con ','. None -> 'n/d'."""
    if value is None:
        return "n/d"
    s = f"{value:{'+' if signed else ''},.{decimals}f}"
    return s.replace(",", "\0").replace(".", ",").replace("\0", ".")


def fmt_delta(d: Delta, decimals: int = 0, unit: str = "") -> str:
    """'▲ +25 (+0,4 %)' | '▬ 0' | '' si no hay comparacion. unit='pp' omite el % relativo."""
    if d.abs is None:
        return ""
    base = f"{d.arrow} {fmt_num(d.abs, decimals, signed=True) if d.abs else fmt_num(0, decimals)}"
    if unit == "pp":
        return f"{base} pp"
    if d.pct is not None and d.abs != 0:
        base += f" ({fmt_num(d.pct, 1, signed=True)} %)"
    return base


def _group(snapshot: Snapshot | None, kind: str) -> dict[str, float | None]:
    return (snapshot or {}).get(kind, {})


def derived(snapshot: Snapshot | None) -> dict[str, float | None]:
    """KPIs calculados a partir de la foto (porcentajes de cancelados)."""
    t = _group(snapshot, "TOTAL")
    return {
        "N": t.get("N"), "N_SIM": t.get("N_SIM"), "RSVP": t.get("RSVP"), "RSVP_AVG": t.get("RSVP_AVG"),
        "CANC_PCT_ALL": pct(t.get("N_CANC"), t.get("N")),
        "CANC_PCT_SIM": pct(t.get("N_CANC_SIM"), t.get("N_SIM")),
    }


def top_n(cur: dict[str, float | None], prev: dict[str, float | None] | None, n: int = TOP_N) -> list[tuple[str, float, Delta]]:
    """Top n por valor desc (empate por nombre). El delta usa el mismo nombre en la foto previa."""
    rows = sorted(((k, v) for k, v in cur.items() if v is not None), key=lambda kv: (-kv[1], kv[0]))[:n]
    return [(k, v, delta(v, None if prev is None else prev.get(k, 0.0))) for k, v in rows]


def check_breakdown_sums(snapshot: Snapshot | None) -> list[str]:
    """Verifica que STATUS, CATEGORY y CITY sumen TOTAL/N. Devuelve la lista de problemas."""
    if not snapshot:
        return ["foto vacia"]
    total = to_float(_group(snapshot, "TOTAL").get("N"))
    if total is None or total == 0:
        return ["TOTAL/N ausente o 0"]
    problems = []
    for kind in ("STATUS", "CATEGORY", "CITY"):
        s = sum(v for v in _group(snapshot, kind).values() if v is not None)
        if abs(s - total) > 1e-6:
            problems.append(f"{kind} suma {fmt_num(s)} y TOTAL/N es {fmt_num(total)}")
    return problems


def suppress_small_cells(values: dict[str, Any], min_cell: int = MIN_CELL,
                         label: str = "otros (<5 miembros)") -> dict[str, float]:
    """Supresion de celdas pequenas: las claves con valor < min_cell se agrupan en 'label'.
    Nunca muestra una celda con menos de min_cell miembros."""
    out: dict[str, float] = {}
    small = 0.0
    for k, v in values.items():
        f = to_float(v)
        if f is None:
            continue
        if f < min_cell:
            small += f
        else:
            out[k] = f
    if small > 0:
        out[label] = out.get(label, 0.0) + small
    return out


def build_members_text(stats: dict[str, Any] | None) -> str:
    """Seccion de miembros del informe (capa pura). stats viene de fetch_members_summary.
    Si la carga sigue en curso muestra el progreso (X/6) en vez de las metricas."""
    lines = ["*Miembros*"]
    if not stats:
        lines.append("• sin datos todavía")
        return "\n".join(lines)
    if stats.get("pending"):
        lines.append(f"• carga de members en curso: *{slack_escape(stats['pending'])}*")
        return "\n".join(lines)
    members = to_float(stats.get("TOTAL_MEMBERS"))
    memberships = to_float(stats.get("TOTAL_MEMBERSHIPS"))
    avg = to_float(stats.get("AVG_MEMBERSHIPS_PER_MEMBER"))
    lines.append(f"• Miembros únicos: *{fmt_num(members)}*  ·  Membresías: *{fmt_num(memberships)}*")
    lines.append(f"• Media de grupos por miembro: *{fmt_num(avg, 2) if avg is not None else 'n/d'}*")
    ov_key, ov_val = stats.get("TOP_OVERLAP_KEY"), to_float(stats.get("TOP_OVERLAP_VALUE"))
    if ov_key:
        lines.append(f"• Mayor solapamiento: *{slack_escape(ov_key)}* "
                     f"({fmt_num(ov_val)} miembros compartidos)")
    return "\n".join(lines)


def evaluate_alerts(cur: Snapshot, prev: Snapshot | None) -> list[str]:
    """Alertas del informe. Las que necesitan comparacion se omiten en la primera corrida."""
    alerts: list[str] = []
    dc, dp = derived(cur), derived(prev)
    mv = _group(cur, "MOVEMENT")

    if prev is not None:
        n_sim = to_float(dc["N_SIM"]) or 0
        rise = None if dc["CANC_PCT_SIM"] is None or dp["CANC_PCT_SIM"] is None else dc["CANC_PCT_SIM"] - dp["CANC_PCT_SIM"]
        if n_sim >= MIN_SIM_EVENTS_FOR_ALERT and rise is not None and round(rise, 6) >= CANCELLED_PCT_SIM_RISE_PP:
            alerts.append(f":warning: El % de cancelados (simulados) subió {fmt_num(rise, 2)} pp "
                          f"(umbral {fmt_num(CANCELLED_PCT_SIM_RISE_PP, 1)} pp): "
                          f"{fmt_num(dp['CANC_PCT_SIM'], 2)} % -> {fmt_num(dc['CANC_PCT_SIM'], 2)} %.")
        d_avg = delta(dc["RSVP_AVG"], dp["RSVP_AVG"])
        if d_avg.pct is not None and round(d_avg.pct, 6) <= -RSVP_AVG_DROP_PCT:
            alerts.append(f":warning: El RSVP promedio cayó {fmt_num(abs(d_avg.pct), 1)} % "
                          f"(umbral {fmt_num(RSVP_AVG_DROP_PCT, 0)} %): {fmt_num(d_avg.prev, 2)} -> {fmt_num(d_avg.cur, 2)}.")
        new_events = to_float(mv.get("NEW_EVENTS"))
        if new_events is not None and new_events == 0:
            alerts.append(":warning: No entraron eventos nuevos en esta corrida.")
        canc = to_float(mv.get("CANCELLED_IN_RUN"))
        if canc is not None and canc >= CANCELLED_IN_RUN_ALERT:
            alerts.append(f":warning: {fmt_num(canc)} cancelaciones en una sola corrida "
                          f"(umbral {CANCELLED_IN_RUN_ALERT}).")

    for problem in check_breakdown_sums(cur):
        alerts.append(f":warning: El desglose no cuadra: {problem}.")
    return alerts


def _kpi_line(label: str, d: Delta, decimals: int = 0, unit: str = "") -> str:
    delta_txt = fmt_delta(d, decimals, unit)
    value = fmt_num(d.cur, decimals) + (" %" if unit == "pp" else "")
    return f"• {label}: *{value}*" + (f"  {delta_txt}" if delta_txt else "")


def _ranking_section(title: str, cur: dict, prev: dict | None) -> str:
    rows = top_n(cur, prev)
    if not rows:
        return f"*{title}*\n• sin datos"
    lines = [f"*{title}*"]
    for i, (name, value, d) in enumerate(rows, start=1):
        dt = fmt_delta(d)
        lines.append(f"{i}. {slack_escape(name)} — *{fmt_num(value)}*" + (f"  {dt}" if dt else ""))
    return "\n".join(lines)


def build_message(dag_id: str, run_id: str, cur: Snapshot, prev: Snapshot | None,
                  prev_run_id: str | None = None,
                  members_text: str | None = None,
                  max_chars: int = MAX_MESSAGE_CHARS) -> str:
    """Arma el mensaje mrkdwn. Si excede max_chars se descartan secciones de menor prioridad."""
    first = prev is None
    dc, dp = derived(cur), derived(prev)
    mv = _group(cur, "MOVEMENT")

    header = [f":bar_chart: *Informe de cambios en eventos* | `{slack_escape(dag_id)}`",
              f"Corrida `{slack_escape(run_id)}`"]
    if first:
        header.append(":information_source: Primera corrida con foto: *no hay comparación todavía*.")
    else:
        prev_ts = (prev or {}).get("run_ts")
        extra = f" ({slack_escape(prev_ts)})" if prev_ts else ""
        header.append(f"Comparado con la corrida anterior `{slack_escape(prev_run_id or '?')}`{extra}")

    def mv_line(label: str, key: str) -> str:
        v = to_float(mv.get(key))
        return f"• {label}: *{fmt_num(v) if v is not None else 'n/d'}*"

    movement = ["*Movimiento de esta corrida*",
                mv_line("Eventos nuevos", "NEW_EVENTS"),
                mv_line("Eventos existentes actualizados", "UPDATED_EVENTS"),
                mv_line("Cancelaciones en esta corrida", "CANCELLED_IN_RUN")]
    if first:
        movement.append("_(sin foto previa: el movimiento no se puede medir)_")

    kpis = ["*KPIs generales*",
            _kpi_line("Eventos totales", delta(dc["N"], dp["N"])),
            _kpi_line("Eventos simulados", delta(dc["N_SIM"], dp["N_SIM"])),
            _kpi_line("RSVP totales", delta(dc["RSVP"], dp["RSVP"])),
            _kpi_line("RSVP promedio por evento", delta(dc["RSVP_AVG"], dp["RSVP_AVG"]), decimals=2),
            _kpi_line("% cancelados (todos)", delta(dc["CANC_PCT_ALL"], dp["CANC_PCT_ALL"]), decimals=2, unit="pp"),
            _kpi_line("% cancelados (simulados)", delta(dc["CANC_PCT_SIM"], dp["CANC_PCT_SIM"]), decimals=2, unit="pp")]

    cur_status, prev_status = _group(cur, "STATUS"), None if first else _group(prev, "STATUS")
    status = ["*Distribución por estado*"]
    for name, value, d in top_n(cur_status, prev_status, n=len(cur_status) or 1):
        dt = fmt_delta(d)
        status.append(f"• {slack_escape(name)}: *{fmt_num(value)}*" + (f"  {dt}" if dt else ""))
    if len(status) == 1:
        status.append("• sin datos")

    tops = [_ranking_section(f"Top {TOP_N} categorías", _group(cur, "CATEGORY"), None if first else _group(prev, "CATEGORY")),
            _ranking_section(f"Top {TOP_N} ciudades", _group(cur, "CITY"), None if first else _group(prev, "CITY"))]

    alerts_list = evaluate_alerts(cur, prev)
    alerts = ["*Alertas*"] + (alerts_list if alerts_list else [":white_check_mark: Sin alertas."])

    sections: list[tuple[str, str, bool]] = [
        ("header", "\n".join(header), False), ("movement", "\n".join(movement), False),
        ("kpis", "\n".join(kpis), False), ("status", "\n".join(status), True),
        ("tops", "\n\n".join(tops), True), ("alerts", "\n".join(alerts), False)]
    if members_text:
        sections.append(("members", members_text.strip(), True))

    def render() -> str:
        return "\n\n".join(text for _, text, _ in sections)

    for drop in ("tops", "status", "members"):
        if len(render()) <= max_chars:
            break
        sections = [s for s in sections if s[0] != drop]
    msg = render()
    if len(msg) > max_chars:
        msg = msg[: max_chars - 40].rstrip() + "\n_…mensaje truncado_"
    return msg


def rows_to_snapshot(rows: Iterable[tuple]) -> Snapshot | None:
    """Filas (RUN_TS, KPI_TYPE, KPI_KEY, KPI_VALUE) -> Snapshot. None si no hay filas."""
    snap: Snapshot = {}
    n = 0
    for run_ts, kind, key, value in rows:
        n += 1
        snap.setdefault("run_ts", run_ts)
        snap.setdefault(kind, {})[key] = to_float(value)
    return snap if n else None


def fetch_snapshot(hook, run_id: str) -> Snapshot | None:
    rows = hook.get_records(
        f"SELECT RUN_TS, KPI_TYPE, KPI_KEY, KPI_VALUE FROM {SNAPSHOT_TABLE} WHERE DAG_RUN_ID = %s",
        parameters=(run_id,))
    return rows_to_snapshot(rows)


def fetch_previous(hook, run_id: str, run_ts) -> tuple[str | None, Snapshot | None]:
    """Foto inmediatamente anterior (otro DAG_RUN_ID con RUN_TS menor), igual que en 07_kpi_snapshot.sql."""
    rows = hook.get_records(
        f"SELECT DAG_RUN_ID FROM (SELECT DAG_RUN_ID, MAX(RUN_TS) AS TS FROM {SNAPSHOT_TABLE} "
        f"WHERE DAG_RUN_ID <> %s AND RUN_TS < %s GROUP BY DAG_RUN_ID) ORDER BY TS DESC LIMIT 1",
        parameters=(run_id, run_ts))
    if not rows:
        return None, None
    prev_id = rows[0][0]
    return prev_id, fetch_snapshot(hook, prev_id)


def fetch_members_summary(hook) -> dict[str, Any] | None:
    """Estado de la carga y metricas agregadas de miembros para el informe.
    Nunca lanza: cualquier fallo devuelve None (la seccion se muestra como 'sin datos')."""
    try:
        loaded = hook.get_first(
            f"SELECT COUNT(*) FROM {INGEST_CONTROL} WHERE STATUS = 'LOADED'")[0]
        if int(loaded) < MEMBERS_EXPECTED_CHUNKS:
            return {"pending": f"{loaded}/{MEMBERS_EXPECTED_CHUNKS}"}
        rows = hook.get_records(
            f"SELECT METRIC_TYPE, METRIC_KEY, METRIC_VALUE FROM {MEMBER_METRICS_TABLE} "
            f"WHERE METRIC_TYPE IN ('TOTALS', 'OVERLAP_TOP_CATEGORIES')")
        if not rows:
            return {"pending": f"{int(loaded)}/{MEMBERS_EXPECTED_CHUNKS} (métricas pendientes)"}
        stats: dict[str, Any] = {}
        for ktype, key, value in rows:
            stats[key] = value
        ov = {k: v for k, v in stats.items() if " + " in str(k)}
        if ov:
            best = max(ov, key=lambda k: to_float(ov[k]) or 0)
            stats["TOP_OVERLAP_KEY"] = best
            stats["TOP_OVERLAP_VALUE"] = ov[best]
        return stats
    except Exception:
        log.exception("[%s] fetch_members_summary fallo: se omite la seccion de miembros", TASK_ID)
        return None


def send_slack(text: str) -> None:
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
    SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN).send(text=text)


def send_insights_report(dag_id: str, run_id: str) -> None:
    """Entrada de la tarea slack_insights_report. Los errores llevan el prefijo [slack_insights_report]
    para que el callback de Slack deje claro que fallo el informe y no la carga de datos."""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    try:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
        cur = fetch_snapshot(hook, run_id)
        if cur is None:
            raise RuntimeError(f"no hay foto en {SNAPSHOT_TABLE} para la corrida {run_id} "
                               f"(¿corrio la tarea kpi_snapshot?)")
        prev_id, prev = fetch_previous(hook, run_id, cur.get("run_ts"))
        text = build_message(dag_id, run_id, cur, prev, prev_run_id=prev_id,
                             members_text=build_members_text(fetch_members_summary(hook)))
        send_slack(text)
        log.info("[%s] informe enviado (%d caracteres, %s)", TASK_ID, len(text),
                 "primera corrida" if prev is None else f"vs {prev_id}")
    except Exception as exc:
        raise RuntimeError(f"[{TASK_ID}] fallo el informe de insights (los datos del pipeline no se ven afectados): {exc}") from exc
