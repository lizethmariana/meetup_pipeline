"""Pruebas de include/slack_report.py. No usan Snowflake ni Slack."""
import sys
import types
from decimal import Decimal

import pytest

from include import slack_report as sr


def snap(n=1000, n_sim=200, rsvp=3000, rsvp_avg=3.0, canc=10, canc_sim=10,
         new=25, upd=5, canc_run=0, status=None, cat=None, city=None, run_ts="2026-10-07 12:00:00"):
    """Foto de ejemplo con desgloses que cuadran con N por defecto."""
    return {
        "run_ts": run_ts,
        "TOTAL": {"N": n, "N_SIM": n_sim, "N_REAL": n - n_sim, "RSVP": rsvp, "RSVP_AVG": rsvp_avg,
                  "N_CANC": canc, "N_CANC_SIM": canc_sim, "N_CANC_REAL": canc - canc_sim},
        "MOVEMENT": {"NEW_EVENTS": new, "UPDATED_EVENTS": upd, "CANCELLED_IN_RUN": canc_run},
        "STATUS": status or {"upcoming": n - canc, "cancelled": canc},
        "CATEGORY": cat or {"tech": n - 300, "music": 200, "(sin categoria)": 100},
        "CITY": city or {"New York": n - 400, "Chicago": 250, "(sin ciudad)": 150},
    }


def test_to_float_acepta_decimal_none_y_basura():
    assert sr.to_float(Decimal("12.3400")) == 12.34
    assert sr.to_float(None) is None
    assert sr.to_float("abc") is None
    assert sr.to_float(float("nan")) is None


def test_pct_division_por_cero_y_nulos():
    assert sr.pct(5, 0) is None
    assert sr.pct(None, 10) is None
    assert sr.pct(5, None) is None
    assert sr.pct(1, 4) == 25.0


def test_delta_sube_baja_y_cero():
    up, down, same = sr.delta(110, 100), sr.delta(90, 100), sr.delta(100, 100)
    assert (up.arrow, up.abs, up.pct) == ("▲", 10, 10.0)
    assert (down.arrow, down.abs, down.pct) == ("▼", -10, -10.0)
    assert (same.arrow, same.abs, same.pct) == ("▬", 0, 0.0)


def test_delta_valor_previo_cero_no_da_porcentaje_ni_error():
    d = sr.delta(5, 0)
    assert d.abs == 5 and d.pct is None and d.arrow == "▲"
    assert "(" not in sr.fmt_delta(d)


def test_delta_sin_comparacion():
    d = sr.delta(5, None)
    assert d.abs is None and d.arrow == ""
    assert sr.fmt_delta(d) == ""


def test_delta_acepta_decimal():
    d = sr.delta(Decimal("3.5"), Decimal("3.0"))
    assert d.abs == pytest.approx(0.5)


def test_formato_es_co():
    assert sr.fmt_num(5807) == "5.807"
    assert sr.fmt_num(1234.5, 2) == "1.234,50"
    assert sr.fmt_num(None) == "n/d"
    assert sr.fmt_num(25, signed=True) == "+25"
    assert sr.fmt_delta(sr.delta(125, 100)) == "▲ +25 (+25,0 %)"
    assert sr.fmt_delta(sr.delta(100, 100)) == "▬ 0"
    assert sr.fmt_delta(sr.delta(2.5, 2.0), decimals=2, unit="pp") == "▲ +0,50 pp"


def test_top_n_orden_empates_y_delta():
    cur = {"b": 10, "a": 10, "c": 30, "d": 1}
    rows = sr.top_n(cur, {"c": 25, "a": 10})
    assert [r[0] for r in rows] == ["c", "a", "b"]
    assert rows[0][2].abs == 5
    assert rows[2][2].abs == 10
    assert all(r[2].arrow == "" for r in sr.top_n(cur, None))


def test_top_n_ignora_nulos():
    assert sr.top_n({"x": None, "y": 2}, None) == [("y", 2, sr.delta(2, None))]


def test_desgloses_cuadran_con_total():
    assert sr.check_breakdown_sums(snap()) == []


@pytest.mark.parametrize("kind", ["STATUS", "CATEGORY", "CITY"])
def test_detecta_desglose_que_no_cuadra(kind):
    s = snap()
    s[kind]["x"] = 7
    problems = sr.check_breakdown_sums(s)
    assert len(problems) == 1 and kind in problems[0]


def test_foto_vacia_o_total_cero_se_reporta():
    assert sr.check_breakdown_sums(None) == ["foto vacia"]
    assert sr.check_breakdown_sums(snap(n=0, canc=0, status={}, cat={}, city={})) == ["TOTAL/N ausente o 0"]


def test_primera_corrida_no_alerta_por_comparacion():
    assert sr.evaluate_alerts(snap(new=0, canc_run=9), None) == []


def test_sin_alertas_cuando_todo_estable():
    assert sr.evaluate_alerts(snap(), snap()) == []


def test_alerta_cancelados_simulados_sube_2pp():
    prev = snap(n_sim=200, canc_sim=10)
    cur = snap(n_sim=200, canc_sim=15, canc=15, status={"upcoming": 985, "cancelled": 15})
    alerts = sr.evaluate_alerts(cur, prev)
    assert len(alerts) == 1 and "cancelados (simulados) subió" in alerts[0]


def test_cancelados_sube_menos_del_umbral_no_alerta():
    prev = snap(n_sim=200, canc_sim=10)
    cur = snap(n_sim=200, canc_sim=13, canc=13, status={"upcoming": 987, "cancelled": 13})
    assert sr.evaluate_alerts(cur, prev) == []


def test_muestra_minima_evita_falso_positivo_de_cancelados():
    prev = snap(n_sim=40, canc_sim=0, canc=0, status={"upcoming": 1000})
    cur = snap(n_sim=49, canc_sim=9, canc=9, status={"upcoming": 991, "cancelled": 9})
    assert not any("cancelados (simulados)" in a for a in sr.evaluate_alerts(cur, prev))
    cur2 = snap(n_sim=50, canc_sim=9, canc=9, status={"upcoming": 991, "cancelled": 9})
    assert any("cancelados (simulados)" in a for a in sr.evaluate_alerts(cur2, prev))


def test_alerta_rsvp_promedio_cae_10_porciento():
    alerts = sr.evaluate_alerts(snap(rsvp_avg=2.7), snap(rsvp_avg=3.0))
    assert any("RSVP promedio cayó" in a for a in alerts)
    assert not any("RSVP promedio" in a for a in sr.evaluate_alerts(snap(rsvp_avg=2.8), snap(rsvp_avg=3.0)))


def test_rsvp_promedio_previo_cero_no_revienta():
    assert sr.evaluate_alerts(snap(rsvp_avg=3.0), snap(rsvp_avg=0.0)) == []
    assert sr.evaluate_alerts(snap(rsvp_avg=None), snap(rsvp_avg=3.0)) == []


def test_alerta_sin_eventos_nuevos():
    alerts = sr.evaluate_alerts(snap(new=0), snap())
    assert any("No entraron eventos nuevos" in a for a in alerts)


def test_alerta_cancelaciones_en_una_corrida():
    assert any("cancelaciones en una sola corrida" in a for a in sr.evaluate_alerts(snap(canc_run=3), snap()))
    assert not any("cancelaciones" in a for a in sr.evaluate_alerts(snap(canc_run=2), snap()))


def test_alerta_si_el_desglose_no_cuadra():
    s = snap()
    s["CITY"]["x"] = 5
    assert any("desglose no cuadra" in a for a in sr.evaluate_alerts(s, snap()))


def test_umbrales_son_constantes_configurables(monkeypatch):
    monkeypatch.setattr(sr, "CANCELLED_IN_RUN_ALERT", 1)
    assert any("cancelaciones" in a for a in sr.evaluate_alerts(snap(canc_run=1), snap()))


def test_mensaje_primera_corrida_lo_dice_y_no_trae_deltas():
    msg = sr.build_message("meetup_pipeline", "run1", snap(new=None, upd=None, canc_run=None), None)
    assert "no hay comparación todavía" in msg
    assert "n/d" in msg
    assert "▲" not in msg and "▼" not in msg
    assert "Sin alertas" in msg


def test_mensaje_con_comparacion_trae_secciones_flechas_y_tops():
    msg = sr.build_message("meetup_pipeline", "run2", snap(n=1025, new=25), snap(n=1000), prev_run_id="run1")
    for fragment in ("Informe de cambios en eventos", "`run2`", "`run1`", "Movimiento de esta corrida",
                     "KPIs generales", "Distribución por estado", "Top 3 categorías", "Top 3 ciudades", "Alertas"):
        assert fragment in msg
    assert "▲ +25" in msg


def test_mensaje_escapa_mrkdwn_en_nombres():
    s = snap(cat={"R&D <beta>": 900, "tech": 100}, n=1000)
    msg = sr.build_message("d", "r", s, snap())
    assert "R&amp;D &lt;beta&gt;" in msg and "<beta>" not in msg


def test_mensaje_largo_se_trunca_sin_perder_lo_esencial():
    many = {f"ciudad {i:03d}": 10 for i in range(300)}
    s = snap(n=3000, city=many, cat={f"cat {i:03d}": 10 for i in range(300)},
             status={f"estado {i:03d}": 10 for i in range(300)})
    msg = sr.build_message("d", "r", s, s)
    assert len(msg) <= sr.MAX_MESSAGE_CHARS
    assert "Movimiento de esta corrida" in msg and "KPIs generales" in msg and "Alertas" in msg


def test_mensaje_truncado_duro_si_nada_alcanza():
    msg = sr.build_message("d", "r", snap(), snap(), max_chars=300)
    assert len(msg) <= 300 and msg.endswith("mensaje truncado_")


def test_mensaje_con_decimal_de_snowflake():
    s = snap()
    s["TOTAL"]["RSVP_AVG"] = Decimal("3.1234")
    sr.build_message("d", "r", sr.rows_to_snapshot([("t", "TOTAL", "N", Decimal("10")),
                                                    ("t", "TOTAL", "RSVP_AVG", Decimal("3.1234"))]), s)


def test_rows_to_snapshot():
    assert sr.rows_to_snapshot([]) is None
    s = sr.rows_to_snapshot([("t0", "TOTAL", "N", Decimal("5")), ("t0", "STATUS", "upcoming", Decimal("5")),
                             ("t0", "MOVEMENT", "NEW_EVENTS", None)])
    assert s["run_ts"] == "t0" and s["TOTAL"]["N"] == 5.0 and s["MOVEMENT"]["NEW_EVENTS"] is None


class FakeHook:
    def __init__(self, tables):
        self.tables = tables

    def get_records(self, sql, parameters=None):
        if sql.startswith("SELECT DAG_RUN_ID FROM"):
            return [(self.tables["prev_id"],)] if self.tables.get("prev_id") else []
        return self.tables["rows"].get(parameters[0], [])


def _install_fakes(monkeypatch, hook, sent):
    mod = types.ModuleType("airflow.providers.snowflake.hooks.snowflake")
    mod.SnowflakeHook = lambda snowflake_conn_id: hook
    for name in ("airflow", "airflow.providers", "airflow.providers.snowflake", "airflow.providers.snowflake.hooks"):
        monkeypatch.setitem(sys.modules, name, sys.modules.get(name, types.ModuleType(name)))
    monkeypatch.setitem(sys.modules, "airflow.providers.snowflake.hooks.snowflake", mod)
    monkeypatch.setattr(sr, "send_slack", sent.append)


def test_tarea_falla_con_mensaje_claro_si_no_hay_foto(monkeypatch):
    sent = []
    _install_fakes(monkeypatch, FakeHook({"rows": {}}), sent)
    with pytest.raises(RuntimeError) as err:
        sr.send_insights_report("meetup_pipeline", "run-x")
    assert "[slack_insights_report]" in str(err.value) and "run-x" in str(err.value) and "kpi_snapshot" in str(err.value)
    assert sent == []


def test_tarea_envia_un_solo_mensaje_primera_corrida(monkeypatch):
    sent = []
    rows = {"run1": [("t1", "TOTAL", "N", 10), ("t1", "STATUS", "upcoming", 10), ("t1", "CATEGORY", "tech", 10),
                     ("t1", "CITY", "NYC", 10), ("t1", "TOTAL", "N_SIM", 0)]}
    _install_fakes(monkeypatch, FakeHook({"rows": rows}), sent)
    sr.send_insights_report("meetup_pipeline", "run1")
    assert len(sent) == 1 and "no hay comparación todavía" in sent[0]


def test_tarea_usa_la_foto_anterior(monkeypatch):
    sent = []
    base = [("t", "STATUS", "upcoming", 10), ("t", "CATEGORY", "tech", 10), ("t", "CITY", "NYC", 10)]
    rows = {"run2": [("t2", "TOTAL", "N", 12)] + [(a, b, c, 12) for a, b, c, _ in base],
            "run1": [("t1", "TOTAL", "N", 10)] + base}
    _install_fakes(monkeypatch, FakeHook({"rows": rows, "prev_id": "run1"}), sent)
    sr.send_insights_report("meetup_pipeline", "run2")
    assert "`run1`" in sent[0] and "▲ +2" in sent[0]


def test_suppress_small_cells_agrupa_bajo_el_umbral():
    vals = {"a": 10, "b": 4, "c": 1, "d": 7}
    out = sr.suppress_small_cells(vals)
    assert out == {"a": 10.0, "d": 7.0, "otros (<5 miembros)": 5.0}
    assert all(v >= sr.MIN_CELL for k, v in out.items() if k != "otros (<5 miembros)")


def test_suppress_small_cells_casos_borde():
    assert sr.suppress_small_cells({}) == {}
    assert sr.suppress_small_cells({"x": 2}) == {"otros (<5 miembros)": 2.0}
    assert sr.suppress_small_cells({"x": 5}) == {"x": 5.0}
    assert sr.suppress_small_cells({"a": None, "b": 3}) == {"otros (<5 miembros)": 3.0}
    assert sr.suppress_small_cells({"otros (<5 miembros)": 9, "x": 1}) == {"otros (<5 miembros)": 10.0}


def test_build_members_text_carga_en_curso():
    txt = sr.build_members_text({"pending": "2/6"})
    assert "carga de members en curso" in txt and "2/6" in txt


def test_build_members_text_con_metricas():
    stats = {"TOTAL_MEMBERS": 800000, "TOTAL_MEMBERSHIPS": 5893886,
             "AVG_MEMBERSHIPS_PER_MEMBER": 7.25, "TOP_OVERLAP_KEY": "tech + career/business",
             "TOP_OVERLAP_VALUE": 12345}
    txt = sr.build_members_text(stats)
    assert "Miembros únicos" in txt and "800.000" in txt
    assert "Membresías" in txt and "5.893.886" in txt
    assert "7,25" in txt
    assert "tech + career/business" in txt and "12.345" in txt


def test_build_members_text_vacio_o_sin_solapamiento():
    assert "sin datos" in sr.build_members_text(None)
    txt = sr.build_members_text({"TOTAL_MEMBERS": 10, "TOTAL_MEMBERSHIPS": 25,
                                 "AVG_MEMBERSHIPS_PER_MEMBER": None})
    assert "n/d" in txt and "solapamiento" not in txt


def test_mensaje_incluye_seccion_miembros():
    msg = sr.build_message("d", "r", snap(), snap(), members_text="*Miembros*\n• x")
    assert "*Miembros*" in msg
    assert "*Miembros*" not in sr.build_message("d", "r", snap(), snap(), members_text=None)


def test_mensaje_largo_descarta_miembros_al_final():
    many = {f"ciudad {i:03d}": 10 for i in range(300)}
    s = snap(n=3000, city=many, cat={f"cat {i:03d}": 10 for i in range(300)},
             status={f"estado {i:03d}": 10 for i in range(300)})
    msg = sr.build_message("d", "r", s, s, members_text="*Miembros*\n" + "y" * 4000)
    assert len(msg) <= sr.MAX_MESSAGE_CHARS and "Miembros" not in msg
    assert "Movimiento de esta corrida" in msg and "KPIs generales" in msg and "Alertas" in msg


class MembersFakeHook:
    """Fake de SnowflakeHook para fetch_members_summary."""
    def __init__(self, loaded=6, rows=None, fail=False):
        self.loaded, self.rows, self.fail = loaded, rows or [], fail

    def get_first(self, sql, parameters=None):
        if self.fail:
            raise RuntimeError("boom")
        return (self.loaded,)

    def get_records(self, sql, parameters=None):
        return self.rows


def test_fetch_members_summary_carga_en_curso():
    out = sr.fetch_members_summary(MembersFakeHook(loaded=2))
    assert out == {"pending": "2/6"}


def test_fetch_members_summary_con_metricas_y_solapamiento():
    rows = [("TOTALS", "TOTAL_MEMBERS", 100), ("TOTALS", "TOTAL_MEMBERSHIPS", 300),
            ("TOTALS", "AVG_MEMBERSHIPS_PER_MEMBER", 3.0),
            ("OVERLAP_TOP_CATEGORIES", "a + b", 12), ("OVERLAP_TOP_CATEGORIES", "a + c", 30)]
    out = sr.fetch_members_summary(MembersFakeHook(loaded=6, rows=rows))
    assert out["TOTAL_MEMBERS"] == 100
    assert out["TOP_OVERLAP_KEY"] == "a + c" and out["TOP_OVERLAP_VALUE"] == 30


def test_fetch_members_summary_fallo_da_none():
    assert sr.fetch_members_summary(MembersFakeHook(fail=True)) is None


def test_exito_por_tarea_controlado_por_variable(monkeypatch):
    import sys
    from pathlib import Path
    dags_dir = str(Path(__file__).resolve().parents[2] / "dags")
    if dags_dir not in sys.path:
        sys.path.insert(0, dags_dir)
    import meetup_pipeline as mp

    sent = []
    monkeypatch.setattr(mp, "notify_task_ok", lambda ctx: sent.append(ctx))

    import airflow.models as am
    monkeypatch.setattr(am.Variable, "get",
                        staticmethod(lambda key, default_var=None: "true"), raising=True)
    mp.notify_task_ok_if_enabled({})
    assert len(sent) == 1

    monkeypatch.setattr(am.Variable, "get",
                        staticmethod(lambda key, default_var=None: "false"), raising=True)
    mp.notify_task_ok_if_enabled({})
    assert len(sent) == 1
