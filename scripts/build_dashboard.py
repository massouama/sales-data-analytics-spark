#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_dashboard.py — Génère un tableau de bord HTML autonome
============================================================

Lit les résultats CSV produits par `src/analytics.py` (répertoire output/) et
construit un fichier HTML unique présentant les KPI et les analyses sous forme
de cartes, de graphiques (Chart.js) et de tableaux.

Chart.js est servi localement (assets/chart.umd.js copié dans output/), donc le
tableau de bord fonctionne **hors-ligne**.

Usage :
    python3 scripts/build_dashboard.py     # -> output/dashboard.html
    open output/dashboard.html
"""
from __future__ import annotations

import csv
import html
import json
import os
import shutil
from datetime import datetime

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output")
ASSETS_DIR = os.path.join(PROJECT_DIR, "assets")
DASHBOARD = os.path.join(OUTPUT_DIR, "dashboard.html")


# --------------------------------------------------------------------------- #
#  Lecture CSV + formatage                                                     #
# --------------------------------------------------------------------------- #
def read_csv(name):
    path = os.path.join(OUTPUT_DIR, name)
    if not os.path.exists(path):
        return [], []
    with open(path, newline="", encoding="utf-8") as fh:
        rows = list(csv.reader(fh))
    return (rows[0], rows[1:]) if rows else ([], [])


def to_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def grp_int(v):
    """Entier avec séparateur de milliers (espace fine)."""
    try:
        return f"{int(round(float(v))):,}".replace(",", " ")
    except (TypeError, ValueError):
        return html.escape(str(v))


def titlecase(s):
    return " ".join(w.capitalize() for w in str(s).split())


def fmt_kpi(label, value):
    if "%" in label:
        return str(value).replace(".", ",") + " %"
    if "£" in label:
        return "£ " + grp_int(value)
    return grp_int(value)


def fmt_cell(col, value):
    c = col.lower()
    if c == "customerid":
        return html.escape(str(value).split(".")[0])
    if c in ("revenue", "monetary"):
        return "£ " + grp_int(value)
    if c in ("orders", "quantity", "customers", "quantitysold",
             "frequency", "recencydays"):
        return grp_int(value)
    return html.escape(str(value))


# --------------------------------------------------------------------------- #
#  Composants HTML                                                             #
# --------------------------------------------------------------------------- #
KPI_ICONS = {
    "Chiffre d'affaires total (£)": ("revenue", "#4f6df5"),
    "Nombre de commandes (factures)": ("orders", "#1aa989"),
    "Nombre de clients distincts": ("customers", "#7c5cfc"),
    "Nombre de produits distincts": ("products", "#f59e0b"),
    "Panier moyen / AOV (£)": ("basket", "#0ea5e9"),
    "Quantite totale vendue": ("qty", "#ef6c4d"),
    "Taux d'annulation (%)": ("cancel", "#f43f6b"),
    "Nombre de pays": ("country", "#10b981"),
}


def build_kpis(kpi_rows):
    # Cartes mises en avant (les plus parlantes), puis le reste en ligne fine.
    primary_order = [
        "Chiffre d'affaires total (£)",
        "Nombre de commandes (factures)",
        "Nombre de clients distincts",
        "Panier moyen / AOV (£)",
        "Nombre de produits distincts",
        "Taux d'annulation (%)",
    ]
    data = {r[0]: r[1] for r in kpi_rows if len(r) >= 2}
    cards = []
    for label in primary_order:
        if label not in data:
            continue
        color = KPI_ICONS.get(label, ("", "#4f6df5"))[1]
        cards.append(
            f'<div class="kpi" style="--accent:{color}">'
            f'<div class="kpi-val">{fmt_kpi(label, data[label])}</div>'
            f'<div class="kpi-lab">{html.escape(label)}</div></div>'
        )
    return "".join(cards)


def build_table(header, rows, max_rows=10):
    if not rows:
        return '<p class="empty">Aucune donnée.</p>'
    th = "".join(f"<th>{html.escape(h)}</th>" for h in header)
    body = []
    for r in rows[:max_rows]:
        tds = "".join(f"<td>{fmt_cell(header[i], c)}</td>" for i, c in enumerate(r))
        body.append(f"<tr>{tds}</tr>")
    return (
        f'<table><thead><tr>{th}</tr></thead>'
        f'<tbody>{"".join(body)}</tbody></table>'
    )


def chart_card(cid, title, subtitle="", tall=False):
    sub = f'<p class="c-sub">{html.escape(subtitle)}</p>' if subtitle else ""
    cls = "card chart-card tall" if tall else "card chart-card"
    return (
        f'<section class="{cls}"><h2>{html.escape(title)}</h2>{sub}'
        f'<div class="canvas-wrap"><canvas id="{cid}"></canvas></div></section>'
    )


def table_card(title, table_html, subtitle=""):
    sub = f'<p class="c-sub">{html.escape(subtitle)}</p>' if subtitle else ""
    return f'<section class="card"><h2>{html.escape(title)}</h2>{sub}{table_html}</section>'


# --------------------------------------------------------------------------- #
#  Génération                                                                  #
# --------------------------------------------------------------------------- #
def build():
    _, kpi_rows = read_csv("kpis.csv")
    h_month, r_month = read_csv("ventes_-_ca_par_mois.csv")
    h_dow, r_dow = read_csv("ventes_-_ca_par_jour_de_semaine.csv")
    h_hour, r_hour = read_csv("ventes_-_ca_par_heure.csv")
    h_pr, r_pr = read_csv("produits_-_top_par_chiffre_daffaires.csv")
    h_pq, r_pq = read_csv("produits_-_top_par_quantite_vendue.csv")
    h_cust, r_cust = read_csv("clients_-_top_par_chiffre_daffaires.csv")
    h_rfm, r_rfm = read_csv("clients_-_analyse_rfm_(meilleurs_clients).csv")
    h_pays, r_pays = read_csv("pays_-_ca_par_pays.csv")

    pays_ex_uk = [r for r in r_pays if r and r[0] != "United Kingdom"]

    payload = {
        "month": {
            "labels": [r[0] for r in r_month],
            "values": [to_float(r[1]) for r in r_month],
        },
        "dow": {
            "labels": [r[0][:3] for r in r_dow],
            "values": [to_float(r[1]) for r in r_dow],
        },
        "hour": {
            "labels": [f"{r[0]}h" for r in r_hour],
            "values": [to_float(r[1]) for r in r_hour],
        },
        "prodRev": {
            "labels": [titlecase(r[1])[:34] for r in r_pr],
            "values": [to_float(r[2]) for r in r_pr],
        },
        "prodQty": {
            "labels": [titlecase(r[1])[:34] for r in r_pq],
            "values": [to_float(r[2]) for r in r_pq],
        },
        "country": {
            "labels": [r[0] for r in pays_ex_uk],
            "values": [to_float(r[1]) for r in pays_ex_uk],
        },
    }

    kpis_html = build_kpis(kpi_rows)
    charts_html = "".join([
        chart_card("chMonth", "Chiffre d'affaires par mois",
                   "Évolution mensuelle — forte saisonnalité de fin d'année"),
        chart_card("chDow", "CA par jour de semaine",
                   "Aucune transaction le samedi dans ce jeu de données"),
        chart_card("chHour", "CA par heure de la journée",
                   "Activité concentrée entre 9 h et 15 h"),
        chart_card("chProdRev", "Top 10 produits par chiffre d'affaires", tall=True),
        chart_card("chProdQty", "Top 10 produits par quantité vendue", tall=True),
        chart_card("chCountry", "Top marchés hors Royaume-Uni",
                   "Le Royaume-Uni représente à lui seul ~85 % du CA", tall=True),
    ])
    tables_html = "".join([
        table_card("Top clients par chiffre d'affaires", build_table(h_cust, r_cust)),
        table_card("Meilleurs clients — analyse RFM", build_table(h_rfm, r_rfm),
                   "Récence (jours) · Fréquence (commandes) · Montant (£)"),
        table_card("Détail du chiffre d'affaires par pays",
                   build_table(h_pays, r_pays)),
    ])

    chartjs_src = "chart.umd.js" if _vendor_chartjs() else (
        "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"
    )
    generated = datetime.now().strftime("%d/%m/%Y à %H:%M")

    return (
        TEMPLATE.replace("__KPIS__", kpis_html)
        .replace("__CHARTS__", charts_html)
        .replace("__TABLES__", tables_html)
        .replace("__DATA__", json.dumps(payload, ensure_ascii=False))
        .replace("__CHARTJS__", chartjs_src)
        .replace("__GENERATED__", generated)
    )


def _vendor_chartjs():
    """Copie assets/chart.umd.js dans output/ pour un fonctionnement hors-ligne."""
    src = os.path.join(ASSETS_DIR, "chart.umd.js")
    if os.path.exists(src):
        shutil.copy(src, os.path.join(OUTPUT_DIR, "chart.umd.js"))
        return True
    return False


TEMPLATE = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Sales Data Analytics — Tableau de bord</title>
<style>
  :root{
    --bg:#eef1f7; --card:#ffffff; --ink:#161d2e; --muted:#67708a;
    --line:#e4e8f1; --line2:#eef1f7; --shadow:0 1px 2px rgba(20,30,60,.05),0 4px 16px rgba(20,30,60,.05);
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--ink);
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
    -webkit-font-smoothing:antialiased;line-height:1.45}
  .wrap{max-width:1180px;margin:0 auto;padding:0 22px 56px}
  header.top{background:linear-gradient(120deg,#4f6df5 0%,#7c5cfc 100%);color:#fff;
    padding:30px 0 34px;margin-bottom:-18px}
  header.top .wrap{padding-bottom:0}
  .brand{display:flex;align-items:center;gap:12px}
  .brand .logo{width:42px;height:42px;border-radius:12px;background:rgba(255,255,255,.18);
    display:flex;align-items:center;justify-content:center;font-size:22px}
  header.top h1{margin:0;font-size:21px;font-weight:600;letter-spacing:.2px}
  header.top p{margin:3px 0 0;font-size:13px;color:rgba(255,255,255,.82)}
  .kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(186px,1fr));gap:14px;margin:34px 0 26px}
  .kpi{background:var(--card);border-radius:16px;padding:18px 18px 16px;box-shadow:var(--shadow);
    border-top:3px solid var(--accent);position:relative}
  .kpi-val{font-size:25px;font-weight:700;letter-spacing:-.3px}
  .kpi-lab{font-size:12px;color:var(--muted);margin-top:5px;line-height:1.35}
  .section-title{font-size:13px;font-weight:600;text-transform:uppercase;letter-spacing:1px;
    color:var(--muted);margin:30px 4px 14px}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(440px,1fr));gap:18px}
  .card{background:var(--card);border-radius:16px;padding:20px 22px;box-shadow:var(--shadow)}
  .card h2{margin:0;font-size:16px;font-weight:600}
  .c-sub{margin:4px 0 0;font-size:12.5px;color:var(--muted)}
  .chart-card .canvas-wrap{position:relative;width:100%;height:300px;margin-top:14px}
  .chart-card.tall .canvas-wrap{height:380px}
  table{width:100%;border-collapse:collapse;font-size:13px;margin-top:14px}
  th,td{padding:9px 10px;text-align:right;border-bottom:1px solid var(--line2);white-space:nowrap}
  th:first-child,td:first-child{text-align:left}
  th{color:var(--muted);font-weight:600;font-size:11.5px;text-transform:uppercase;letter-spacing:.5px;
    border-bottom:1px solid var(--line)}
  td{font-variant-numeric:tabular-nums}
  tbody tr:hover{background:#f6f8fd}
  tbody tr:last-child td{border-bottom:none}
  .empty{color:var(--muted);font-style:italic}
  footer{margin-top:34px;color:#97a0b8;font-size:12px;text-align:center}
  @media(max-width:520px){.grid{grid-template-columns:1fr}}
</style>
</head>
<body>
  <header class="top">
    <div class="wrap">
      <div class="brand">
        <div class="logo">🛒</div>
        <div>
          <h1>Sales Data Analytics</h1>
          <p>Online Retail Dataset · Apache Spark + Hadoop HDFS · généré le __GENERATED__</p>
        </div>
      </div>
    </div>
  </header>

  <div class="wrap">
    <div class="kpis">__KPIS__</div>

    <div class="section-title">Analyses graphiques</div>
    <div class="grid">__CHARTS__</div>

    <div class="section-title">Clients &amp; pays — détails</div>
    <div class="grid">__TABLES__</div>

    <footer>Tableau de bord généré à partir des résultats PySpark (output/*.csv) — autonome &amp; hors-ligne.</footer>
  </div>

<script src="__CHARTJS__"></script>
<script>
const DATA = __DATA__;
const FONT = "-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif";
if (window.Chart) {
  Chart.defaults.font.family = FONT;
  Chart.defaults.font.size = 12;
  Chart.defaults.color = "#67708a";
  const gbp = v => "£" + Math.round(v).toLocaleString("en-GB");
  const kshort = v => "£" + (Math.abs(v) >= 1e6 ? (v/1e6).toFixed(1)+"M" : Math.round(v/1000)+"k");
  const grid = { color:"#eef1f7", drawBorder:false };
  const noGrid = { display:false, drawBorder:false };
  const tip = lbl => ({ callbacks:{ label: c => " " + lbl(c.parsed.y!=null?c.parsed.y:c.parsed.x) } });

  new Chart(chMonth, {
    type:"line",
    data:{ labels:DATA.month.labels, datasets:[{ data:DATA.month.values,
      borderColor:"#4f6df5", backgroundColor:"rgba(79,109,245,.12)", borderWidth:2.5,
      fill:true, tension:.38, pointRadius:3, pointBackgroundColor:"#4f6df5",
      pointHoverRadius:5 }] },
    options:{ responsive:true, maintainAspectRatio:false,
      plugins:{ legend:{display:false}, tooltip:tip(gbp) },
      scales:{ x:{ grid:noGrid }, y:{ grid, ticks:{ callback:kshort } } } }
  });

  new Chart(chDow, {
    type:"bar",
    data:{ labels:DATA.dow.labels, datasets:[{ data:DATA.dow.values,
      backgroundColor:"#1aa989", borderRadius:7, maxBarThickness:46 }] },
    options:{ responsive:true, maintainAspectRatio:false,
      plugins:{ legend:{display:false}, tooltip:tip(gbp) },
      scales:{ x:{ grid:noGrid }, y:{ grid, ticks:{ callback:kshort } } } }
  });

  new Chart(chHour, {
    type:"bar",
    data:{ labels:DATA.hour.labels, datasets:[{ data:DATA.hour.values,
      backgroundColor:"#7c5cfc", borderRadius:5, maxBarThickness:26 }] },
    options:{ responsive:true, maintainAspectRatio:false,
      plugins:{ legend:{display:false}, tooltip:tip(gbp) },
      scales:{ x:{ grid:noGrid }, y:{ grid, ticks:{ callback:kshort } } } }
  });

  const hbar = (cv, d, color) => new Chart(cv, {
    type:"bar",
    data:{ labels:d.labels, datasets:[{ data:d.values, backgroundColor:color,
      borderRadius:6, maxBarThickness:22 }] },
    options:{ indexAxis:"y", responsive:true, maintainAspectRatio:false,
      plugins:{ legend:{display:false}, tooltip:tip(gbp) },
      scales:{ x:{ grid, ticks:{ callback:kshort } }, y:{ grid:noGrid } } }
  });
  hbar(chProdRev, DATA.prodRev, "#4f6df5");
  hbar(chProdQty, DATA.prodQty, "#f59e0b");
  hbar(chCountry, DATA.country, "#ef6c4d");
}
</script>
</body>
</html>"""


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(DASHBOARD, "w", encoding="utf-8") as fh:
        fh.write(build())
    print(f"[dashboard] généré : {DASHBOARD}")
    print("[dashboard] ouvrir avec :  open output/dashboard.html")
