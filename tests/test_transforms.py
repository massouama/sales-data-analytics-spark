#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests unitaires des transformations (logique de nettoyage et KPI).
==================================================================

Tests déterministes sur un petit jeu de données contrôlé, exécutés en
Spark local (pas besoin du cluster ni de HDFS). Aucune dépendance externe
(ni pytest) : succès/échec via `assert`, code de sortie non nul si échec.

Lancement (dans le conteneur spark-master) :
    docker exec spark-master /spark/bin/spark-submit /app/tests/test_transforms.py
"""
from __future__ import annotations

import os
import sys

# Importer le module analytics depuis src/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
sys.path.insert(0, "/app/src")  # chemin dans le conteneur

from pyspark.sql import SparkSession  # noqa: E402

from analytics import (  # noqa: E402
    RAW_SCHEMA,
    basic_transform,
    clean_sales,
    compute_kpis,
    revenue_by_country,
    top_products_by_revenue,
)

# Jeu de données de test : 6 lignes couvrant tous les cas de nettoyage.
TEST_ROWS = [
    # valides
    ("536365", "85123A", "WHITE HANGING HEART", 6, "2010-12-01 08:26:00", 2.55, 17850, "United Kingdom"),
    ("536365", "71053", "WHITE METAL LANTERN", 6, "2010-12-01 08:26:00", 3.39, 17850, "United Kingdom"),
    ("536366", "22633", "HAND WARMER", 2, "2010-12-02 09:00:00", 1.85, 17851, "France"),
    # annulation (InvoiceNo commence par 'C') -> à exclure
    ("C536379", "22633", "HAND WARMER", -1, "2010-12-02 09:10:00", 1.85, 17851, "France"),
    # prix nul + client manquant -> à exclure
    ("536380", "99999", "FREE GIFT", 5, "2010-12-03 10:00:00", 0.0, None, "United Kingdom"),
    # quantité négative -> à exclure
    ("536381", "85123A", "WHITE HANGING HEART", -3, "2010-12-03 11:00:00", 2.55, 17852, "Germany"),
]


def approx(a: float, b: float, tol: float = 0.01) -> bool:
    return abs(a - b) <= tol


def main() -> int:
    spark = (
        SparkSession.builder.appName("TestTransforms")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    raw = spark.createDataFrame(TEST_ROWS, schema=RAW_SCHEMA)
    typed = basic_transform(raw)
    sales = clean_sales(typed)

    failures = []

    def check(name, cond):
        status = "PASS" if cond else "FAIL"
        print(f"  [{status}] {name}")
        if not cond:
            failures.append(name)

    print("\n=== Tests de nettoyage / transformation ===")

    # 1. Le nettoyage ne garde que les 3 lignes valides
    n_valid = sales.count()
    check(f"clean_sales garde 3 lignes valides (obtenu {n_valid})", n_valid == 3)

    # 2. TotalPrice calculé correctement (6 * 2.55 = 15.30)
    row1 = sales.filter("StockCode = '85123A'").collect()[0]
    check("TotalPrice = Quantity * UnitPrice", approx(row1["TotalPrice"], 15.30))

    # 3. Indicateur d'annulation détecté
    n_cancel = typed.filter("IsCancellation = true").count()
    check(f"1 annulation détectée (obtenu {n_cancel})", n_cancel == 1)

    # 4. Colonnes temporelles dérivées présentes
    cols = set(sales.columns)
    check(
        "colonnes dérivées présentes (YearMonth, DayOfWeek, Hour)",
        {"YearMonth", "DayOfWeek", "Hour", "TotalPrice"}.issubset(cols),
    )

    print("\n=== Tests des KPI ===")
    kpis = compute_kpis(typed, sales)
    revenue = kpis["Chiffre d'affaires total (£)"]
    check("Lignes brutes = 6", kpis["Lignes brutes"] == 6)
    check("Lignes valides = 3", kpis["Lignes de ventes valides"] == 3)
    check(
        "CA total = 39.34 (obtenu {})".format(revenue),
        approx(revenue, 39.34),
    )
    check("Commandes = 2", kpis["Nombre de commandes (factures)"] == 2)
    check("Clients distincts = 2", kpis["Nombre de clients distincts"] == 2)
    check("Produits distincts = 3", kpis["Nombre de produits distincts"] == 3)
    check("Quantité totale = 14", kpis["Quantite totale vendue"] == 14)
    check("AOV = 19.67", approx(kpis["Panier moyen / AOV (£)"], 19.67))
    check(
        "Taux d'annulation = 16.67%",
        approx(kpis["Taux d'annulation (%)"], 16.67),
    )

    print("\n=== Tests des analyses ===")
    top_prod = top_products_by_revenue(sales, 10).collect()
    check(
        "Top produit par CA = 71053 (20.34)",
        top_prod[0]["StockCode"] == "71053" and approx(top_prod[0]["Revenue"], 20.34),
    )
    by_country = {r["Country"]: r["Revenue"] for r in revenue_by_country(sales, 10).collect()}
    check("CA Royaume-Uni = 35.64", approx(by_country.get("United Kingdom", 0), 35.64))
    check("CA France = 3.70", approx(by_country.get("France", 0), 3.70))

    spark.stop()

    print("\n" + "=" * 50)
    if failures:
        print(f"ÉCHEC : {len(failures)} test(s) en échec :")
        for f in failures:
            print(f"  - {f}")
        print("=" * 50)
        return 1
    print("SUCCÈS : tous les tests sont passés.")
    print("=" * 50)
    return 0


if __name__ == "__main__":
    sys.exit(main())
