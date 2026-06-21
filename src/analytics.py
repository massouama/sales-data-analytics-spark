#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sales Data Analytics with Apache Spark — Application batch (Partie 1)
====================================================================

Application PySpark qui :

  1. Charge le jeu de données "Online Retail" (depuis HDFS par défaut).
  2. Nettoie et transforme les données.
  3. Réalise plusieurs analyses : ventes, produits, clients, pays.
  4. Affiche les résultats à l'utilisateur (console).
  5. Calcule des indicateurs métier (KPI) et des métriques de performance.
  6. Sauvegarde chaque résultat dans des fichiers CSV (répertoire de sortie).

Lancement type (dans le conteneur spark-master) :

    /spark/bin/spark-submit --master spark://spark-master:7077 \
        /app/src/analytics.py \
        --input hdfs://namenode:9000/data/online_retail.csv \
        --output /app/output

Les fonctions de transformation sont écrites pour être importables et testées
unitairement (voir tests/test_transforms.py).
"""
from __future__ import annotations

import argparse
import csv
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Schéma explicite : plus robuste et rapide qu'une inférence automatique.
# InvoiceDate est lu en texte puis converti en timestamp (voir basic_transform).
RAW_SCHEMA = StructType(
    [
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", StringType(), True),
        StructField("UnitPrice", DoubleType(), True),
        StructField("CustomerID", IntegerType(), True),
        StructField("Country", StringType(), True),
    ]
)

DATE_FMT = "yyyy-MM-dd HH:mm:ss"


# --------------------------------------------------------------------------- #
#  Chargement                                                                  #
# --------------------------------------------------------------------------- #
def load_data(spark: SparkSession, path: str) -> DataFrame:
    """Charge le CSV brut avec le schéma explicite."""
    return (
        spark.read.option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .schema(RAW_SCHEMA)
        .csv(path)
    )


# --------------------------------------------------------------------------- #
#  Transformation / nettoyage                                                  #
# --------------------------------------------------------------------------- #
def basic_transform(df: DataFrame) -> DataFrame:
    """
    Typage et enrichissement SANS filtrage de lignes.

    - Conversion de InvoiceDate en timestamp.
    - Nettoyage des chaînes (trim).
    - Indicateur d'annulation (les factures annulées commencent par 'C').
    - Colonne dérivée TotalPrice = Quantity * UnitPrice.
    - Colonnes temporelles : Year, Month, YearMonth, DayOfWeek, Hour.

    On ne filtre rien ici afin de pouvoir calculer des indicateurs globaux
    (ex. taux d'annulation) avant nettoyage.
    """
    return (
        df.withColumn("InvoiceNo", F.trim(F.col("InvoiceNo")))
        .withColumn("StockCode", F.trim(F.col("StockCode")))
        .withColumn("Description", F.trim(F.col("Description")))
        .withColumn("Country", F.trim(F.col("Country")))
        .withColumn("InvoiceTimestamp", F.to_timestamp("InvoiceDate", DATE_FMT))
        .withColumn(
            "IsCancellation",
            F.col("InvoiceNo").startswith("C").cast("boolean"),
        )
        .withColumn("TotalPrice", F.round(F.col("Quantity") * F.col("UnitPrice"), 2))
        .withColumn("Year", F.year("InvoiceTimestamp"))
        .withColumn("Month", F.month("InvoiceTimestamp"))
        .withColumn("YearMonth", F.date_format("InvoiceTimestamp", "yyyy-MM"))
        .withColumn("DayOfWeek", F.date_format("InvoiceTimestamp", "EEEE"))
        .withColumn("Hour", F.hour("InvoiceTimestamp"))
    )


def clean_sales(df: DataFrame) -> DataFrame:
    """
    Garde uniquement les lignes de ventes valides :

    - hors annulations,
    - Quantity > 0,
    - UnitPrice > 0,
    - StockCode et InvoiceNo non nuls.

    C'est la base analytique pour le chiffre d'affaires.
    """
    return df.filter(
        (~F.col("IsCancellation"))
        & (F.col("Quantity") > 0)
        & (F.col("UnitPrice") > 0)
        & F.col("InvoiceNo").isNotNull()
        & F.col("StockCode").isNotNull()
    )


# --------------------------------------------------------------------------- #
#  KPI globaux                                                                 #
# --------------------------------------------------------------------------- #
def compute_kpis(typed: DataFrame, sales: DataFrame) -> dict:
    """Calcule les indicateurs métier globaux."""
    total_rows = typed.count()
    cancel_rows = typed.filter(F.col("IsCancellation")).count()

    agg = sales.agg(
        F.round(F.sum("TotalPrice"), 2).alias("revenue"),
        F.countDistinct("InvoiceNo").alias("orders"),
        F.countDistinct("CustomerID").alias("customers"),
        F.countDistinct("StockCode").alias("products"),
        F.sum("Quantity").alias("quantity"),
        F.countDistinct("Country").alias("countries"),
    ).collect()[0]

    orders = agg["orders"] or 0
    revenue = agg["revenue"] or 0.0
    quantity = agg["quantity"] or 0

    return {
        "Lignes brutes": total_rows,
        "Lignes de ventes valides": sales.count(),
        "Chiffre d'affaires total (£)": round(revenue, 2),
        "Nombre de commandes (factures)": orders,
        "Nombre de clients distincts": agg["customers"] or 0,
        "Nombre de produits distincts": agg["products"] or 0,
        "Quantite totale vendue": quantity,
        "Nombre de pays": agg["countries"] or 0,
        "Panier moyen / AOV (£)": round(revenue / orders, 2) if orders else 0.0,
        "Articles moyens par commande": round(quantity / orders, 2) if orders else 0.0,
        "Taux d'annulation (%)": round(100.0 * cancel_rows / total_rows, 2)
        if total_rows
        else 0.0,
    }


# --------------------------------------------------------------------------- #
#  Analyses                                                                    #
# --------------------------------------------------------------------------- #
def revenue_by_month(sales: DataFrame) -> DataFrame:
    return (
        sales.groupBy("YearMonth")
        .agg(
            F.round(F.sum("TotalPrice"), 2).alias("Revenue"),
            F.countDistinct("InvoiceNo").alias("Orders"),
            F.sum("Quantity").alias("Quantity"),
        )
        .orderBy("YearMonth")
    )


def revenue_by_dayofweek(sales: DataFrame) -> DataFrame:
    # On garde l'index numérique pour trier (1=Lundi ... 7=Dimanche).
    return (
        sales.withColumn("dow_idx", ((F.dayofweek("InvoiceTimestamp") + 5) % 7) + 1)
        .groupBy("DayOfWeek", "dow_idx")
        .agg(F.round(F.sum("TotalPrice"), 2).alias("Revenue"))
        .orderBy("dow_idx")
        .drop("dow_idx")
    )


def revenue_by_hour(sales: DataFrame) -> DataFrame:
    return (
        sales.groupBy("Hour")
        .agg(F.round(F.sum("TotalPrice"), 2).alias("Revenue"))
        .orderBy("Hour")
    )


def top_products_by_revenue(sales: DataFrame, n: int = 10) -> DataFrame:
    return (
        sales.groupBy("StockCode", "Description")
        .agg(F.round(F.sum("TotalPrice"), 2).alias("Revenue"))
        .orderBy(F.desc("Revenue"))
        .limit(n)
    )


def top_products_by_quantity(sales: DataFrame, n: int = 10) -> DataFrame:
    return (
        sales.groupBy("StockCode", "Description")
        .agg(F.sum("Quantity").alias("QuantitySold"))
        .orderBy(F.desc("QuantitySold"))
        .limit(n)
    )


def top_customers_by_revenue(sales: DataFrame, n: int = 10) -> DataFrame:
    return (
        sales.filter(F.col("CustomerID").isNotNull())
        .groupBy("CustomerID")
        .agg(
            F.round(F.sum("TotalPrice"), 2).alias("Revenue"),
            F.countDistinct("InvoiceNo").alias("Orders"),
            F.sum("Quantity").alias("Quantity"),
        )
        .orderBy(F.desc("Revenue"))
        .limit(n)
    )


def customer_rfm(sales: DataFrame, n: int = 10) -> DataFrame:
    """
    Analyse RFM simplifiée par client :
      - Recency  : nombre de jours depuis le dernier achat (réf. = date max du jeu)
      - Frequency: nombre de commandes distinctes
      - Monetary : chiffre d'affaires total

    Triée par valeur monétaire décroissante (meilleurs clients).
    """
    max_date = sales.agg(F.max("InvoiceTimestamp").alias("m")).collect()[0]["m"]
    return (
        sales.filter(F.col("CustomerID").isNotNull())
        .groupBy("CustomerID")
        .agg(
            F.max("InvoiceTimestamp").alias("LastPurchase"),
            F.countDistinct("InvoiceNo").alias("Frequency"),
            F.round(F.sum("TotalPrice"), 2).alias("Monetary"),
        )
        .withColumn("RecencyDays", F.datediff(F.lit(max_date), F.col("LastPurchase")))
        .select("CustomerID", "RecencyDays", "Frequency", "Monetary")
        .orderBy(F.desc("Monetary"))
        .limit(n)
    )


def revenue_by_country(sales: DataFrame, n: int = 10) -> DataFrame:
    return (
        sales.groupBy("Country")
        .agg(
            F.round(F.sum("TotalPrice"), 2).alias("Revenue"),
            F.countDistinct("InvoiceNo").alias("Orders"),
            F.countDistinct("CustomerID").alias("Customers"),
        )
        .orderBy(F.desc("Revenue"))
        .limit(n)
    )


# --------------------------------------------------------------------------- #
#  Affichage + sauvegarde                                                      #
# --------------------------------------------------------------------------- #
def show_and_save(df: DataFrame, name: str, output_dir: str, n: int = 20) -> None:
    """Affiche le résultat à l'écran et l'écrit en CSV propre (1 fichier)."""
    print(f"\n{'=' * 70}\n>>> {name}\n{'=' * 70}")
    rows = df.limit(n).collect()
    # Affichage console
    df.show(n, truncate=False)
    # Sauvegarde CSV (depuis le driver -> 1 fichier propre)
    os.makedirs(output_dir, exist_ok=True)
    safe = name.lower().replace(" ", "_").replace("/", "_").replace("'", "")
    path = os.path.join(output_dir, f"{safe}.csv")
    if rows:
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(rows[0].asDict().keys())
            for r in rows:
                writer.writerow(list(r.asDict().values()))
        print(f"[saved] {path}")


def save_kpis(kpis: dict, output_dir: str) -> None:
    print(f"\n{'=' * 70}\n>>> INDICATEURS METIER (KPI)\n{'=' * 70}")
    width = max(len(k) for k in kpis)
    for k, v in kpis.items():
        print(f"  {k:<{width}} : {v}")
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "kpis.csv")
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["Indicateur", "Valeur"])
        for k, v in kpis.items():
            writer.writerow([k, v])
    print(f"\n[saved] {path}")


# --------------------------------------------------------------------------- #
#  Programme principal                                                         #
# --------------------------------------------------------------------------- #
def main() -> None:
    parser = argparse.ArgumentParser(description="Sales Data Analytics (PySpark)")
    parser.add_argument(
        "--input",
        default="hdfs://namenode:9000/data/online_retail.csv",
        help="Chemin du CSV source (HDFS ou local).",
    )
    parser.add_argument(
        "--output",
        default="/app/output",
        help="Répertoire de sortie pour les résultats CSV.",
    )
    parser.add_argument("--top", type=int, default=10, help="Taille des classements.")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("SalesDataAnalytics")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n[analytics] Lecture des données : {args.input}")
    raw = load_data(spark, args.input)
    typed = basic_transform(raw)
    sales = clean_sales(typed).cache()

    # ---- KPI globaux ----
    kpis = compute_kpis(typed, sales)
    save_kpis(kpis, args.output)

    # ---- Analyses VENTES ----
    show_and_save(revenue_by_month(sales), "Ventes - CA par mois", args.output, 24)
    show_and_save(
        revenue_by_dayofweek(sales), "Ventes - CA par jour de semaine", args.output, 7
    )
    show_and_save(revenue_by_hour(sales), "Ventes - CA par heure", args.output, 24)

    # ---- Analyses PRODUITS ----
    show_and_save(
        top_products_by_revenue(sales, args.top),
        "Produits - Top par chiffre d'affaires",
        args.output,
        args.top,
    )
    show_and_save(
        top_products_by_quantity(sales, args.top),
        "Produits - Top par quantite vendue",
        args.output,
        args.top,
    )

    # ---- Analyses CLIENTS ----
    show_and_save(
        top_customers_by_revenue(sales, args.top),
        "Clients - Top par chiffre d'affaires",
        args.output,
        args.top,
    )
    show_and_save(
        customer_rfm(sales, args.top),
        "Clients - Analyse RFM (meilleurs clients)",
        args.output,
        args.top,
    )

    # ---- Analyses PAYS ----
    show_and_save(
        revenue_by_country(sales, args.top),
        "Pays - CA par pays",
        args.output,
        args.top,
    )

    print("\n[analytics] Analyses terminées. Résultats CSV dans :", args.output)
    spark.stop()


if __name__ == "__main__":
    main()
