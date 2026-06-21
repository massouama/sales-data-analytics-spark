#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extension Data Streaming — Spark Structured Streaming
=====================================================

Démonstration du traitement temps réel : Spark surveille un répertoire et
traite, au fur et à mesure de leur arrivée, des micro-lots de transactions
(fichiers CSV déposés par le producteur `scripts/stream_producer.sh`).

À chaque micro-lot, l'application met à jour et affiche en continu :
  - le chiffre d'affaires cumulé par pays (classement) ;
  - le nombre de commandes cumulé.

C'est l'équivalent "temps réel" de l'analyse batch `analytics.py`, sans
système de messagerie externe : la source de flux est un répertoire de fichiers.

Lancement (dans le conteneur spark-master) :

    /spark/bin/spark-submit /app/src/streaming.py --input /app/streaming_input
"""
from __future__ import annotations

import argparse
import os
import sys

# Permet d'importer les transformations partagées avec l'app batch.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from analytics import RAW_SCHEMA, basic_transform, clean_sales  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Sales Analytics — Streaming")
    parser.add_argument(
        "--input",
        default="/app/streaming_input",
        help="Répertoire surveillé (les fichiers CSV y sont déposés).",
    )
    parser.add_argument(
        "--checkpoint",
        default="/tmp/streaming_checkpoint",
        help="Répertoire de checkpoint du flux.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=1,
        help="Nombre max de fichiers traités par micro-lot (débit simulé).",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("SalesDataAnalyticsStreaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[streaming] Surveillance du répertoire : {args.input}")
    print("[streaming] Déposez des fichiers CSV (avec en-tête) pour voir le flux.")
    print("[streaming] Ctrl+C pour arrêter.\n")

    # Source de flux : fichiers CSV déposés dans le répertoire.
    raw_stream = (
        spark.readStream.option("header", True)
        .option("maxFilesPerTrigger", args.max_files)
        .schema(RAW_SCHEMA)
        .csv(args.input)
    )

    sales = clean_sales(basic_transform(raw_stream))

    # Agrégation cumulée (stateful) : CA et commandes par pays.
    by_country = sales.groupBy("Country").agg(
        F.round(F.sum("TotalPrice"), 2).alias("Revenue"),
        F.approx_count_distinct("InvoiceNo").alias("Orders"),
        F.sum("Quantity").alias("Quantity"),
    )

    query = (
        by_country.orderBy(F.desc("Revenue"))
        .writeStream.outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .option("checkpointLocation", args.checkpoint)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
