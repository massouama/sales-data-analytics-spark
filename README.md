# 🛒 Sales Data Analytics with Apache Spark

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.3.0-E25A1C?logo=apachespark&logoColor=white)
![Hadoop HDFS](https://img.shields.io/badge/Hadoop%20HDFS-3.2.1-66CCFF?logo=apachehadoop&logoColor=black)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-PySpark-3776AB?logo=python&logoColor=white)
![License](https://img.shields.io/badge/usage-acad%C3%A9mique-lightgrey)

Solution d'analyse de données de ventes e-commerce avec **Apache Spark (PySpark)**
fonctionnant dans **Docker**, le jeu de données étant stocké dans **Hadoop HDFS**.

Projet réalisé sur le jeu de données **Online Retail** (UCI Machine Learning
Repository) : ~541 000 transactions d'un détaillant en ligne britannique.

> Source du dataset : <https://archive.ics.uci.edu/dataset/352/online+retail>

> ⚡ **Pour lancer le projet en une commande**, voir
> [`DEMARRAGE_RAPIDE.md`](DEMARRAGE_RAPIDE.md) :
> `bash scripts/setup_all.sh`

---

## 1. Objectifs (rappel du sujet)

**Partie 1 – Apache Spark** : application PySpark dans Docker qui
- charge le dataset,
- nettoie et transforme les données,
- réalise plusieurs analyses (ventes, produits, clients, pays),
- affiche les résultats,
- calcule des indicateurs métier (KPI).

**Partie 2 – Hadoop HDFS** : stocker le fichier du dataset dans HDFS.

**Extension (facultative)** : traitement temps réel (Spark Structured Streaming).

---

## 2. Architecture

```
                ┌──────────────────────────── Réseau Docker "bigdata" ───────────────────────────┐
                │                                                                                   │
  data/online_retail.csv ──put──►  ┌───────────┐        ┌───────────┐                              │
                │                   │ namenode  │◄──────►│ datanode  │   (Hadoop HDFS)              │
                │                   └─────┬─────┘        └───────────┘                              │
                │                         │ hdfs://namenode:9000/data/online_retail.csv             │
                │                         ▼                                                          │
                │                   ┌──────────────┐      ┌──────────────┐                          │
                │   spark-submit ──►│ spark-master │◄────►│ spark-worker │   (Apache Spark)         │
                │                   └──────┬───────┘      └──────────────┘                          │
                └──────────────────────────┼──────────────────────────────────────────────────────┘
                                           ▼
                                    ./output/*.csv   (résultats des analyses)
```

| Service        | Image                                            | UI web (hôte)            |
|----------------|--------------------------------------------------|--------------------------|
| `namenode`     | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | <http://localhost:9871> |
| `datanode`     | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | <http://localhost:9865> |
| `spark-master` | `bde2020/spark-master:3.3.0-hadoop3.3`            | <http://localhost:8082> |
| `spark-worker` | `bde2020/spark-worker:3.3.0-hadoop3.3`            | <http://localhost:8083> |

> Les ports hôtes (9871 / 9865 / 8082 / 8083) ont été choisis pour **éviter tout
> conflit** avec un éventuel Hadoop déjà installé sur la machine (qui utilise
> souvent 9870 / 9864 / 8088).

---

## 3. Prérequis

- **Docker** et **Docker Compose v2** (`docker compose`).
- **Python 3** + `pip` sur l'hôte (uniquement pour préparer le dataset :
  conversion xlsx → CSV via `pandas` / `openpyxl`, installés automatiquement).
- ~3 Go d'espace disque (images Docker) + ~70 Mo (dataset).
- Connexion internet (téléchargement du dataset et des images au premier lancement).

---

## 4. Démarrage rapide (tout-en-un)

```bash
cd "Projet"
bash scripts/setup_all.sh
```

Ce script enchaîne : préparation du dataset → démarrage de la stack →
stockage dans HDFS → exécution des analyses. Les résultats apparaissent dans
`./output/`.

---

## 5. Démarrage pas à pas

```bash
# 1) Télécharger et préparer le dataset (data/online_retail.csv)
bash scripts/download_data.sh

# 2) Démarrer la stack Docker (HDFS + Spark)
docker compose up -d

# 3) Stocker le dataset dans HDFS (Partie 2)
bash scripts/load_to_hdfs.sh

# 4) Lancer l'application d'analyse PySpark (Partie 1)
bash scripts/run_analytics.sh
```

### Extension streaming (facultatif) — 2 terminaux

```bash
# Terminal A : démarrer l'application de streaming
bash scripts/run_streaming.sh

# Terminal B : alimenter le flux (8 lots de 20000 lignes, 1 toutes les 5 s)
bash scripts/stream_producer.sh
```

### Tests unitaires

```bash
docker exec spark-master /spark/bin/spark-submit --master "local[1]" \
  /app/tests/test_transforms.py
```

### Arrêt

```bash
docker compose down            # arrête et supprime les conteneurs
docker compose down -v         # + supprime les volumes HDFS (réinitialise les données)
```

---

## 6. Structure du projet

```
Projet/
├── docker-compose.yml          # Stack HDFS + Spark
├── hadoop.env                  # Configuration Hadoop
├── README.md                   # Ce fichier
├── data/                       # Dataset (téléchargé, hors git)
├── src/
│   ├── analytics.py            # Application PySpark batch (Partie 1)
│   └── streaming.py            # Extension Structured Streaming
├── scripts/
│   ├── download_data.sh        # Téléchargement + conversion du dataset
│   ├── load_to_hdfs.sh         # Stockage HDFS (Partie 2)
│   ├── run_analytics.sh        # Soumission de l'app batch
│   ├── run_streaming.sh        # Lancement du streaming
│   ├── stream_producer.sh      # Producteur de flux
│   └── setup_all.sh            # Orchestration complète
├── tests/
│   └── test_transforms.py      # Tests unitaires (nettoyage, KPI, analyses)
├── output/                     # Résultats CSV générés
└── docs/
    ├── VALIDATION.md           # Document de validation (tests & résultats)
    └── RAPPORT.md              # Base du rapport (contexte, méthode, résultats)
```

---

## 7. Analyses réalisées

| Domaine   | Analyses |
|-----------|----------|
| **KPI**       | CA total, nb de commandes, nb de clients, nb de produits, quantité totale, panier moyen (AOV), articles/commande, taux d'annulation |
| **Ventes**    | CA par mois, CA par jour de semaine, CA par heure |
| **Produits**  | Top produits par CA, top produits par quantité vendue |
| **Clients**   | Top clients par CA, analyse RFM (Récence / Fréquence / Montant) |
| **Pays**      | CA / commandes / clients par pays |

Voir [`docs/VALIDATION.md`](docs/VALIDATION.md) pour les résultats détaillés et
les scénarios de test.
