# Démarrage rapide — Sales Data Analytics with Apache Spark

> Guide pour lancer et vérifier le projet sur **n'importe quelle machine**.
> Aucune connaissance préalable du projet n'est nécessaire.

---

## 1. Prérequis (une seule chose)

- **Docker Desktop** installé et **démarré** (icône Docker active).
  - Télécharger : <https://www.docker.com/products/docker-desktop/>
- **Windows uniquement** : lancer les commandes `bash` ci-dessous depuis
  **WSL2** ou **Git Bash** (les scripts sont en `.sh`).
  Sur macOS / Linux, le Terminal suffit.

> Le jeu de données est **déjà inclus** dans l'archive (`data/online_retail.csv`),
> donc aucune connexion internet n'est requise pour les données. Seules les
> images Docker se téléchargent automatiquement au premier lancement.

---

## 2. Tout lancer — une seule commande

Ouvrir un terminal **dans le dossier du projet**, puis :

```bash
bash scripts/setup_all.sh
```

Cette commande enchaîne automatiquement :
1. préparation du jeu de données,
2. démarrage de la stack Docker (HDFS + Spark),
3. stockage du dataset dans HDFS,
4. exécution de l'application d'analyse PySpark.

> Premier lancement : quelques minutes (téléchargement des images Docker).
> Les suivants sont rapides.

---

## 3. Vérifier que « ça tourne »

| Quoi vérifier | Comment |
|---|---|
| **Résultats des analyses** | s'affichent dans le terminal (KPI + tableaux ventes/produits/clients/pays) |
| **Tableau de bord visuel** | ouvrir le fichier `output/dashboard.html` dans un navigateur |
| **Interface HDFS** (stockage) | <http://localhost:9871> → *Utilities → Browse the file system → /data* |
| **Interface Spark** (calcul) | <http://localhost:8082> |
| **Conteneurs actifs** | `docker compose ps` → 4 services *Up* |

Résultat attendu (extrait) : chiffre d'affaires total **10 666 684,54 £**,
**19 960** commandes, **4 338** clients, **38** pays.

---

## 4. Lancer les tests automatiques

```bash
docker exec spark-master /spark/bin/spark-submit --master local[1] /app/tests/test_transforms.py
```

Résultat attendu : **`SUCCÈS : tous les tests sont passés.`** (16 tests sur 16).

---

## 5. (Optionnel) Extension temps réel — 2 terminaux

```bash
# Terminal A : démarrer l'application de streaming
bash scripts/run_streaming.sh

# Terminal B : alimenter le flux
bash scripts/stream_producer.sh
```

On observe le chiffre d'affaires par pays se mettre à jour en continu.

---

## 6. Arrêter / nettoyer

```bash
docker compose down        # arrête les conteneurs
docker compose down -v     # + réinitialise les données HDFS
```

---

## Dépannage rapide

| Problème | Solution |
|---|---|
| `port is already allocated` | Un autre service utilise le port → le libérer, ou modifier le mapping dans `docker-compose.yml` |
| `Cannot connect to the Docker daemon` | Docker Desktop n'est pas démarré → le lancer |
| Le job Spark semble lent (Mac Apple Silicon) | Normal : images `amd64` émulées sur arm64 ; laisser tourner |
| `docker: command not found` | Docker n'est pas installé / pas dans le PATH |

---

## Contenu du projet

- `docker-compose.yml`, `hadoop.env` — infrastructure HDFS + Spark
- `src/` — application PySpark (`analytics.py`) + streaming (`streaming.py`)
- `scripts/` — scripts de lancement
- `tests/` — tests unitaires
- `docs/` — rapports (FR/EN) + guide de révision + document de validation
- `data/` — jeu de données
- `output/` — résultats générés (CSV + tableau de bord)
