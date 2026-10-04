# Industrialisation d'un Pipeline E-commerce

Pipeline Data Engineering de bout en bout pour l'analyse des ventes d'un
e-commerce, industrialisé avec **Git**, **Jenkins**, **Apache Airflow**,
**MongoDB** et **Docker**.

## 1. Contexte

Les ventes étaient exportées en CSV et traitées manuellement (production tardive
des indicateurs, risque d'erreurs, pas d'historisation ni de traçabilité). Ce
projet met en place une plateforme automatisée qui **collecte, valide, calcule
et stocke** des indicateurs décisionnels fiables.

## 2. Architecture

```
Développeur → Git → Jenkins → (tests, validation DAG, déploiement, trigger)
                                     │
                                     ▼
                              Apache Airflow
                 (extraction → transformation → KPI → stockage)
                                     │
                                     ▼
                                 MongoDB
```

## 3. Dataset

Dataset cible : **Olist Brazilian E-Commerce**
(<https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce>).

Un générateur de données synthétiques respectant le même schéma est fourni pour
pouvoir exécuter le pipeline sans téléchargement :

```bash
python scripts/generate_dataset.py --rows 20000 --out data/dataset.csv
```

Schéma minimal (`data/dataset.csv`) :

| Champ        | Description                 |
|--------------|-----------------------------|
| CommandeID   | Identifiant unique commande |
| Date         | Date de la transaction      |
| Produit      | Produit vendu               |
| Categorie    | Catégorie du produit        |
| Quantite     | Quantité vendue             |
| Prix         | Prix unitaire               |
| Montant      | Montant total               |
| Region       | Région du client            |
| Client       | Identifiant du client       |

## 4. Structure du projet

```
airflow-ecommerce-project/
├── dags/
│   ├── ecommerce_sales_pipeline.py   # DAG Airflow
│   └── sales_metrics.py              # logique métier pure (testable)
├── tests/
│   └── test_pipeline.py              # tests pytest
├── data/
│   └── dataset.csv                   # données d'entrée
├── scripts/
│   ├── check_mongodb.py              # vérification du stockage (stage Jenkins)
│   └── generate_dataset.py           # générateur de dataset
├── docker/
│   └── airflow.Dockerfile
├── Jenkinsfile
├── requirements.txt
├── docker-compose.yml
└── README.md
```

## 5. DAG `ecommerce_sales_pipeline`

Étapes (cf. §6.2 du cahier des charges) :

1. `FileSensor` : attend la présence du CSV ;
2. vérifie l'existence du fichier ;
3. vérifie qu'il n'est pas vide (arrêt propre sinon) ;
4. contrôle qualité des données (lignes valides/rejetées → `errors.csv`) ;
5. `BranchPythonOperator` : choisit le chemin selon le taux de validité ;
6. charge les données ;
7. calcule les indicateurs métier ;
8. transmet les métriques via **XCom** ;
9. crée **dynamiquement** une tâche d'analyse par catégorie ;
10. gère les erreurs via **Trigger Rules** ;
11. génère un rapport final (`report.json`) ;
12. stocke les métriques dans **MongoDB**.

### Indicateurs calculés (§7)

Nombre de commandes, nombre de clients, chiffre d'affaires, panier moyen,
top 10 produits, CA par catégorie, CA par région, évolution mensuelle,
lignes valides, lignes rejetées.

### Règles de gestion (§8)

- identifiant de commande unique ;
- montant négatif → ligne rejetée ;
- quantité nulle/négative → invalide ;
- fichier vide → arrêt propre ;
- lignes incorrectes isolées dans `errors.csv` ;
- chaque exécution historisée ;
- statut enregistré : `success`, `failed` ou `partial`.

## 6. Stockage MongoDB (§9)

- Base : `ecommerce_analytics`
- Collection : `sales_metrics`

Un document par exécution (`global_metrics`, `top_products`, `region_metrics`,
`category_metrics`, `monthly_evolution`, `quality`).

## 7. CI/CD Jenkins (§10)

Stages du `Jenkinsfile` : `Checkout` → `Install dependencies` → `Run tests`
(pytest) → `Validate DAG` (`py_compile`) → `Deploy DAG` → `Trigger DAG` →
`Verify MongoDB`.

## 8. Conteneurisation (§11)

```bash
docker compose up -d --build
```

Services exposés :

| Service            | URL / Port             |
|--------------------|------------------------|
| Airflow Webserver  | http://localhost:8080  (admin / admin) |
| Jenkins            | http://localhost:8081  |
| MongoDB            | localhost:27017        |
| PostgreSQL         | interne (métadonnées Airflow) |

## 9. Exécution locale (sans Docker)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python scripts/generate_dataset.py --rows 20000 --out data/dataset.csv
pytest -v
```

## 10. Workflow Git

Branches `main` (stable) et `dev` (développement). Le travail est réalisé sur
`dev` puis fusionné vers `main`.
