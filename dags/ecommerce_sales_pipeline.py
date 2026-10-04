"""DAG Airflow `ecommerce_sales_pipeline`.

Orchestration du pipeline e-commerce (cf. §6.2 du cahier des charges) :
FileSensor -> contrôles fichier -> qualité des données -> branchement ->
chargement -> calcul des KPI (XCom) -> analyses dynamiques par catégorie ->
rapport final -> stockage MongoDB. La gestion d'erreurs s'appuie sur les
Trigger Rules.

La logique de calcul pure vit dans `sales_metrics.py` (testable sans Airflow).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule

import sales_metrics

# ── Chemins (surclassables par variables d'environnement pour Docker/CI) ──────
DATA_DIR = os.environ.get("ECOMMERCE_DATA_DIR", "/opt/airflow/data")
DATASET_PATH = os.path.join(DATA_DIR, "dataset.csv")
ERROR_PATH = os.path.join(DATA_DIR, "errors.csv")
REPORT_PATH = os.path.join(DATA_DIR, "report.json")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/")
MONGO_DB = os.environ.get("MONGO_DB", "ecommerce_analytics")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "sales_metrics")

SEUIL_QUALITE = float(os.environ.get("ECOMMERCE_SEUIL_QUALITE", "0.5"))

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ── Tâches Python ─────────────────────────────────────────────────────────────


def verifier_existence_fichier(**_):
    """§6.2.2 : échoue explicitement si le fichier source est absent."""
    if not os.path.exists(DATASET_PATH):
        raise FileNotFoundError(f"Fichier source introuvable : {DATASET_PATH}")
    return DATASET_PATH


def verifier_fichier_non_vide(**_):
    """§6.2.3 : arrête proprement le workflow (skip) si le fichier est vide."""
    if os.path.getsize(DATASET_PATH) == 0:
        raise AirflowSkipException("Fichier source vide : arrêt propre du pipeline.")
    df = sales_metrics.charger_csv(DATASET_PATH)
    if df.empty:
        raise AirflowSkipException("Aucune ligne de données : arrêt propre du pipeline.")
    return int(len(df))


def controler_qualite(ti, **_):
    """§6.2.4 : sépare lignes valides/rejetées, écrit errors.csv, pousse les
    compteurs qualité en XCom."""
    df = sales_metrics.charger_csv(DATASET_PATH)
    valides, rejetees = sales_metrics.valider_lignes(df)

    if not rejetees.empty:
        rejetees.to_csv(ERROR_PATH, index=False)

    total = len(df)
    ratio_valide = len(valides) / total if total else 0.0
    ti.xcom_push(key="valid_rows", value=int(len(valides)))
    ti.xcom_push(key="invalid_rows", value=int(len(rejetees)))
    ti.xcom_push(key="ratio_valide", value=ratio_valide)
    return ratio_valide


def choisir_chemin(ti, **_):
    """§6.2.5 : BranchPythonOperator. Au-dessus du seuil de qualité on traite,
    sinon on route vers l'arrêt."""
    ratio = ti.xcom_pull(task_ids="controler_qualite", key="ratio_valide") or 0.0
    return "charger_donnees" if ratio >= SEUIL_QUALITE else "arreter_pipeline"


def arreter_pipeline(**_):
    """Branche d'arrêt : qualité insuffisante, le pipeline s'interrompt proprement."""
    raise AirflowSkipException("Qualité des données insuffisante : pipeline arrêté.")


def charger_donnees(ti, **_):
    """§6.2.6 : (re)charge et valide, pousse les catégories détectées en XCom."""
    df = sales_metrics.charger_csv(DATASET_PATH)
    valides, _ = sales_metrics.valider_lignes(df)
    categories = sales_metrics.lister_categories(valides)
    ti.xcom_push(key="categories", value=categories)
    return int(len(valides))


def calculer_kpis(ti, **_):
    """§6.2.7-8 : calcule tous les indicateurs métier et les transmet via XCom."""
    df = sales_metrics.charger_csv(DATASET_PATH)
    valides, rejetees = sales_metrics.valider_lignes(df)

    document = sales_metrics.construire_document(
        execution_date=datetime.utcnow().strftime("%Y-%m-%d"),
        source_file=os.path.basename(DATASET_PATH),
        status="success",
        valides=valides,
        rejetees=rejetees,
    )
    ti.xcom_push(key="document", value=document)
    return document["global_metrics"]


def analyser_categorie(categorie: str, **_):
    """Tâche d'analyse pour UNE catégorie (générée dynamiquement, §6.2.9)."""
    df = sales_metrics.charger_csv(DATASET_PATH)
    valides, _ = sales_metrics.valider_lignes(df)
    colonne = "Categorie" if "Categorie" in valides.columns else "Produit"
    sous_ensemble = valides[valides[colonne].astype(str) == categorie]
    return {
        "category": categorie,
        "quantity": int(sous_ensemble["Quantite"].sum()),
        "revenue": round(float(sous_ensemble["Montant"].sum()), 2),
    }


def generer_rapport(ti, **_):
    """§6.2.11 : agrège le document final ; `all_done` pour s'exécuter même en cas
    d'échec amont et refléter un statut `partial`/`failed`."""
    document = ti.xcom_pull(task_ids="calculer_kpis", key="document")
    invalid_rows = ti.xcom_pull(task_ids="controler_qualite", key="invalid_rows") or 0

    if document is None:
        document = {
            "execution_date": datetime.utcnow().strftime("%Y-%m-%d"),
            "dag_id": "ecommerce_sales_pipeline",
            "status": "failed",
            "quality": {"valid_rows": 0, "invalid_rows": int(invalid_rows), "error_file": "errors.csv"},
        }
    else:
        document["status"] = "partial" if invalid_rows else "success"

    with open(REPORT_PATH, "w", encoding="utf-8") as fichier:
        json.dump(document, fichier, ensure_ascii=False, indent=2)
    ti.xcom_push(key="document", value=document)
    return document["status"]


def stocker_mongodb(ti, **_):
    """§6.2.12 / §9 : insère le document d'indicateurs dans MongoDB."""
    from pymongo import MongoClient

    document = ti.xcom_pull(task_ids="generer_rapport", key="document")
    if document is None:
        raise ValueError("Aucun document à stocker.")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        collection = client[MONGO_DB][MONGO_COLLECTION]
        resultat = collection.insert_one(dict(document))
        return str(resultat.inserted_id)
    finally:
        client.close()


# ── Définition du DAG ─────────────────────────────────────────────────────────

with DAG(
    dag_id="ecommerce_sales_pipeline",
    description="Pipeline e-commerce : ingestion CSV, qualité, KPI et stockage MongoDB.",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "data-engineering"],
) as dag:
    attendre_fichier = FileSensor(
        task_id="attendre_fichier",
        filepath=DATASET_PATH,
        poke_interval=30,
        timeout=60 * 10,
        mode="reschedule",
    )

    existence = PythonOperator(task_id="verifier_existence_fichier", python_callable=verifier_existence_fichier)
    non_vide = PythonOperator(task_id="verifier_fichier_non_vide", python_callable=verifier_fichier_non_vide)
    qualite = PythonOperator(task_id="controler_qualite", python_callable=controler_qualite)

    branche = BranchPythonOperator(task_id="choisir_chemin", python_callable=choisir_chemin)
    arret = PythonOperator(task_id="arreter_pipeline", python_callable=arreter_pipeline)

    chargement = PythonOperator(task_id="charger_donnees", python_callable=charger_donnees)
    kpis = PythonOperator(task_id="calculer_kpis", python_callable=calculer_kpis)

    # §6.2.9 : tâches d'analyse créées dynamiquement, une par catégorie connue du
    # dataset. Catalogue figé au parsing du DAG (les catégories Olist sont stables).
    CATEGORIES_CONNUES = [
        "informatique",
        "telephonie",
        "accessoires",
        "audio",
        "gaming",
        "electromenager",
        "bureautique",
        "reseau",
    ]
    taches_categorie = []
    for _categorie in CATEGORIES_CONNUES:
        tache = PythonOperator(
            task_id=f"analyse_categorie_{_categorie}",
            python_callable=analyser_categorie,
            op_kwargs={"categorie": _categorie},
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
        taches_categorie.append(tache)

    rapport = PythonOperator(
        task_id="generer_rapport",
        python_callable=generer_rapport,
        trigger_rule=TriggerRule.ALL_DONE,  # §6.2.10 : s'exécute même sur échec amont
    )
    stockage = PythonOperator(
        task_id="stocker_mongodb",
        python_callable=stocker_mongodb,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    # Enchaînement
    attendre_fichier >> existence >> non_vide >> qualite >> branche
    branche >> arret >> rapport
    branche >> chargement >> kpis >> taches_categorie >> rapport
    rapport >> stockage
