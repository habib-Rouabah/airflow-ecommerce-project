"""Logique métier pure du pipeline e-commerce.

Ce module ne dépend PAS d'Airflow : il contient uniquement les fonctions de
validation et de calcul d'indicateurs, pour qu'elles soient testables avec
pytest sans avoir à installer Airflow. Le DAG `ecommerce_sales_pipeline.py`
importe ces fonctions.
"""

from __future__ import annotations

import pandas as pd

# Colonnes minimales attendues (cf. §5 du cahier des charges).
COLONNES_REQUISES = ["Date", "Produit", "Quantite", "Prix", "Montant", "Region", "Client"]


def charger_csv(chemin: str) -> pd.DataFrame:
    """Charge le CSV de ventes en typant les colonnes utiles."""
    df = pd.read_csv(chemin)
    df.columns = [c.strip() for c in df.columns]
    return df


def valider_lignes(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Sépare les lignes valides des lignes rejetées selon les règles métier (§8).

    Règles appliquées :
    - toutes les colonnes requises doivent être présentes et non nulles ;
    - `Montant` ne doit pas être négatif ;
    - `Quantite` doit être strictement positive ;
    - `Prix` ne doit pas être négatif ;
    - l'identifiant de commande (`CommandeID` si présent) doit être unique.

    Retourne (valides, rejetees). `rejetees` porte une colonne `motif_rejet`.
    """
    manquantes = [c for c in COLONNES_REQUISES if c not in df.columns]
    if manquantes:
        raise ValueError(f"Colonnes manquantes dans le fichier : {manquantes}")

    travail = df.copy()
    travail["Quantite"] = pd.to_numeric(travail["Quantite"], errors="coerce")
    travail["Prix"] = pd.to_numeric(travail["Prix"], errors="coerce")
    travail["Montant"] = pd.to_numeric(travail["Montant"], errors="coerce")

    motifs = pd.Series("", index=travail.index)

    champs_nuls = travail[COLONNES_REQUISES].isna().any(axis=1)
    motifs = motifs.mask(champs_nuls & (motifs == ""), "champ_requis_manquant")

    montant_negatif = travail["Montant"] < 0
    motifs = motifs.mask(montant_negatif & (motifs == ""), "montant_negatif")

    quantite_invalide = travail["Quantite"] <= 0
    motifs = motifs.mask(quantite_invalide & (motifs == ""), "quantite_invalide")

    prix_negatif = travail["Prix"] < 0
    motifs = motifs.mask(prix_negatif & (motifs == ""), "prix_negatif")

    # Unicité de l'identifiant de commande, seulement si la colonne existe.
    if "CommandeID" in travail.columns:
        doublons = travail["CommandeID"].duplicated(keep="first")
        motifs = motifs.mask(doublons & (motifs == ""), "commande_dupliquee")

    rejetees = travail[motifs != ""].copy()
    rejetees["motif_rejet"] = motifs[motifs != ""]
    valides = travail[motifs == ""].copy()
    return valides, rejetees


def calculer_metriques_globales(valides: pd.DataFrame) -> dict:
    """Indicateurs globaux : commandes, clients, CA, panier moyen (§7)."""
    nb_commandes = int(valides["CommandeID"].nunique()) if "CommandeID" in valides.columns else int(len(valides))
    nb_clients = int(valides["Client"].nunique())
    chiffre_affaires = float(valides["Montant"].sum())
    panier_moyen = round(chiffre_affaires / nb_commandes, 2) if nb_commandes else 0.0
    return {
        "nb_commandes": nb_commandes,
        "nb_clients": nb_clients,
        "chiffre_affaires": round(chiffre_affaires, 2),
        "panier_moyen": panier_moyen,
    }


def calculer_top_produits(valides: pd.DataFrame, limite: int = 10) -> list[dict]:
    """Top N des produits par quantité vendue, avec le CA associé (§7)."""
    groupe = valides.groupby("Produit").agg(sales=("Quantite", "sum"), revenue=("Montant", "sum"))
    top = groupe.sort_values("sales", ascending=False).head(limite)
    return [
        {"product": produit, "sales": int(ligne.sales), "revenue": round(float(ligne.revenue), 2)}
        for produit, ligne in top.iterrows()
    ]


def calculer_metriques_region(valides: pd.DataFrame) -> list[dict]:
    """CA et nombre de commandes par région (§7)."""
    cle_commande = "CommandeID" if "CommandeID" in valides.columns else "Client"
    groupe = valides.groupby("Region").agg(orders=(cle_commande, "nunique"), revenue=("Montant", "sum"))
    groupe = groupe.sort_values("revenue", ascending=False)
    return [
        {"region": region, "orders": int(ligne.orders), "revenue": round(float(ligne.revenue), 2)}
        for region, ligne in groupe.iterrows()
    ]


def calculer_metriques_categorie(valides: pd.DataFrame) -> list[dict]:
    """CA et quantités par catégorie. Replie sur `Produit` si pas de `Categorie`."""
    colonne = "Categorie" if "Categorie" in valides.columns else "Produit"
    groupe = valides.groupby(colonne).agg(quantity=("Quantite", "sum"), revenue=("Montant", "sum"))
    groupe = groupe.sort_values("revenue", ascending=False)
    return [
        {"category": cat, "quantity": int(ligne.quantity), "revenue": round(float(ligne.revenue), 2)}
        for cat, ligne in groupe.iterrows()
    ]


def calculer_evolution_mensuelle(valides: pd.DataFrame) -> list[dict]:
    """Évolution du CA agrégé par mois (§7)."""
    travail = valides.copy()
    travail["Date"] = pd.to_datetime(travail["Date"], errors="coerce")
    travail = travail.dropna(subset=["Date"])
    travail["mois"] = travail["Date"].dt.to_period("M").astype(str)
    groupe = travail.groupby("mois").agg(revenue=("Montant", "sum")).sort_index()
    return [{"month": mois, "revenue": round(float(ligne.revenue), 2)} for mois, ligne in groupe.iterrows()]


def lister_categories(valides: pd.DataFrame) -> list[str]:
    """Catégories distinctes présentes dans les données (pour les tâches dynamiques)."""
    colonne = "Categorie" if "Categorie" in valides.columns else "Produit"
    return sorted(str(v) for v in valides[colonne].dropna().unique())


def construire_document(
    execution_date: str,
    source_file: str,
    status: str,
    valides: pd.DataFrame,
    rejetees: pd.DataFrame,
    dataset: str = "olist",
    error_file: str = "errors.csv",
) -> dict:
    """Assemble le document MongoDB final (§9)."""
    return {
        "execution_date": execution_date,
        "dag_id": "ecommerce_sales_pipeline",
        "dataset": dataset,
        "source_file": source_file,
        "status": status,
        "global_metrics": calculer_metriques_globales(valides),
        "top_products": calculer_top_produits(valides),
        "category_metrics": calculer_metriques_categorie(valides),
        "region_metrics": calculer_metriques_region(valides),
        "monthly_evolution": calculer_evolution_mensuelle(valides),
        "quality": {
            "valid_rows": int(len(valides)),
            "invalid_rows": int(len(rejetees)),
            "error_file": error_file,
        },
    }
