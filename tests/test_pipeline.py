import os
import sys
import pandas as pd
import pytest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))
import sales_metrics

@pytest.fixture
def df_exemple() -> pd.DataFrame:
    return pd.DataFrame([{'CommandeID': 'C1', 'Date': '2026-01-05', 'Produit': 'Notebook', 'Categorie': 'informatique', 'Quantite': 2, 'Prix': 500, 'Montant': 1000, 'Region': 'Sao Paulo', 'Client': 'U1'}, {'CommandeID': 'C2', 'Date': '2026-01-12', 'Produit': 'Souris', 'Categorie': 'accessoires', 'Quantite': 3, 'Prix': 20, 'Montant': 60, 'Region': 'Sao Paulo', 'Client': 'U1'}, {'CommandeID': 'C3', 'Date': '2026-02-01', 'Produit': 'Notebook', 'Categorie': 'informatique', 'Quantite': 1, 'Prix': 500, 'Montant': 500, 'Region': 'Rio', 'Client': 'U2'}, {'CommandeID': 'C4', 'Date': '2026-02-15', 'Produit': 'Clavier', 'Categorie': 'accessoires', 'Quantite': 4, 'Prix': 30, 'Montant': 120, 'Region': 'Rio', 'Client': 'U2'}, {'CommandeID': 'C5', 'Date': '2026-03-03', 'Produit': 'Ecran', 'Categorie': 'informatique', 'Quantite': 2, 'Prix': 150, 'Montant': 300, 'Region': 'Sao Paulo', 'Client': 'U2'}, {'CommandeID': 'C6', 'Date': '2026-03-04', 'Produit': 'Ecran', 'Categorie': 'informatique', 'Quantite': 1, 'Prix': 150, 'Montant': -150, 'Region': 'Rio', 'Client': 'U2'}, {'CommandeID': 'C7', 'Date': '2026-03-05', 'Produit': 'Souris', 'Categorie': 'accessoires', 'Quantite': 0, 'Prix': 20, 'Montant': 0, 'Region': 'Rio', 'Client': 'U3'}, {'CommandeID': 'C1', 'Date': '2026-03-06', 'Produit': 'Notebook', 'Categorie': 'informatique', 'Quantite': 1, 'Prix': 500, 'Montant': 500, 'Region': 'Rio', 'Client': 'U3'}])

def test_validation_separe_valides_et_rejetees(df_exemple):
    valides, rejetees = sales_metrics.valider_lignes(df_exemple)
    assert len(valides) == 5
    assert len(rejetees) == 3
    assert set(rejetees['motif_rejet']) == {'montant_negatif', 'quantite_invalide', 'commande_dupliquee'}

def test_colonnes_manquantes_leve_erreur():
    df = pd.DataFrame([{'Date': '2026-01-01', 'Produit': 'X'}])
    with pytest.raises(ValueError):
        sales_metrics.valider_lignes(df)

def test_metriques_globales(df_exemple):
    valides, _ = sales_metrics.valider_lignes(df_exemple)
    metriques = sales_metrics.calculer_metriques_globales(valides)
    assert metriques['nb_commandes'] == 5
    assert metriques['nb_clients'] == 2
    assert metriques['chiffre_affaires'] == 1980.0
    assert metriques['panier_moyen'] == 396.0

def test_top_produits_trie_par_quantite(df_exemple):
    valides, _ = sales_metrics.valider_lignes(df_exemple)
    top = sales_metrics.calculer_top_produits(valides, limite=3)
    assert top[0]['product'] == 'Clavier'
    assert all(('revenue' in p for p in top))

def test_metriques_region(df_exemple):
    valides, _ = sales_metrics.valider_lignes(df_exemple)
    regions = {r['region']: r for r in sales_metrics.calculer_metriques_region(valides)}
    assert regions['Sao Paulo']['revenue'] == 1360.0
    assert regions['Rio']['revenue'] == 620.0

def test_metriques_categorie(df_exemple):
    valides, _ = sales_metrics.valider_lignes(df_exemple)
    categories = {c['category']: c for c in sales_metrics.calculer_metriques_categorie(valides)}
    assert categories['informatique']['revenue'] == 1800.0
    assert categories['accessoires']['revenue'] == 180.0

def test_evolution_mensuelle(df_exemple):
    valides, _ = sales_metrics.valider_lignes(df_exemple)
    evolution = sales_metrics.calculer_evolution_mensuelle(valides)
    mois = {e['month']: e['revenue'] for e in evolution}
    assert mois['2026-01'] == 1060.0
    assert mois['2026-02'] == 620.0
    assert mois['2026-03'] == 300.0

def test_document_final_structure(df_exemple):
    valides, rejetees = sales_metrics.valider_lignes(df_exemple)
    doc = sales_metrics.construire_document(execution_date='2026-06-01', source_file='dataset.csv', status='success', valides=valides, rejetees=rejetees)
    assert doc['dag_id'] == 'ecommerce_sales_pipeline'
    assert doc['global_metrics']['nb_commandes'] == 5
    assert doc['quality']['valid_rows'] == 5
    assert doc['quality']['invalid_rows'] == 3
    for cle in ('top_products', 'region_metrics', 'category_metrics', 'monthly_evolution'):
        assert cle in doc
