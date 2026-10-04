from __future__ import annotations
import argparse
import csv
import random
from datetime import date, timedelta
CATALOGUE = {'informatique': [('Notebook', 500), ('Ecran 27"', 150), ('PC Portable', 900), ('Disque SSD', 80)], 'telephonie': [('Smartphone', 600), ('Coque', 15), ('Chargeur', 25)], 'accessoires': [('Souris', 20), ('Clavier', 30), ('Tapis', 10), ('Webcam', 45)], 'audio': [('Casque', 70), ('Enceinte', 120), ('Ecouteurs', 50)], 'gaming': [('Manette', 60), ('Volant', 200), ('Chaise Gaming', 250)], 'electromenager': [('Cafetiere', 90), ('Grille-pain', 40)], 'bureautique': [('Imprimante', 130), ('Ramette', 8)], 'reseau': [('Routeur', 85), ('Switch', 110), ('Cable', 12)]}
REGIONS = ['Sao Paulo', 'Rio', 'Brasilia', 'Salvador', 'Fortaleza', 'Belo Horizonte', 'Curitiba']

def generer(rows: int, out: str, graine: int=42) -> None:
    random.seed(graine)
    produits = [(cat, nom, prix) for cat, items in CATALOGUE.items() for nom, prix in items]
    debut = date(2026, 1, 1)
    with open(out, 'w', newline='', encoding='utf-8') as fichier:
        writer = csv.writer(fichier)
        writer.writerow(['CommandeID', 'Date', 'Produit', 'Categorie', 'Quantite', 'Prix', 'Montant', 'Region', 'Client'])
        for i in range(rows):
            categorie, produit, prix = random.choice(produits)
            quantite = random.randint(1, 5)
            jour = debut + timedelta(days=random.randint(0, 300))
            client = f'U{random.randint(1, max(2, rows // 5))}'
            montant = round(prix * quantite, 2)
            tirage = random.random()
            if tirage < 0.01:
                montant = -montant
            elif tirage < 0.02:
                quantite = 0
            writer.writerow([f'C{i + 1}', jour.isoformat(), produit, categorie, quantite, prix, montant, random.choice(REGIONS), client])
    print(f'{rows} lignes écrites dans {out}')
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Génère un dataset e-commerce synthétique.')
    parser.add_argument('--rows', type=int, default=20000)
    parser.add_argument('--out', default='data/dataset.csv')
    parser.add_argument('--seed', type=int, default=42)
    args = parser.parse_args()
    generer(args.rows, args.out, args.seed)
