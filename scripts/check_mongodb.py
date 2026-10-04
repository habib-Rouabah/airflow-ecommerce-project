"""Vérifie le contenu stocké dans MongoDB (stage « Verify MongoDB » du Jenkinsfile).

Affiche le nombre de documents et le dernier document inséré dans
`ecommerce_analytics.sales_metrics`. Code de sortie non nul si la collection est
vide ou inaccessible, pour faire échouer le pipeline Jenkins le cas échéant.
"""

from __future__ import annotations

import json
import os
import sys

from pymongo import MongoClient

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.environ.get("MONGO_DB", "ecommerce_analytics")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "sales_metrics")


def main() -> int:
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        collection = client[MONGO_DB][MONGO_COLLECTION]
        total = collection.count_documents({})
        print(f"Collection {MONGO_DB}.{MONGO_COLLECTION} : {total} document(s).")
        if total == 0:
            print("Aucun document trouvé.", file=sys.stderr)
            return 1
        dernier = collection.find_one(sort=[("_id", -1)])
        dernier.pop("_id", None)
        print(json.dumps(dernier, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:  # connexion/accès impossible → échec explicite
        print(f"Erreur d'accès MongoDB : {exc}", file=sys.stderr)
        return 1
    finally:
        try:
            client.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
