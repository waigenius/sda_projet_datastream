# json_to_elast_bulk.py
# Création d'index (mapping) + envoi rapide en BULK vers Elasticsearch

import os, json, math, time, sys
import requests

# ====== CONFIG ======
ES_BASE   = "http://localhost:9200"
INDEX     = "courses_index"           # nom de l'index
RESET     = False                     # True => DELETE/PUT avant import
PIPELINE  = None                      # ex: "courses_geo_pipeline" ou None

USE_NDJSON = True                    # True pour envoyer directement le NDJSON
SRC_JSON   = "data/courses_array.json"   # liste de documents (array JSON)
SRC_NDJSON = "data/courses_bulk.ndjson"  # NDJSON (paires {index}\n{doc}\n)
BULK_SIZE  = 1000                     # taille des lots si USE_NDJSON=False
TIMEOUT_S  = 300
# ====================

MAPPING_BODY = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {
            "agent_timestamp": {"type": "date"},
            "confort":         {"type": "keyword"},
            "prix_base_per_km":{"type": "float"},
            "distance_km":     {"type": "float"},
            "prix_travel":     {"type": "float"},
            "nomclient":       {"type": "keyword"},
            "telephoneClient": {"type": "keyword"},
            "nomDriver":       {"type": "keyword"},
            "telephoneDriver": {"type": "keyword"},
            "locationClient":  {"type": "geo_point"},
            "locationDriver":  {"type": "geo_point"}
        }
    }
}

def es(url, method="GET", **kw):
    r = requests.request(method, url, timeout=TIMEOUT_S, **kw)
    return r

def index_exists():
    r = es(f"{ES_BASE}/{INDEX}", "HEAD")
    return r.status_code == 200

def ensure_index():
    if RESET and index_exists():
        print(f"RESET: DELETE /{INDEX}")
        es(f"{ES_BASE}/{INDEX}", "DELETE").raise_for_status()

    if not index_exists():
        print(f"PUT /{INDEX} (mapping)")
        r = es(f"{ES_BASE}/{INDEX}", "PUT", json=MAPPING_BODY)
        try:
            r.raise_for_status()
        except Exception:
            print("Réponse:", r.text[:1000])
            raise
    else:
        print(f"Index déjà présent: {INDEX}")

def bulk_url():
    return f"{ES_BASE}/{INDEX}/_bulk" + (f"?pipeline={PIPELINE}" if PIPELINE else "")

def send_bulk_payload(payload: str):
    r = es(
        bulk_url(),
        "POST",
        headers={"Content-Type": "application/x-ndjson"},
        data=payload.encode("utf-8"),
    )
    r.raise_for_status()
    resp = r.json()
    errors = resp.get("errors", False)
    err_items = sum(1 for it in resp.get("items", []) if list(it.values())[0].get("error"))
    return errors, err_items, len(resp.get("items", []))

def bulk_from_json_array(path: str, batch_size: int):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "courses" in data:
        data = data["courses"]
    if not isinstance(data, list):
        raise ValueError("Le fichier doit contenir une LISTE de documents ou une clé 'courses'.")

    total = len(data)
    print(f"Docs à envoyer: {total} (batch={batch_size})")
    t0 = time.time()
    sent = 0
    total_err = 0

    for i in range(0, total, batch_size):
        batch = data[i:i+batch_size]
        lines = []
        for doc in batch:
            lines.append('{"index":{}}')
            lines.append(json.dumps(doc, ensure_ascii=False))
        payload = "\n".join(lines) + "\n"
        errors, err_items, items = send_bulk_payload(payload)

        sent += len(batch)
        total_err += err_items
        pct = math.floor(100 * sent / total)
        print(f"[{pct:3d}%] batch {i//batch_size+1} → ok={len(batch)-err_items}, err={err_items}")

    dt = time.time() - t0
    print(f"\nTerminé: envoyés={sent}, erreurs={total_err}, temps={dt:.1f}s "
          f"({sent/max(dt,1):.0f} docs/s)")

def bulk_from_ndjson(path: str):
    print(f"Envoi direct du NDJSON: {path}")
    t0 = time.time()
    with open(path, "rb") as f:
        r = es(
            bulk_url(),
            "POST",
            headers={"Content-Type": "application/x-ndjson"},
            data=f,
        )
    try:
        r.raise_for_status()
    except Exception:
        print("Réponse:", r.text[:2000])
        raise
    resp = r.json()
    err_items = sum(1 for it in resp.get("items", []) if list(it.values())[0].get("error"))
    items = len(resp.get("items", []))
    dt = time.time() - t0
    print(f"Terminé: items={items}, erreurs={err_items}, temps={dt:.1f}s "
          f"({items/max(dt,1):.0f} docs/s)")
    if resp.get("errors"):
        print("⚠️ Quelques items en erreur. Exemples :")
        shown = 0
        for it in resp["items"]:
            info = list(it.values())[0]
            if "error" in info:
                print(json.dumps(info["error"], ensure_ascii=False))
                shown += 1
                if shown >= 5:
                    break

if __name__ == "__main__":
    # 0) Vérifier qu'ES répond
    try:
        es(ES_BASE).raise_for_status()
    except Exception:
        print(f"❌ Elasticsearch introuvable sur {ES_BASE}. Lance ES puis réessaie.")
        sys.exit(1)

    # 1) Créer/Reset l'index avec mapping
    ensure_index()

    # 2) Envoi bulk
    if USE_NDJSON and os.path.exists(SRC_NDJSON):
        bulk_from_ndjson(SRC_NDJSON)
    else:
        bulk_from_json_array(SRC_JSON, BULK_SIZE)

    # 3) Petit check: afficher le count
    try:
        r = es(f"{ES_BASE}/{INDEX}/_count")
        print("Count:", r.json().get("count"))
    except Exception:
        pass
