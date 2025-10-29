#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
make_final_data_week.py
-----------------------
Génère des courses synthétiques sur **7 jours** à partir :
- d'un template JSON (structure des champs) -> data/data_projet.json
- d'un CSV de coordonnées -> data/uber-split2.csv

Produit 3 fichiers prêts pour Elasticsearch/Kibana :
1) data/courses_array.json   (liste JSON)
2) data/courses_stream.jsonl (JSON Lines)
3) data/courses_bulk.ndjson  (NDJSON pour l'API _bulk)

Notes :
- Timestamps en UTC, "timezone-aware" (pas de DeprecationWarning).
- Champs geo_point : locationClient / locationDriver
- Conserve au maximum les noms de clés du template (même "logitude" s'il est présent).
"""

import json, math, random, os
from datetime import datetime, timedelta, timezone
import pandas as pd

# ---------- Paramètres temporels ----------
DAYS = 7                  # nombre de jours (1 semaine)
TRIPS_PER_DAY = 500       # nombre de courses par jour (à ajuster)
PEAK_HOURS = [(7, 10), (17, 20)]  # heures de pointe pour la génération
PEAK_PROBA = 0.65         # probabilité de placer un trajet dans une heure de pointe
START_DATE = datetime.now(timezone.utc) - timedelta(days=DAYS-1)  # couvre J-(DAYS-1) ... J

# ---------- Fichiers d'entrée/sortie ----------
CSV_PATH = "data/uber-split2.csv"
TEMPLATE_PATH = "data/data_projet.json"
ARRAY_PATH = "data/courses_array.json"
JSONL_PATH = "data/courses_stream.jsonl"
BULK_PATH = "data/courses_bulk.ndjson"

# ---------- Helpers ----------

def load_template(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Accepte { "data": [ {...} ] } ou [ {...} ]
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], list) and data["data"]:
        return data["data"][0]
    elif isinstance(data, list) and data:
        return data[0]
    else:
        raise ValueError("Le fichier template ne contient pas de structure attendue (liste non vide).")

def find_lat_lon_cols(df):
    cols = [c for c in df.columns]
    lower_map = {c.lower(): c for c in cols}
    lat_candidates = [c for c in cols if "lat" in c.lower()]
    lon_candidates = [c for c in cols if ("lon" in c.lower()) or ("long" in c.lower())]
    if not lat_candidates or not lon_candidates:
        for common_lat in ["lat", "latitude", "pickup_latitude", "dropoff_latitude"]:
            if common_lat in lower_map:
                lat_candidates.append(lower_map[common_lat])
        for common_lon in ["lon", "longitude", "pickup_longitude", "dropoff_longitude"]:
            if common_lon in lower_map:
                lon_candidates.append(lower_map[common_lon])
    if not lat_candidates or not lon_candidates:
        raise ValueError("Impossible d'identifier les colonnes latitude/longitude dans le CSV.")
    return lat_candidates[0], lon_candidates[0]

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2.0)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2.0)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def jitter_coord(lat, lon, max_km=3.0):
    # +/- max_km autour du client pour simuler le driver proche
    km_per_deg_lat = 111.0
    km_per_deg_lon = 111.0 * math.cos(math.radians(lat))
    dlat_deg = (random.uniform(-max_km, max_km) / km_per_deg_lat)
    dlon_deg = (random.uniform(-max_km, max_km) / km_per_deg_lon) if km_per_deg_lon != 0 else 0.0
    return lat + dlat_deg, lon + dlon_deg

def random_time_in_day(day_dt):
    """Datetime UTC aléatoire dans la journée, biaisé vers heures de pointe."""
    if PEAK_HOURS and random.random() < PEAK_PROBA:
        h1, h2 = random.choice(PEAK_HOURS)
        hour = random.randint(h1, max(h1, h2-1))
    else:
        hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    micro = random.randint(0, 999999)
    return day_dt.replace(hour=hour, minute=minute, second=second, microsecond=micro)

# ---------- Chargement des entrées ----------
df = pd.read_csv(CSV_PATH)
template = load_template(TEMPLATE_PATH)
lat_col, lon_col = find_lat_lon_cols(df)

# Déterminer les clés exactes présentes dans le template
def get_prop_keys(section_key):
    section = template.get(section_key, {})
    keys = list(section.keys())
    lon_key = next((k for k in keys if "long" in k.lower()), None)   # match aussi "logitude"
    lat_key = next((k for k in keys if "lat"  in k.lower()), None)
    return lon_key or "longitude", lat_key or "latitude", keys

client_lon_key, client_lat_key, _ = get_prop_keys("properties-client")
driver_lon_key, driver_lat_key, _ = get_prop_keys("properties-driver")

# Confort & prix
confort_levels = ["low", "standard", "high"]
base_per_km_template = template.get("prix_base_per_km", 2)

def confort_price_per_km(confort):
    if confort == "low":
        return max(1.0, base_per_km_template * 0.8)
    if confort == "standard":
        return base_per_km_template
    if confort == "high":
        return base_per_km_template * 1.5
    return base_per_km_template

def fake_phone(prefix="07"):
    return prefix + "".join(random.choice("0123456789") for _ in range(8))

def fake_name(is_driver=False):
    drivers = ["DIOP", "NDAO", "KONE", "DIALLO", "OUATTARA", "KABONGO", "MARTIN", "DURAND"]
    clients = ["FALL", "NDIAYE", "KOTO", "NGBANGA", "DUPONT", "MOREAU", "BERTRAND", "MUKENDI"]
    return random.choice(drivers if is_driver else clients)

def build_message(lat, lon, ts_iso):
    confort = random.choice(confort_levels)
    # driver autour du client (≈3 km)
    dlat, dlon = jitter_coord(lat, lon, max_km=3.0)
    dist_km = haversine(lat, lon, dlat, dlon)
    ppk = confort_price_per_km(confort)
    price = round(ppk * max(1.0, dist_km), 2)

    return {
        "confort": confort,
        "prix_base_per_km": ppk,
        "properties-client": {
            client_lon_key: round(float(lon), 6),
            client_lat_key: round(float(lat), 6),
            "nomclient": fake_name(is_driver=False),
            "telephoneClient": fake_phone(prefix="06"),
        },
        "properties-driver": {
            driver_lon_key: round(float(dlon), 6),
            driver_lat_key: round(float(dlat), 6),
            "nomDriver": fake_name(is_driver=True),
            "telephoneDriver": fake_phone(prefix="07"),
        },
        "distance_km": round(dist_km, 3),
        "prix_travel": price,
        "agent_timestamp": ts_iso,

        # Champs geo_point pour Kibana Maps
        "locationClient": { "lat": round(float(lat), 6),  "lon": round(float(lon), 6) },
        "locationDriver": { "lat": round(float(dlat), 6), "lon": round(float(dlon), 6) }
    }

# échantillonner suffisamment de points pour une semaine
needed = DAYS * TRIPS_PER_DAY
sample = df.sample(min(len(df), needed), random_state=42).reset_index(drop=True)

messages = []
cursor = 0
for d in range(DAYS):
    day_dt = (START_DATE + timedelta(days=d)).replace(hour=0, minute=0, second=0, microsecond=0)
    # on peut aussi varier le volume par jour si besoin :
    trips_today = TRIPS_PER_DAY
    for _ in range(trips_today):
        if cursor >= len(sample):
            # si manque de points, on re-échantillonne au hasard dans le df complet
            row = df.sample(1).iloc[0]
        else:
            row = sample.iloc[cursor]
            cursor += 1

        lat = float(row[lat_col])
        lon = float(row[lon_col])
        if not (math.isfinite(lat) and math.isfinite(lon)):
            continue

        ts = random_time_in_day(day_dt)
        ts_iso = ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")  # ISO8601 UTC avec 'Z'

        messages.append(build_message(lat, lon, ts_iso))

# ---------- Écriture des sorties ----------

os.makedirs(os.path.dirname(ARRAY_PATH), exist_ok=True)

# 1) JSON array (joli)
with open(ARRAY_PATH, "w", encoding="utf-8") as f:
    json.dump(messages, f, ensure_ascii=False, indent=2)

# 2) JSON Lines (une ligne = un document)
with open(JSONL_PATH, "w", encoding="utf-8") as f:
    for m in messages:
        f.write(json.dumps(m, ensure_ascii=False) + "\n")

# 3) NDJSON pour Elastic _bulk
with open(BULK_PATH, "w", encoding="utf-8") as f:
    for m in messages:
        f.write('{"index":{}}\n')
        f.write(json.dumps(m, ensure_ascii=False) + "\n")

print("Fichiers générés :")
print(" - JSON array :", ARRAY_PATH)
print(" - JSON Lines :", JSONL_PATH)
print(" - BULK NDJSON:", BULK_PATH)
print("Total documents :", len(messages))
