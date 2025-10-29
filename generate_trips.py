#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import random
import argparse
from datetime import datetime, timedelta, timezone
from math import radians, sin, cos, asin, sqrt
from pathlib import Path
from typing import Tuple, List, Dict, Any


# =========================
# Configuration par défaut
# =========================
DEFAULT_N_TRIPS = 3000
OUTPUT_FILE = Path("data/data_projet_vtest.json")

CONFORT_LEVELS = {
    "low": 1.5,
    "standard": 2.0,
    "high": 3.0,
}

CLIENT_NAMES = [
    "FALL", "NDIAYE", "DIOUF", "SOW", "BA",
    "GUEYE", "MUKENDI", "KANE", "NDAO", "SECK"
]
DRIVER_NAMES = [
    "DIOP", "CAMARA", "NDAO", "SECK", "DIALLO",
    "MARTIN", "GUEYE", "LO", "BA", "NDIAYE"
]


# =========================
# Utilitaires
# =========================
def iso_z(dt: datetime) -> str:
    """Format ISO8601 sans microsecondes, suffixe Z (UTC)."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso_z(s: str) -> datetime:
    """Parse 'YYYY-MM-DDTHH:MM:SSZ' en datetime UTC."""
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def random_timestamp_between(start: datetime, end: datetime) -> str:
    """Timestamp uniforme entre start et end (UTC), formaté en Z."""
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    total = (end - start).total_seconds()
    pick = start + timedelta(seconds=random.uniform(0, total))
    return iso_z(pick)


def random_coord_near_nyc() -> Tuple[float, float]:
    """
    Coordonnée (lon, lat) proche de NYC (~30 km autour de Manhattan).
    Manhattan ~ lon=-73.9857, lat=40.7484
    """
    lon = random.uniform(-74.10, -73.80)
    lat = random.uniform(40.60, 40.90)
    return lon, lat


def haversine(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Distance entre 2 points (km)."""
    R = 6371.0
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon, dlat = lon2 - lon1, lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return R * 2 * asin(sqrt(a))


# =========================
# Génération
# =========================
def generate_trips(n_trips: int, start: datetime, end: datetime) -> List[Dict[str, Any]]:
    trips: List[Dict[str, Any]] = []

    for _ in range(n_trips):
        confort = random.choice(list(CONFORT_LEVELS.keys()))
        prix_base = CONFORT_LEVELS[confort]

        # Coordonnées NYC
        lon_c, lat_c = random_coord_near_nyc()
        lon_d, lat_d = random_coord_near_nyc()

        # Distance & prix
        dist_km = round(haversine(lon_c, lat_c, lon_d, lat_d), 3)
        prix = round(prix_base * dist_km, 2)

        # Identités
        client = random.choice(CLIENT_NAMES)
        driver = random.choice(DRIVER_NAMES)

        # Timestamp uniforme entre start et end (UTC)
        ts = random_timestamp_between(start, end)

        trip = {
            "confort": confort,
            "prix_base_per_km": prix_base,
            "properties-client": {
                "longitude": round(lon_c, 6),
                "latitude": round(lat_c, 6),
                "nomclient": client,
                "telephoneClient": f"06{random.randint(10000000, 99999999)}",
                # compatibilité DAGs (string "lon, lat")
                "location": f"{lon_c}, {lat_c}",
            },
            "properties-driver": {
                "longitude": round(lon_d, 6),
                "latitude": round(lat_d, 6),
                "nomDriver": driver,
                "telephoneDriver": f"07{random.randint(10000000, 99999999)}",
                "location": f"{lon_d}, {lat_d}",
            },
            # Champs utilisés par tes DAGs
            "distance": dist_km,
            "prix_travel": prix,
            "agent_timestamp": ts,
        }

        trips.append(trip)

    return trips


# =========================
# Main CLI
# =========================
def main():
    parser = argparse.ArgumentParser(
        description="Génère des trajets NYC compatibles avec les DAGs Airflow (DAG1→DAG2)."
    )
    parser.add_argument("--n", type=int, default=DEFAULT_N_TRIPS,
                        help=f"Nombre de trajets à générer (défaut={DEFAULT_N_TRIPS})")
    parser.add_argument("--start", type=str, default=None,
                        help="Début ISO8601 UTC (ex: 2025-10-01T00:00:00Z). Défaut: 1er octobre de l'année courante.")
    parser.add_argument("--end", type=str, default=None,
                        help="Fin ISO8601 UTC (ex: 2025-10-29T23:59:59Z). Défaut: maintenant (UTC).")
    parser.add_argument("--seed", type=int, default=None,
                        help="Graine aléatoire pour reproductibilité (optionnel).")
    parser.add_argument("--out", type=str, default=str(OUTPUT_FILE),
                        help=f"Chemin du fichier de sortie (défaut: {OUTPUT_FILE}).")

    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
    start_default = datetime(year=now_utc.year, month=10, day=1, tzinfo=timezone.utc)

    start = parse_iso_z(args.start) if args.start else start_default
    end = parse_iso_z(args.end) if args.end else now_utc

    if end <= start:
        raise SystemExit("La date de fin doit être postérieure à la date de début.")

    trips = generate_trips(args.n, start, end)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"data": trips}

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=3)

    print(f"[OK] Fichier généré : {out_path}  (trajets: {len(trips)})")
    if trips:
        print(f"Plage temporelle: {trips[0]['agent_timestamp']} → {trips[-1]['agent_timestamp']}")
        # Aperçu succinct
        sample = trips[min(0, len(trips)-1)]
        print("Exemple de trajet:")
        print(json.dumps(sample, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()