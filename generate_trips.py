import json
import random
import argparse
from datetime import datetime, timedelta
from math import radians, sin, cos, asin, sqrt
from pathlib import Path

# ---------------------------
# CONFIG DE BASE
# ---------------------------
DEFAULT_N_TRIPS = 4500
OUTPUT_FILE = Path("data/data_projet_vtest.json")

CONFORT_LEVELS = {
    "low": 1.5,
    "standard": 2.0,
    "high": 3.0,
}

CLIENT_NAMES = ["FALL", "NDIAYE", "DIOUF", "SOW", "BA", "GUEYE", "MUKENDI", "KANE", "NDAO", "SECK"]
DRIVER_NAMES = ["DIOP", "CAMARA", "NDAO", "SECK", "DIALLO", "MARTIN", "GUEYE", "LO", "BA", "NDIAYE"]

# ---------------------------
# HELPERS
# ---------------------------
def random_coord_near_nyc():
    """
    Retourne une coordonnée (lon, lat) proche de New York City.
    Zone : ~30 km autour de Manhattan.
    """
    # Manhattan center approx: lon=-73.9857, lat=40.7484
    lon = random.uniform(-74.10, -73.80)
    lat = random.uniform(40.60, 40.90)
    return lon, lat

def haversine(lon1, lat1, lon2, lat2):
    """Distance en km entre deux points géographiques."""
    R = 6371
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return R * 2 * asin(sqrt(a))

def random_timestamp_within_last_4_weeks():
    """Retourne un timestamp aléatoire entre début octobre et maintenant (UTC)."""
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(weeks=4)
    random_seconds = random.uniform(0, (end_date - start_date).total_seconds())
    random_time = start_date + timedelta(seconds=random_seconds)
    return random_time.isoformat(timespec="seconds") + "Z"

# ---------------------------
# GÉNÉRATION
# ---------------------------
def generate_trips(n_trips: int):
    trips = []
    for _ in range(n_trips):
        confort = random.choice(list(CONFORT_LEVELS.keys()))
        prix_base = CONFORT_LEVELS[confort]

        # Coordonnées client & chauffeur autour de NYC
        lon_c, lat_c = random_coord_near_nyc()
        lon_d, lat_d = random_coord_near_nyc()

        # Calcul distance & prix
        dist = round(haversine(lon_c, lat_c, lon_d, lat_d), 3)
        prix = round(prix_base * dist, 2)

        client = random.choice(CLIENT_NAMES)
        driver = random.choice(DRIVER_NAMES)

        trip = {
            "confort": confort,
            "prix_base_per_km": prix_base,
            "properties-client": {
                "longitude": lon_c,
                "latitude": lat_c,
                "nomclient": client,
                "telephoneClient": f"06{random.randint(10000000, 99999999)}",
                "location": f"{lon_c}, {lat_c}"
            },
            "properties-driver": {
                "longitude": lon_d,
                "latitude": lat_d,
                "nomDriver": driver,
                "telephoneDriver": f"07{random.randint(10000000, 99999999)}",
                "location": f"{lon_d}, {lat_d}"
            },
            "distance": dist,
            "prix_travel": prix,
            "agent_timestamp": random_timestamp_within_last_4_weeks()
        }

        trips.append(trip)
    return trips

# ---------------------------
# MAIN
# ---------------------------
def main():
    parser = argparse.ArgumentParser(description="Génère un fichier de trajets NYC pour les DAGs Airflow.")
    parser.add_argument("--n", type=int, default=DEFAULT_N_TRIPS, help="Nombre de trajets à générer (défaut=3000)")
    args = parser.parse_args()

    trips = generate_trips(args.n)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    output = {"data": trips}
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=3)

    print(f"[OK] Fichier généré : {OUTPUT_FILE} ({len(trips)} trajets)")
    print(f"Plage temporelle : {trips[0]['agent_timestamp']} → {trips[-1]['agent_timestamp']}")
    print(f"Exemple de trajet :\n{json.dumps(trips[0], indent=3)}")

if __name__ == "__main__":
    main()