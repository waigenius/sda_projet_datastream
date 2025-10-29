import json
import random
import argparse
from datetime import datetime
from math import radians, sin, cos, asin, sqrt
from pathlib import Path

# ---------------------------
# CONFIG DE BASE
# ---------------------------
DEFAULT_N_TRIPS = 3000
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
def random_coord():
    """Retourne une coordonnée (lon, lat) aléatoire autour d'une zone réaliste (New York)."""
    lon = random.uniform(-74.1, -73.9)
    lat = random.uniform(40.65, 40.85)
    return lon, lat

def haversine(lon1, lat1, lon2, lat2):
    """Distance en km entre deux points géographiques."""
    R = 6371
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return R * 2 * asin(sqrt(a))

# ---------------------------
# GÉNÉRATION
# ---------------------------
def generate_trips(n_trips: int):
    trips = []
    for _ in range(n_trips):
        confort = random.choice(list(CONFORT_LEVELS.keys()))
        prix_base = CONFORT_LEVELS[confort]

        # Coordonnées client & chauffeur
        lon_c, lat_c = random_coord()
        lon_d, lat_d = random_coord()

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
                "location": f"{lon_c}, {lat_c}"  # pour compatibilité DAGs
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
            "agent_timestamp": datetime.utcnow().isoformat() + "Z"
        }

        trips.append(trip)
    return trips

# ---------------------------
# MAIN
# ---------------------------
def main():
    parser = argparse.ArgumentParser(description="Génère un fichier de trajets compatibles avec DAGs Airflow.")
    parser.add_argument("--n", type=int, default=DEFAULT_N_TRIPS, help="Nombre de trajets à générer (défaut=3000)")
    args = parser.parse_args()

    trips = generate_trips(args.n)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    output = {"data": trips}
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=3)

    print(f"[OK] Fichier généré : {OUTPUT_FILE} ({len(trips)} trajets)")
    print(f"Exemple : {trips[0] if trips else 'aucun'}")

if __name__ == "__main__":
    main()