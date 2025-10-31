#!/usr/bin/env python3
"""
Standalone Transform & GCS Upload (Hive Partitioning)
PowerShell에서 바로 실행 가능!
"""
from pathlib import Path
import json
from typing import Any, Dict, List
from datetime import datetime
import pandas as pd
import os

# === CONFIG ===
SERVICE_ACCOUNT_KEY = "taxi-streaming-project-ca97054b822a.json"
PROJECT_ID = "taxi-streaming-project"
GCS_BUCKET = "datastream-rides-bucket"
EXPORT_DIR = Path("exports")
PREVIEW_DIR = Path("exports/preview")

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_KEY

# === HELPERS ===
def flatten(d: Dict[str, Any], prefix: str = "", sep: str = "_") -> Dict[str, Any]:
    """Aplatit un dictionnaire imbriqué"""
    out: Dict[str, Any] = {}
    for k, v in d.items():
        key = f"{prefix}{sep}{k}" if prefix else k
        if isinstance(v, dict):
            out.update(flatten(v, key, sep))
        else:
            out[key] = v
    return out

# === STEP 1: Load Data ===
def load_data() -> List[Dict[str, Any]]:
    """Charge tous les JSON du dossier preview/"""
    if not PREVIEW_DIR.exists():
        print(f"❌ {PREVIEW_DIR} n'existe pas!")
        return []
    
    json_files = list(PREVIEW_DIR.glob("*.json"))
    if not json_files:
        print(f"❌ Pas de fichiers JSON dans {PREVIEW_DIR}!")
        return []
    
    print(f"📂 {len(json_files)} fichiers trouvés dans {PREVIEW_DIR}")
    
    all_docs = []
    for json_file in json_files:
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                raw = json.load(f)
            
            if isinstance(raw, dict) and "data" in raw:
                all_docs.extend(raw["data"])
            elif isinstance(raw, list):
                all_docs.extend(raw)
            else:
                all_docs.append(raw)
                
            print(f"  ✅ {json_file.name}: {len(raw.get('data', [raw]))} docs")
        except Exception as e:
            print(f"  ❌ {json_file.name}: {e}")
    
    print(f"\n📊 Total: {len(all_docs)} documents\n")
    return all_docs

# === STEP 2: Transform ===
def transform_data(raw_docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transforme les données (flatten + timestamp)"""
    if not raw_docs:
        return []
    
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    transformed = []
    
    for d in raw_docs:
        try:
            d = dict(d)
            d["agent_timestamp"] = ts
            flat = flatten(d)
            transformed.append(flat)
        except Exception as e:
            print(f"⚠️ Erreur transform: {e}")
    
    print(f"🔄 Transformé: {len(transformed)} documents\n")
    return transformed

# === STEP 3: Export Parquet (Hive) ===
def export_parquet_hive(docs: List[Dict[str, Any]]) -> Path:
    """Exporte en Parquet avec Hive partitioning"""
    if not docs:
        raise ValueError("Aucun document à exporter")
    
    now = datetime.utcnow()
    
    # Hive structure
    part_dir = EXPORT_DIR / f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
    part_dir.mkdir(parents=True, exist_ok=True)
    
    file_path = part_dir / f"part-{int(now.timestamp())}.parquet"
    
    df = pd.DataFrame(docs)
    df.to_parquet(file_path, index=False)
    
    print(f"💾 Parquet créé: {file_path}")
    print(f"   Lignes: {len(df)}\n")
    return file_path

# === STEP 4: Upload GCS ===
def upload_gcs(local_file: Path):
    """Upload vers GCS avec Hive structure"""
    try:
        from google.cloud import storage
        
        if not os.path.exists(SERVICE_ACCOUNT_KEY):
            print(f"❌ Clé Service Account introuvable: {SERVICE_ACCOUNT_KEY}")
            return
        
        print("☁️  Upload vers GCS...")
        
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(GCS_BUCKET)
        
        # Extraire le chemin Hive
        parts = local_file.parts
        export_idx = parts.index("exports") + 1
        gcs_path = "/".join(parts[export_idx:])
        
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(str(local_file))
        
        print(f"✅ Upload réussi!")
        print(f"   GCS: gs://{GCS_BUCKET}/{gcs_path}\n")
        
    except ImportError:
        print("⚠️  google-cloud-storage non installé")
        print("   Installer: pip install google-cloud-storage")
    except Exception as e:
        print(f"❌ Erreur upload GCS: {e}")

# === MAIN ===
def main():
    print("=" * 70)
    print("🚀 Transform JSON → Parquet (Hive) → GCS")
    print("=" * 70)
    print()
    
    # 1. Load
    print("[1/4] Chargement des données...")
    docs = load_data()
    
    if not docs:
        print("❌ Aucune donnée à traiter!")
        return
    
    # 2. Transform
    print("[2/4] Transformation...")
    transformed = transform_data(docs)
    
    if not transformed:
        print("❌ Transformation échouée!")
        return
    
    # 3. Export Parquet
    print("[3/4] Export Parquet (Hive partitioning)...")
    parquet_file = export_parquet_hive(transformed)
    
    # 4. Upload GCS
    print("[4/4] Upload vers GCS...")
    upload_gcs(parquet_file)
    
    print("=" * 70)
    print("✅ Terminé!")
    print("=" * 70)
    print()
    print("📊 Résumé:")
    print(f"  - Documents traités: {len(transformed)}")
    print(f"  - Fichier local: {parquet_file}")
    print(f"  - GCS bucket: gs://{GCS_BUCKET}/year=*/month=*/day=*/hour=*/")
    print()
    print("🔄 Prochaine étape:")
    print("  → Vérifier dans GCS Console")
    print("  → Rafraîchir BigQuery External Table")

if __name__ == "__main__":
    main()
