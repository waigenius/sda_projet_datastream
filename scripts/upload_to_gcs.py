# -*- coding: utf-8 -*-
"""
Upload Parquet files to Google Cloud Storage
Uploads all parquet files from exports/ to GCS with partition structure
"""
from google.cloud import storage
import os
from pathlib import Path

# ============================================
# CONFIGURATION
# ============================================
PROJECT_ID = "taxi-streaming-project"
BUCKET_NAME = "datastream-rides-bucket"
SERVICE_ACCOUNT_KEY = "taxi-streaming-project-ca97054b822a.json"

# Définir les credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_KEY

# ============================================
# FUNCTIONS
# ============================================
def upload_parquet_files():
    """Upload tous les fichiers parquet vers GCS avec structure partitionnée"""
    
    # Vérifier que le fichier de clés existe
    if not os.path.exists(SERVICE_ACCOUNT_KEY):
        print(f"❌ Erreur: Fichier de clé '{SERVICE_ACCOUNT_KEY}' introuvable!")
        print(f"   Chemin actuel: {os.getcwd()}")
        print(f"\n💡 Solutions:")
        print(f"   1. Placer le fichier JSON dans le dossier actuel")
        print(f"   2. Ou modifier SERVICE_ACCOUNT_KEY avec le chemin complet")
        return
    
    # Initialiser le client GCS
    try:
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(BUCKET_NAME)
        print(f"✅ Connecté au bucket: gs://{BUCKET_NAME}/")
    except Exception as e:
        print(f"❌ Erreur de connexion GCS: {e}")
        return
    
    # Chercher tous les fichiers parquet
    exports_dir = Path("exports")
    
    if not exports_dir.exists():
        print(f"❌ Erreur: Le dossier 'exports/' n'existe pas!")
        print(f"   Chemin actuel: {os.getcwd()}")
        return
    
    parquet_files = list(exports_dir.rglob("*.parquet"))
    
    if not parquet_files:
        print(f"⚠️  Aucun fichier .parquet trouvé dans {exports_dir}/")
        return
    
    print(f"\n📦 {len(parquet_files)} fichiers parquet trouvés")
    print(f"🚀 Début de l'upload vers gs://{BUCKET_NAME}/\n")
    
    # Upload chaque fichier
    uploaded = 0
    failed = 0
    
    for local_file in parquet_files:
        # Conserver la structure: year=YYYY/month=MM/day=DD/hour=HH/file.parquet
        relative_path = local_file.relative_to(exports_dir)
        gcs_path = str(relative_path).replace("\\", "/")
        
        try:
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(str(local_file))
            print(f"✅ {gcs_path}")
            uploaded += 1
        except Exception as e:
            print(f"❌ Échec: {gcs_path} - {e}")
            failed += 1
    
    # Résumé
    print(f"\n{'='*60}")
    print(f"📊 Résumé de l'upload")
    print(f"{'='*60}")
    print(f"✅ Succès:  {uploaded}/{len(parquet_files)}")
    if failed > 0:
        print(f"❌ Échecs:  {failed}/{len(parquet_files)}")
    print(f"\n📍 URI BigQuery:")
    print(f"   gs://{BUCKET_NAME}/year=*/month=*/day=*/hour=*/*.parquet")
    print(f"{'='*60}\n")

# ============================================
# MAIN
# ============================================
if __name__ == "__main__":
    print("="*60)
    print("📤 Upload Parquet → Google Cloud Storage")
    print("="*60)
    print(f"Project:  {PROJECT_ID}")
    print(f"Bucket:   {BUCKET_NAME}")
    print(f"Key file: {SERVICE_ACCOUNT_KEY}")
    print("="*60 + "\n")
    
    upload_parquet_files()
