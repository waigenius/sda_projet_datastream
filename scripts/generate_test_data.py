import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# test data
data = {
    'nomclient': ['FALL'] * 5,
    'telephoneClient': ['060786575'] * 5,
    'locationClient': ['2.3522, 48.8566'] * 5,
    'distance': [944.494, 500.2, 1200.5, 300.1, 800.0],
    'confort': ['standard', 'High', 'Medium', 'standard', 'High'],
    'prix_travel': [1888.99, 2500.0, 3000.0, 600.0, 4000.0],
    'nomDriver': ['DIOP'] * 5,
    'locationDriver': ['3.7038, 40.4168'] * 5,
    'telephoneDriver': ['070786575'] * 5,
    'agent_timestamp': ['2024-08-02T16:09:47Z'] * 5
}

print("Creating DataFrame...")
df = pd.DataFrame(data)
filename = '2024-10-24T10-00-00Z.parquet'
print(f"Writing to {filename}...")
pq.write_table(pa.Table.from_pandas(df), filename)
print(f"✓ {filename} created successfully!")
