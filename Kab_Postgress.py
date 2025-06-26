import pandas as pd
from sqlalchemy import create_engine

# --- Konfigurasi database PostgreSQL ---
db_user = 'postgres'       # ganti dengan username PostgreSQL
db_password = 'farhan'   # ganti dengan password PostgreSQL
db_host = 'localhost'               # atau IP server PostgreSQL
db_port = '5432'                    # default port PostgreSQL
db_name = 'SKRIpsi'           # ganti dengan nama database kamu
table_name = 'kota_kabupaten'           # ganti dengan nama tabel tujuan

           # pastikan file ini ada di direktori project

# --- Baca data dari Excel ---
df = pd.read_excel(r"C:\Users\areta\Downloads\TA\TA1\Codingan\Scrapping\Kota_Kabupaten.xlsx")

# --- Koneksi ke PostgreSQL ---
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# --- Impor ke PostgreSQL ---
df.to_sql(table_name, engine, if_exists='replace', index=False)

print(f'Data berhasil diimpor ke tabel "{table_name}" di database "{db_name}"')