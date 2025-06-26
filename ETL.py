import pandas as pd

# Muat file Excel utama
df = pd.read_excel(r"C:\Users\areta\Downloads\TA\TA1\Codingan\Scrapping\Setiap_Pemda_Setiap_Provinsi_Multiprocessor_Combined.xlsx")

# Daftar kata kunci pada kolom 'Akun' yang ingin dihapus
keywords_to_remove = [
    "PAD",
    "TKDD",
    "Pendapatan Lainnya",
    "Belanja Barang Jasa",
    "Penerimaan Pembiayaan Daerah",
    "Pengeluaran Pembiayaan Daerah",
    "Pendapatan Daerah",
    "Belanja Daerah",
    "Pembiayaan Daerah"
]

# Hapus baris yang kolom 'Akun'-nya mengandung kata kunci
filtered_df = df[~df['akun'].str.strip().isin(keywords_to_remove)]

# Hapus duplikat, simpan yang terakhir
df_cleaned = filtered_df.drop_duplicates(subset=['Region', 'Year', 'Pemda_Number', 'akun'], keep='last')

# Baca file pengeluaran
df_pengeluaran = pd.read_excel(r"C:\Users\areta\Downloads\TA\TA1\Codingan\Scrapping\pengeluaran.xlsx")

# Gabungkan dengan data pengeluaran
df_merged = pd.merge(df_cleaned, df_pengeluaran, on="akun", how="left")

# Ubah urutan kolom agar 'Pengeluaran' dan 'Sub_Pengeluaran' setelah 'Pemda_Number'
cols = list(df_merged.columns)
insert_at = cols.index('Pemda_Number') + 1
for col in ['Pengeluaran', 'Sub_Pengeluaran']:
    cols.insert(insert_at, cols.pop(cols.index(col)))
    insert_at += 1
df_merged = df_merged[cols]

# Fungsi konversi format M ke Rupiah (Miliar)
def convert_to_number(value):
    if isinstance(value, str):
        return float(value.replace("M", "").replace(".", "").replace(",", ".").strip())
    return value

# Konversi nilai 'anggaran' dan 'realisasi' ke bentuk rupiah
df_merged['anggaran_rupiah'] = df_merged['anggaran'].apply(convert_to_number)
df_merged['realisasi_rupiah'] = df_merged['realisasi'].apply(convert_to_number)

# Ubah nama kolom 'akun' jadi 'Kat_Pengeluaran'
df_merged.rename(columns={'akun': 'Kat_Pengeluaran'}, inplace=True)

# Hapus kolom yang tidak diperlukan
df_final = df_merged.drop(columns=['anggaran', 'realisasi', 'persentase'])

# Mapping provinsi ke ID
provinsi_mapping = {
    "Aceh": 1, "Sumatera_Utara": 2, "Sumatera_Barat": 3, "Riau": 4,
    "Jambi": 5, "Sumatera_Selatan": 6, "Bengkulu": 7, "Lampung": 8,
    "DKI_Jakarta": 9, "Jawa_Barat": 10, "Jawa_Tengah": 11, "DI_Yogyakarta": 12,
    "Jawa_Timur": 13, "Kalimantan_Barat": 14, "Kalimantan_Tengah": 15, "Kalimantan_Selatan": 16,
    "Kalimantan_Timur": 17, "Sulawesi_Utara": 18, "Sulawesi_Tengah": 19, "Sulawesi_Selatan": 20,
    "Sulawesi_Tenggara": 21, "Bali": 22, "Nusa_Tenggara_Barat": 23, "Nusa_Tenggara_Timur": 24,
    "Maluku": 25, "Papua": 26, "Maluku_Utara": 27, "Banten": 28,
    "Bangka_Belitung": 29, "Gorontalo": 30, "Kepulauan_Riau": 31, "Papua_Barat": 32,
    "Sulawesi_Barat": 33, "Kalimantan_Utara": 34, "Papua_Selatan": 35, "Papua_Tengah": 36,
    "Papua_Pegunungan": 37, "Papua_Barat_Daya": 38
}

# Asumsikan kolom nama provinsi di Excel bernama "Provinsi"
# Tambahkan kolom baru bernama "id_provinsi" berdasarkan mapping
df_final['id_provinsi'] = df_final['Region'].map(provinsi_mapping)

# Buat kolom Id sebagai gabungan Provinsi Number dan Count
df_final["Id"] = df_final["id_provinsi"].astype(int).astype(str) + "." + df_final["Pemda_Number"].astype(str)

# Ambil hanya kolom Id dan Pemda
df_final = df_final[["Id", "Region", "Year", "Pengeluaran","Sub_Pengeluaran", "Kat_Pengeluaran", "anggaran_rupiah", "realisasi_rupiah"]]

# Simpan hasil ke file Excel
output_file_path = r"C:\Users\areta\Downloads\TA\TA1\Codingan\Scrapping\DATAAPBDSEMPURNA.xlsx"
df_final.to_excel(output_file_path, index=False)

print(f"The cleaned and merged Excel file is ready! Saved at:\n{output_file_path}")
