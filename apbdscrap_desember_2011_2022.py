import requests
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Daftar provinsi
provinsi_data = [
    {"id": 1, "nama": "Aceh", "total_pemda": 23},
    {"id": 2, "nama": "Sumatera Utara", "total_pemda": 33},
    {"id": 3, "nama": "Sumatera Barat", "total_pemda": 19},
    {"id": 4, "nama": "Riau", "total_pemda": 12},
    {"id": 5, "nama": "Jambi", "total_pemda": 11},
    {"id": 6, "nama": "Sumatera Selatan", "total_pemda": 17},
    {"id": 7, "nama": "Bengkulu", "total_pemda": 10},
    {"id": 8, "nama": "Lampung", "total_pemda": 15},
    {"id": 9, "nama": "DKI Jakarta", "total_pemda": 6},
    {"id": 10, "nama": "Jawa Barat", "total_pemda": 27},
    {"id": 11, "nama": "Jawa Tengah", "total_pemda": 35},
    {"id": 12, "nama": "DI Yogyakarta", "total_pemda": 5},
    {"id": 13, "nama": "Jawa Timur", "total_pemda": 38},
    {"id": 14, "nama": "Kalimantan Barat", "total_pemda": 14},
    {"id": 15, "nama": "Kalimantan Tengah", "total_pemda": 14},
    {"id": 16, "nama": "Kalimantan Selatan", "total_pemda": 13},
    {"id": 17, "nama": "Kalimantan Timur", "total_pemda": 10},
    {"id": 18, "nama": "Sulawesi Utara", "total_pemda": 15},
    {"id": 19, "nama": "Sulawesi Tengah", "total_pemda": 13},
    {"id": 20, "nama": "Sulawesi Selatan", "total_pemda": 24},
    {"id": 21, "nama": "Sulawesi Tenggara", "total_pemda": 17},
    {"id": 22, "nama": "Bali", "total_pemda": 9},
    {"id": 23, "nama": "Nusa Tenggara Barat", "total_pemda": 10},
    {"id": 24, "nama": "Nusa Tenggara Timur", "total_pemda": 22},
    {"id": 25, "nama": "Maluku", "total_pemda": 11},
    {"id": 26, "nama": "Papua", "total_pemda": 9},
    {"id": 27, "nama": "Maluku Utara", "total_pemda": 10},
    {"id": 28, "nama": "Banten", "total_pemda": 8},
    {"id": 29, "nama": "Bangka Belitung", "total_pemda": 7},
    {"id": 30, "nama": "Gorontalo", "total_pemda": 6},
    {"id": 31, "nama": "Kepulauan Riau", "total_pemda": 7},
    {"id": 32, "nama": "Papua Barat", "total_pemda": 7},
    {"id": 33, "nama": "Sulawesi Barat", "total_pemda": 6},
    {"id": 34, "nama": "Kalimantan Utara", "total_pemda": 5},
    {"id": 35, "nama": "Papua Selatan", "total_pemda": 4},
    {"id": 36, "nama": "Papua Tengah", "total_pemda": 8},
    {"id": 37, "nama": "Papua Pegunungan", "total_pemda": 8},
    {"id": 38, "nama": "Papua Barat Daya", "total_pemda": 6}
]

# Direktori untuk menyimpan file
output_dir = r"C:\Users\areta\Downloads\TA\TA1\Codingan\Scrapping\hasilscrap"
os.makedirs(output_dir, exist_ok=True)

# Tahun mulai dan tahun saat ini
tahun_awal = 2019
tahun_akhir = datetime.now().year  # Ambil tahun saat ini secara otomatis

# Buat range tahun dari 2019 hingga tahun saat ini (termasuk tahun akhir)
tahun_range = range(tahun_awal, tahun_akhir + 1)

# Bulan Desember saja
bulan = 12

# Fungsi untuk mendownload file
def download_file(prov_id, prov_name, pemda_code, pemda_name, tahun, bulan):
    url = f"https://djpk.kemenkeu.go.id/portal/csv_apbd?type=apbd&periode={bulan}&tahun={tahun}&provinsi={prov_id}&pemda={pemda_code}"
    filename = f"{output_dir}/{prov_name}_{pemda_name}_{tahun}_{bulan}.csv"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        with open(filename, 'wb') as f:
            f.write(response.content)
        return f"Downloaded: {filename}"
    except Exception as e:
        return f"Failed: {filename}, Error: {e}"

# Menyiapkan tugas unduhan
tasks = []

for prov in provinsi_data:
    prov_id = f"{prov['id']:02d}"  # ID provinsi dengan format 2 digit
    prov_name = prov["nama"].replace(" ", "_")
    total_pemda = prov["total_pemda"]
    
    for tahun in tahun_range:
        for pemda in range(-1, total_pemda + 1):
            if pemda == -1:
                pemda_code = "--"
                pemda_name = "Provinsi"
            elif pemda == 0:
                pemda_code = "00"
                pemda_name = "Semua_Pemda"
            else:
                pemda_code = f"{pemda:02d}"
                pemda_name = f"Pemda_{pemda}"
            
            # Tambahkan tugas unduhan ke daftar
            tasks.append((prov_id, prov_name, pemda_code, pemda_name, tahun, bulan))

# Multithreading dengan ThreadPoolExecutor
start_time = time.time()
max_workers = 14  # Gunakan sesuai jumlah logical processors Anda
results = []

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_task = {executor.submit(download_file, *task): task for task in tasks}
    for future in as_completed(future_to_task):
        try:
            result = future.result()
            results.append(result)
            print(result)
        except Exception as e:
            print(f"Error in task: {e}")

end_time = time.time()
print(f"Completed all downloads in {end_time - start_time:.2f} seconds.")
