import os
import shutil

# Direktori asal tempat file disimpan
source_dir = r"C:\Users\areta\Downloads\TA\TA1\Codingan\Scrapping\hasilscrap"

# Direktori tujuan tempat file akan dikelompokkan berdasarkan bulan Desember
destination_dir = r"C:\Users\areta\Downloads\TA\TA1\Codingan\Scrapping\kelompokdesemberpertahun"
os.makedirs(destination_dir, exist_ok=True)

# Daftar nama provinsi
provinsi_list = [
    "Aceh", "Sumatera_Utara", "Sumatera_Barat", "Riau", "Jambi", "Sumatera_Selatan",
    "Bengkulu", "Lampung", "DKI_Jakarta", "Jawa_Barat", "Jawa_Tengah", "DI_Yogyakarta",
    "Jawa_Timur", "Kalimantan_Barat", "Kalimantan_Tengah", "Kalimantan_Selatan",
    "Kalimantan_Timur", "Sulawesi_Utara", "Sulawesi_Tengah", "Sulawesi_Selatan",
    "Sulawesi_Tenggara", "Bali", "Nusa_Tenggara_Barat", "Nusa_Tenggara_Timur",
    "Maluku", "Papua", "Maluku_Utara", "Banten", "Bangka_Belitung", "Gorontalo",
    "Kepulauan_Riau", "Papua_Barat", "Sulawesi_Barat", "Kalimantan_Utara",
    "Papua_Selatan", "Papua_Tengah", "Papua_Pegunungan", "Papua_Barat_Daya"
]

# Fungsi untuk memindahkan file bulan Desember
def move_desember_file(file_name):
    if file_name.endswith(".csv") and "_12.csv" in file_name:  # Hanya file bulan Desember
        # Identifikasi nama provinsi dan tahun dari file
        for provinsi in provinsi_list:
            if provinsi in file_name:  # Cocokkan nama provinsi dalam nama file
                # Ambil tahun dari nama file
                try:
                    tahun = file_name.split("_")[-2]  # Ambil elemen kedua terakhir sebagai tahun
                    if not tahun.isdigit():
                        raise ValueError("Tahun tidak valid.")
                except IndexError:
                    print(f"Gagal membaca tahun dari file: {file_name}")
                    return False
                
                # Tentukan direktori tujuan
                source_file = os.path.join(source_dir, file_name)
                destination_folder = os.path.join(destination_dir, provinsi, tahun)
                os.makedirs(destination_folder, exist_ok=True)  # Buat folder jika belum ada
                destination_file = os.path.join(destination_folder, file_name)
                
                # Pindahkan file
                shutil.move(source_file, destination_file)
                print(f"Moved: {file_name} -> {destination_folder}")
                return True
    return False

# Iterasi melalui semua file di source_dir
files_moved = 0
for file_name in os.listdir(source_dir):
    if move_desember_file(file_name):
        files_moved += 1

print(f"Total files moved for December: {files_moved}")
