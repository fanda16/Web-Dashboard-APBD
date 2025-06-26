import pandas as pd
import os
import multiprocessing
from multiprocessing import Pool

# Define the base folder path containing the CSV files
base_folder_path = r"C:\Users\areta\Downloads\TA\TA1\Codingan\Scrapping\kelompokdesemberpertahun"

# List of provinces
provinces = [
    'Aceh', 'Bali', 'Bangka_Belitung', 'Banten', 'Bengkulu', 'DI_Yogyakarta', 'DKI_Jakarta', 
    'Gorontalo', 'Jambi', 'Jawa_Barat', 'Jawa_Tengah', 'Jawa_Timur', 'Kalimantan_Barat', 
    'Kalimantan_Selatan', 'Kalimantan_Tengah', 'Kalimantan_Timur', 'Kalimantan_Utara', 'Lampung', 
    'Nusa_Tenggara_Barat', 'Nusa_Tenggara_Timur', 'Sulawesi_Barat', 'Sulawesi_Selatan', 
    'Sulawesi_Tengah', 'Sulawesi_Tenggara', 'Sulawesi_Utara', 'Sumatera_Barat', 'Sumatera_Selatan', 
    'Sumatera_Utara', 'Maluku','Papua', 'Riau', 'Papua_Barat', 'Papua_Barat_Daya', 'Papua_Pegunungan', 
    'Papua_Selatan', 'Papua_Tengah', 'Kepulauan_Riau', 'Maluku_Utara'
]

# Function to get list of years (subfolders) automatically from the province folder
def get_years_for_province(province_folder_path):
    try:
        return [int(folder) for folder in os.listdir(province_folder_path) if os.path.isdir(os.path.join(province_folder_path, folder)) and folder.isdigit()]
    except Exception as e:
        print(f"Error reading years from {province_folder_path}: {e}")
        return []

# Function to process each file
def process_file(file_path, province, year, pemda_number):
    try:
        df = pd.read_csv(file_path, on_bad_lines='skip')
        df.insert(0, 'Region', province)
        df.insert(1, 'Year', year)
        df.insert(2, 'Pemda_Number', pemda_number)
        return df
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None

# Main processing function
def process_all_files():
    tasks = []

    for province in provinces:
        province_folder = os.path.join(base_folder_path, province)
        if not os.path.isdir(province_folder):
            continue

        # Get years dynamically for this province
        years = get_years_for_province(province_folder)

        for year in years:
            for pemda_number in range(1, 40):
                file_path = os.path.join(province_folder, str(year), f'{province}_Pemda_{pemda_number}_{year}_12.csv')
                if os.path.exists(file_path):
                    tasks.append((file_path, province, year, pemda_number))

    # Parallel processing
    with Pool(processes=multiprocessing.cpu_count()) as pool:
        results = pool.starmap(process_file, tasks)

    results = [r for r in results if r is not None]

    if results:
        final_df = pd.concat(results, ignore_index=True)
        output_path = r"C:\Users\areta\Downloads\TA\TA1\Codingan\Scrapping\Setiap_Pemda_Setiap_Provinsi_Multiprocessor_Combined.xlsx"
        final_df.to_excel(output_path, index=False)
        print(f"The combined Excel file is ready! Saved at {output_path}")
    else:
        print("No valid data to save.")

if __name__ == '__main__':
    process_all_files()
