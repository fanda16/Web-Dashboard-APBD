import requests
from bs4 import BeautifulSoup
import pandas as pd
from multiprocessing import Pool

# URL template for scraping (modify provinsi dynamically)
base_url = "https://djpk.kemenkeu.go.id/portal/data/apbd?periode=12&tahun=2024&provinsi={provinsi}&pemda=00"

# Province mapping (from the select dropdown in the HTML)
province_mapping = {
    "1": "Provinsi Aceh",
    "2": "Provinsi Sumatera Utara",
    "3": "Provinsi Sumatera Barat",
    "4": "Provinsi Riau",
    "5": "Provinsi Jambi",
    "6": "Provinsi Sumatera Selatan",
    "7": "Provinsi Bengkulu",
    "8": "Provinsi Lampung",
    "9": "Provinsi DKI Jakarta",
    "10": "Provinsi Jawa Barat",
    "11": "Provinsi Jawa Tengah",
    "12": "Provinsi DI Yogyakarta",
    "13": "Provinsi Jawa Timur",
    "14": "Provinsi Kalimantan Barat",
    "15": "Provinsi Kalimantan Tengah",
    "16": "Provinsi Kalimantan Selatan",
    "17": "Provinsi Kalimantan Timur",
    "18": "Provinsi Sulawesi Utara",
    "19": "Provinsi Sulawesi Tengah",
    "20": "Provinsi Sulawesi Selatan",
    "21": "Provinsi Sulawesi Tenggara",
    "22": "Provinsi Bali",
    "23": "Provinsi Nusa Tenggara Barat",
    "24": "Provinsi Nusa Tenggara Timur",
    "25": "Provinsi Maluku",
    "26": "Provinsi Papua",
    "27": "Provinsi Maluku Utara",
    "28": "Provinsi Banten",
    "29": "Provinsi Bangka Belitung",
    "30": "Provinsi Gorontalo",
    "31": "Provinsi Kepulauan Riau",
    "32": "Provinsi Papua Barat",
    "33": "Provinsi Sulawesi Barat",
    "34": "Provinsi Kalimantan Utara",
    "35": "Provinsi Papua Selatan",
    "36": "Provinsi Papua Tengah",
    "37": "Provinsi Papua Pegunungan",
    "38": "Provinsi Papua Barat Daya"
}

# Function to scrape a single province
def scrape_provinsi(provinsi):
    # Format the URL with the province number
    url = base_url.format(provinsi=f"{provinsi:02d}")
    
    # List to hold the data for this province
    province_data = []
    
    try:
        # Send HTTP request and get the page content
        response = requests.get(url)
        if response.status_code == 200:
            # Parse the page content with BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the <select> element for municipalities by id
            select_element = soup.find('select', id='sel_pemda')
            
            if select_element:
                # Find all <option> elements within the <select>
                options = select_element.find_all('option')
                
                # Initialize a counter for this province
                count = 1
                
                # Iterate over options and collect data
                for option in options:
                    name = option.get_text(strip=True)
                    value = option.get('value')
                    
                    # Filter out "Semua pemda" and "Provinsi Aceh"
                    if name and value != '--' and value != '00':
                        province_data.append([f"{provinsi:02d}", province_mapping.get(f"{provinsi:02d}", "Unknown Province"), name, count])
                        count += 1
    except Exception as e:
        print(f"Error scraping province {provinsi:02d}: {e}")
    
    return province_data

# Function to run the multiprocessing and combine results
def main():
    # List of province numbers (1 to 38)
    provinces = list(range(1, 39))

    # Use Pool to run the scraping concurrently
    with Pool(processes=8) as pool:  # You can adjust the number of processes as needed
        results = pool.map(scrape_provinsi, provinces)
    
    # Flatten the list of results (since each result is a list of rows for each province)
    all_data = [item for sublist in results for item in sublist]

    # Create a DataFrame from the collected data
    df = pd.DataFrame(all_data, columns=["Provinsi Number", "Province Name", "Municipality", "Count"])

    # Buat kolom Id sebagai gabungan Provinsi Number dan Count
    df["Id"] = df["Provinsi Number"].astype(int).astype(str) + "." + df["Count"].astype(str)

    # Rename Municipality menjadi Pemda
    df.rename(columns={"Municipality": "Pemda"}, inplace=True)

    # Ambil hanya kolom Id dan Pemda
    df = df[["Id", "Pemda"]]

    # Save the DataFrame to an Excel file
    df.to_excel(r"C:\Users\areta\Downloads\TA\TA1\Codingan\Scrapping\Kota_Kabupaten.xlsx", index=False)
    print("Data has been successfully saved to 'municipalities_with_province_name.xlsx'.")

if __name__ == "__main__":
    main()

