import subprocess

# Daftar script yang ingin dijalankan secara berurutan
scripts = [
    "apbdscrap_desember_2011_2022.py",
    "pengelompokandesemberpertahun.py",
    "pemdaonly_join_all_pemda_multiprocessor.py",
    "ETL.py",
    "connect_postgress.py"
]

def run_scripts(script_list):
    for script in script_list:
        print(f"\n==================== Menjalankan {script} ====================")
        try:
            result = subprocess.run(["python", script], check=True, capture_output=True, text=True)
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"❌ Terjadi kesalahan saat menjalankan {script}:")
            print(e.stderr)
            break

if __name__ == "__main__":
    run_scripts(scripts)
