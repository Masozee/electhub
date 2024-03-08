import mysql.connector
import requests
import os
import json
import sqlite3
import random
import time
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

HEADER_VARIANTS = [
    {"id": 1, "headers":
        {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://pemilu2024.kpu.go.id/",
            "Sec-Ch-Ua": "\" Not A;Brand\";v=\"99\", \"Chrome\";v=\"91\", \"Chromium\";v=\"91\"",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "\"Windows\"",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
    },
    {"id": 2, "headers":
        {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://pemilu2024.kpu.go.id/",
            "Sec-Ch-Ua": "\"Chromium\";v=\"90\", \"Google Chrome\";v=\"90\", \" Not A;Brand\";v=\"99\"",
            "Sec-Ch-Ua-Mobile": "?1",
            "Sec-Ch-Ua-Platform": "\"Android\"",
            "User-Agent": "Mozilla/5.0 (Linux; Android 11) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.210 Mobile Safari/537.36"
        }
    },
    {"id": 2, "headers":
        {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://pemilu2024.kpu.go.id/",
            "Sec-Ch-Ua": "\" Not A;Brand\";v=\"99\", \"Microsoft Edge\";v=\"91\", \"Chromium\";v=\"91\"",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "\"Windows\"",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.864.48 Safari/537.36 Edg/91.0.864.48"
        }
    },
    {"id": 3, "headers":
        {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://pemilu2024.kpu.go.id/",
            "Sec-Ch-Ua": "\"Mozilla Firefox\";v=\"89\", \" Not A;Brand\";v=\"99\"",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "\"Linux\"",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0"
        }
    },
    {"id": 4, "headers":
        {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://pemilu2024.kpu.go.id/",
            "Sec-Ch-Ua": "\"AppleWebKit\";v=\"605\", \"Safari\";v=\"605\", \" Not A;Brand\";v=\"99\"",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "\"macOS\"",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Safari/605.1.15"
        }
    },
    {"id": 5, "headers":
        {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://pemilu2024.kpu.go.id/",
            "Sec-Ch-Ua": "\" Not A;Brand\";v=\"99\", \"Chrome\";v=\"93\", \"Chromium\";v=\"93\"",
            "Sec-Ch-Ua-Mobile": "?1",
            "Sec-Ch-Ua-Platform": "\"Android\"",
            "User-Agent": "Mozilla/5.0 (Linux; arm_64; Android 11) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Mobile Safari/537.36"
        }
    },
    {"id": 6, "headers":
        {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://pemilu2024.kpu.go.id/",
            "Sec-Ch-Ua": "\"Chromium\";v=\"92\", \" Not A;Brand\";v=\"99\", \"Google Chrome\";v=\"92\"",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "\"Windows\"",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36"
        }
    },
    {"id": 7, "headers":
        {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://pemilu2024.kpu.go.id/",
            "Sec-Ch-Ua": "\" Not A;Brand\";v=\"99\", \"Opera\";v=\"76\", \"Chromium\";v=\"90\"",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "\"Linux\"",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36 OPR/76.0.4017.177"
        }
    },
    {"id": 8, "headers":
        {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://pemilu2024.kpu.go.id/",
            "Sec-Ch-Ua": "\" Not A;Brand\";v=\"99\", \"Firefox\";v=\"88\", \"Mozilla Firefox\";v=\"88\"",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "\"macOS\"",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Gecko/20100101 Firefox/88.0"
        }
    },
    {"id": 9, "headers":
        {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://pemilu2024.kpu.go.id/",
            "Sec-Ch-Ua": "\" Not A;Brand\";v=\"99\", \"Safari\";v=\"605\", \"AppleWebKit\";v=\"605\"",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "\"iOS\"",
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Mobile/15E148 Safari/604.1"
        }
    }
]

def connect_to_sqlite_db(db_path='data_pemilu_2024_capres.sqlite'):
    """Connects to the SQLite database."""
    return sqlite3.connect(db_path)

# Fetches region codes and names from the SQLite database
def fetch_kode_nama(db_name='data_pemilu_2024_capres.sqlite'):
    conn = connect_to_sqlite_db(db_name)
    c = conn.cursor()
    c.execute("SELECT kode, nama FROM regions WHERE tingkat = 5")
    results = c.fetchall()
    conn.close()
    return results

# Connects to the MySQL database
def connect_to_database():
    user_name = os.getenv('MYSQL_USER')
    user_password = os.getenv('MYSQL_PASSWORD')
    host_name = os.getenv('MYSQL_HOST')  # Changed from "csis-election.mysql.database.azure.com"
    db_name = os.getenv('MYSQL_DB')
    ssl_ca = os.getenv('SSL_CA')

    return mysql.connector.connect(
        user=user_name,
        password=user_password,
        host=host_name,
        database=db_name,
        ssl_ca=ssl_ca
    )

# Sets up the MySQL database schema
def setup_database():
    conn = connect_to_database()
    c = conn.cursor()
    # SQL queries to create tables
    c.execute('''
        CREATE TABLE IF NOT EXISTS election_data_pdpr (
            id INT AUTO_INCREMENT PRIMARY KEY,
            kode VARCHAR(255) UNIQUE,
            nama TEXT,
            images TEXT,
            suara_sah INT,
            suara_tidak_sah INT,
            ts TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS party_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            election_id INT,
            party_id INT,
            jml_suara_total INT,
            jml_suara_partai INT,
            FOREIGN KEY(election_id) REFERENCES election_data_pdpr(id)
        )
    ''')
    conn.commit()
    conn.close()

# Inserts fetched election and party data into the MySQL database
def insert_election_and_party_data(kode, nama, data):
    conn = connect_to_database()
    c = conn.cursor()

    c.execute("SELECT id FROM election_data_pdpr WHERE kode = %s", (kode,))
    if c.fetchone():
        print(f"Entry for {kode} already exists, skipping...")
        conn.close()
        return

    # Processing and inserting data
    suara_sah = data.get('administrasi', {}).get('suara_sah', 0)
    suara_tidak_sah = data.get('administrasi', {}).get('suara_tidak_sah', 0)

    

    c.execute('''INSERT INTO election_data_pdpr (kode, nama, images, suara_sah, suara_tidak_sah, ts)
                 VALUES (%s, %s, %s, %s, %s, %s)''',
              (kode, nama, json.dumps(data.get('images', [])), suara_sah, suara_tidak_sah, data.get('ts', '')))
    election_id = c.lastrowid

    chart_data = data.get("chart", {})
    if isinstance(chart_data, str):
        try:
            chart_data = json.loads(chart_data)  # Parse the string into a dictionary
        except json.JSONDecodeError:
            print("Error decoding JSON from chart data")
            chart_data = {}  # Default to an empty dictionary if parsing fails

    for party_id, party_info in chart_data.items():
        c.execute('''INSERT INTO party_data (election_id, party_id, jml_suara_total, jml_suara_partai)
                     VALUES (%s, %s, %s, %s)''',
                  (election_id, party_id, party_info.get("jml_suara_total", 0), party_info.get("jml_suara_partai", 0)))

    conn.commit()
    conn.close()

# Retrieves the latest processed region code from the MySQL database
def get_latest_processed_kode():
    conn = connect_to_database()
    c = conn.cursor()
    c.execute("SELECT kode FROM election_data_pdpr ORDER BY id DESC LIMIT 1")
    result = c.fetchone()
    conn.close()
    return result[0] if result else None

# Randomly selects a set of HTTP headers for API requests
def get_random_headers():
    choice = random.choice(HEADER_VARIANTS)  # Select a random header variant
    headers = choice["headers"]  # Get the headers dictionary from the chosen variant
    return headers

if __name__ == "__main__":
    setup_database()

    kode_nama_pairs = fetch_kode_nama()
    latest_kode = get_latest_processed_kode()

    if latest_kode:
        kode_nama_pairs = [pair for pair in kode_nama_pairs if pair[0] > latest_kode]

    for kode, nama in tqdm(kode_nama_pairs, desc="Processing data"):
        headers = get_random_headers()
        json_url = f"https://sirekap-obj-data.kpu.go.id/pemilu/hhcw/pdpr/{kode[:2]}/{kode[:4]}/{kode[:6]}/{kode[:10]}/{kode}.json"

        try:
            response = requests.get(json_url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                insert_election_and_party_data(kode, nama, data)
            else:
                print(f"Failed to fetch data for {kode}: HTTP {response.status_code}")
        except Exception as e:
            print(f"An error occurred while fetching data for {kode}: {e}")
        
        time.sleep(random.uniform(1, 3))