import requests
import csv
import mysql.connector

github_csv_url = 'https://github.com/birasraluca/pi/blob/main/CSV%20Files/BU.csv'

mysql_host = 'root'
mysql_user = 'root'
mysql_password = 'root'
mysql_database = 'pi'
mysql_table_name = 'BU'

def download_csv_from_github(github_csv_url):
    response = requests.get(github_csv_url)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Failed to download CSV file. Status Code: {response.status_code}")

def create_mysql_database(csv_data, mysql_table_name):
    conn = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor = conn.cursor()

    cursor.execute(f'''CREATE TABLE IF NOT EXISTS {mysql_table_name} (
                        BU VARCHAR(255),
                        Division VARCHAR(255),
                        BU VARCHAR(255),
                        Division VARCHAR(255)
                    )''')

    csv_reader = csv.reader(csv_data.splitlines())
    next(csv_reader)
    for row in csv_reader:
        placeholders = ', '.join(['%s'] * len(row))
        cursor.execute(f"INSERT INTO {mysql_table_name} VALUES ({placeholders})", tuple(row))

    conn.commit()
    conn.close()

github_csv_data = download_csv_from_github(github_csv_url)

create_mysql_database(github_csv_data, mysql_table_name)
