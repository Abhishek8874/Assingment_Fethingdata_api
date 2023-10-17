# Assingment_Fethingdata_api
import requests
import mysql.connector
import json

# Retrieve and Parse Data from the API
api_url = "https://api.bigw.com.au/api/stores/v0/list"
response = requests.get(api_url)

if response.status_code == 200:
    store_data = json.loads(response.text)
    print("store_data", store_data)
else:
    print("Failed to retrieve data from the API")
    exit()

#  Create MySQL Database and Tables
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "MySql@108",
    "database": "store2"
}

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Create the database if it doesn't exist
cursor.execute("CREATE DATABASE IF NOT EXISTS reomnify")
cursor.execute("USE reomnify")

# Create the store_info table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS store_info (
        store_id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255),
        latitude DECIMAL(10, 6),
        longitude DECIMAL(10, 6),
        postcode VARCHAR(10),
        suburb VARCHAR(255),
        street VARCHAR(255),
        state VARCHAR(255)
    )
""")

# Create the store_details table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS store_details (
        store_id INT,
        day_of_week VARCHAR(20),
        open_time VARCHAR(20),
        close_time VARCHAR(20),
        PRIMARY KEY (store_id, day_of_week),
        FOREIGN KEY (store_id) REFERENCES store_info(store_id)
    )
""")

# Step 3: Insert Data into MySQL Tables
for store in store_data:
    store_info = (
        store["name"],
        store["location"]["latitude"],
        store["location"]["longitude"],
        store["address"]["postcode"],
        store["address"]["suburb"],
        store["address"]["street"],
        store["address"]["state"]
    )
    cursor.execute("""
        INSERT INTO store_info (name, latitude, longitude, postcode, suburb, street, state)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, store_info)
    store_id = cursor.lastrowid

    for day, hours in store["openHours"].items():
        open_hours = (store_id, day, hours.get("openTime"), hours.get("closeTime"))
        cursor.execute("""
            INSERT INTO store_details (store_id, day_of_week, open_time, close_time)
            VALUES (%s, %s, %s, %s)
        """, open_hours)

# Commit changes and close the connection
conn.commit()
conn.close()
print("completed")
