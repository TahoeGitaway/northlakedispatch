import pandas as pd
import sqlite3

CSV_FILE = "data/properties_geocoded.csv"
DB_FILE = "data/properties.db"

def main():
    df = pd.read_csv(CSV_FILE)

    # Connect to database (creates it if it doesn't exist)
    conn = sqlite3.connect(DB_FILE)

    # Write dataframe to table
    df.to_sql("properties", conn, if_exists="replace", index=False)

    conn.close()

    print("Database created at:", DB_FILE)

if __name__ == "__main__":
    main()
