from flask import Flask
import sqlite3

app = Flask(__name__)

@app.route("/")
def home():
    conn = sqlite3.connect("data/properties.db")
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM properties")
    count = cursor.fetchone()[0]

    conn.close()

    return f"Database connected. {count} properties loaded."

if __name__ == "__main__":
    app.run(debug=True)
