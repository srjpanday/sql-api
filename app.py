from flask import Flask, request, jsonify
import gspread
import mysql.connector
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

app = Flask(__name__)

# ---------------- Google Sheets Authentication ----------------
def authenticate_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
    return gspread.authorize(creds)

# ---------------- MySQL Database Connection ----------------
def fetch_sql_data():
    try:
        # Database connection details
        db_config = {
            "host": "103.184.242.95",  # MySQL Server IP
            "port": 3306,               
            "database": "aludecor",     
            "user": "alpl",             
            "password": "ALuDeC0r*20",      
        }

        # Connect to MySQL Database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Authenticate Google Sheets
        gc = authenticate_google_sheets()
        sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/14XqSrEv4dHqUmfWDKQKKAU16XNR-DcCGc2I-smvXDDw/edit")
        sheet = sh.worksheet("SQL data")

        # Get table name & date range from Google Sheets
        table_name = sheet.cell(2, 1).value  # A2 = Table Name
        start_date = sheet.cell(2, 2).value  # B2 = Start Date
        end_date = sheet.cell(2, 3).value    # C2 = End Date

        if not table_name or not start_date or not end_date:
            return "❌ Table name or date range is missing in Google Sheets!"

        # Convert date from DD/MM/YYYY to YYYY-MM-DD
        try:
            start_date = datetime.strptime(start_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return "❌ Date format error. Ensure the date is in DD/MM/YYYY format."

        # SQL query to fetch data within the given date range
        sql_query = f"""
            SELECT * FROM {table_name} 
            WHERE DateAndTime BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'
        """
        cursor.execute(sql_query)

        # Fetch results and column headers
        data = cursor.fetchall()
        column_headers = [i[0] for i in cursor.description]

        # ---------------- Writing Data to Google Sheet ----------------
        sheet.batch_clear(["A3:ZZ"])  # Clears only data from A3 onward
        sheet.update('A3', [column_headers])  # Insert headers in A3

        # Process data and convert datetime objects to strings
        processed_data = []
        for row in data:
            processed_row = [str(cell) if isinstance(cell, datetime) else cell for cell in row]
            processed_data.append(processed_row)

        # Insert data in batches
        batch_size = 500
        for i in range(0, len(processed_data), batch_size):
            sheet.update(f"A{4 + i}", processed_data[i:i + batch_size])

        # Close connections
        cursor.close()
        conn.close()

        return f"✅ Data Fetch Complete. {len(processed_data)} rows inserted."

    except Exception as e:
        return f"❌ Error: {e}"

@app.route('/fetch_data', methods=['GET'])
def fetch_data():
    result = fetch_sql_data()
    return jsonify({"message": result})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
