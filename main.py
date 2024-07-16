# Import necessary modules from Flask and SQLite3
from flask import Flask, jsonify, request
import sqlite3
from flask_cors import CORS

# Create an instance of the Flask class
app = Flask(__name__)

# Enable Cross-Origin Resource Sharing (CORS) for the Flask app
CORS(app)

# Define the path to the SQLite database
DATABASE = "./scenario_2.db3"


# Function to establish a connection to the SQLite database
def get_db():
    # Connect to the database and set the row factory to sqlite3.Row
    # This allows us to access columns by name
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


# Define a route for the '/ids' endpoint with GET method
@app.route("/ids", methods=["GET"])
def get_ids():
    # Get a database connection
    conn = get_db()
    # Create a cursor object
    cursor = conn.cursor()
    # Execute a SQL query to select distinct IDs from the table
    cursor.execute("SELECT DISTINCT ID FROM T_RECH_Results")
    # Fetch all the rows from the result of the query
    rows = cursor.fetchall()
    # Extract the IDs from the rows
    ids = [row["ID"] for row in rows]
    # Close the database connection
    conn.close()
    # Return the IDs as a JSON response
    return jsonify(ids)


# Define a route for the '/data' endpoint with GET method
@app.route("/data", methods=["GET"])
def get_data():
    # Get the 'id' parameter from the request's query string
    id = request.args.get("id")
    # Get a database connection
    conn = get_db()
    # Create a cursor object
    cursor = conn.cursor()
    # Execute a SQL query to select Time and Q_m3 for the given ID
    cursor.execute("SELECT Time, Q_m3 FROM T_RECH_Results WHERE ID = ?", (id,))
    # Fetch all the rows from the result of the query
    rows = cursor.fetchall()
    # Extract the Time and Q_m3 values from the rows
    time_series = [row["Time"] for row in rows]
    runoff_series = [row["Q_m3"] for row in rows]
    # Close the database connection
    conn.close()
    # Return the time series and runoff series as a JSON response
    return jsonify({"time": time_series, "runoff": runoff_series})


# Main entry point of the script
if __name__ == "__main__":
    # Run the Flask app with debug mode enabled
    app.run(debug=True)
