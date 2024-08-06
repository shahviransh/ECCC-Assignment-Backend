# Import necessary modules from Flask and SQLite3
from flask import Flask, jsonify, request
import sqlite3
import pandas as pd
import shapefile
from flask_cors import CORS
import dbf

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


# Function to create or update a DBF file for an existing shapefile
def create_dbf(df, dbf_file_path):
    # Define the fields for the .dbf file
    fields = ""
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            fields += col + f" N({10},{0}); "  # Numeric type with no decimal places
        elif pd.api.types.is_float_dtype(dtype):
            fields += col + f" N({15},{5})"  # Numeric type with 5 decimal places
        else:
            fields += col + f" C({10}); "  # Character type with length 10

    # Create the .dbf file and add records
    table = dbf.Table(dbf_file_path, fields)
    table.open(dbf.READ_WRITE)
    for _, row in df.iterrows():
        # Convert each row to a tuple, handle NaN values
        row_tuple = tuple(str(value) if pd.notna(value) else "" for value in row)
        table.append(row_tuple)

    print(f"Data successfully written to {dbf_file_path}")


# Define a route to generate and save the DBF file
@app.route("/create_dbf", methods=["GET"])
def create_dbf_file():
    conn = get_db()
    query = "SELECT ID, Time, Q_m3 FROM T_RECH_Results"
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Create the DBF file
    create_dbf(df, "./spatial_data/basin.dbf")
    return jsonify({"message": "DBF file created successfully"})


# Main entry point of the script
if __name__ == "__main__":
    # Run the Flask app with debug mode enabled
    app.run(debug=True)
