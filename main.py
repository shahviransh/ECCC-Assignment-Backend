# Import necessary modules from Flask and SQLite3
from flask import Flask, jsonify, request
import sqlite3
import pandas as pd
import shapefile
from flask_cors import CORS
import h5py
import numpy as np
import rasterio
from rasterio.features import shapes
from shapely.geometry import shape as sh
import geopandas as gpd
import dbf
from geo.Geoserver import Geoserver
import shutil

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


def preprocess_raster_data(dataset, nrows, ncols):
    if len(dataset.shape) == 1:
        dataset = dataset.reshape(int(nrows), int(ncols))
    elif len(dataset.shape) == 2:
        pass  # Already 2D
    else:
        raise ValueError("Raster data must be 2D or 3D")
    return dataset


def raster_to_vector(raster):
    mask = raster != -32768  # Assuming NODATA_VALUE is -32768
    shapes_generator = shapes(raster, mask=mask)
    geoms = [(sh(geom), value) for geom, value in shapes_generator]
    geoms = pd.DataFrame(geoms, columns=["geometry", "River_ID"])
    return geoms


def read_river_network(file_path, dataset_name):
    with h5py.File(file_path, "r") as hdf_file:
        asc = hdf_file["asc"]
        dataset = np.array(asc[dataset_name], np.float32)
        attributes = asc.attrs
        ncols = 30
        nrows = 7591
        xllcorner = attributes["XLLCENTER"]
        yllcorner = attributes["YLLCENTER"]
        cellsize = attributes["DX"]
        NODATA_value = attributes["NODATA_VALUE"]
        # Reshape the dataset to 2D
        dataset = preprocess_raster_data(dataset, nrows, ncols)
    return dataset


# Function to fetch runoff data from SQLite database
def fetch_runoff_data(db_path):
    conn = sqlite3.connect(db_path)
    query = "SELECT ID, Time, Q_m3 FROM T_RECH_Results"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


# Function to add runoff data to vector data
def add_runoff_to_vector(vector_data, runoff_data):
    merged = runoff_data.merge(
        vector_data, left_on="ID", right_on="River_ID", how="left"
    )
    return merged


# Function to publish the vector data to GeoServer
def publish_vector_data(
    geoserver_url, workspace, layer_name, vector_data, username, password
):
    geo = Geoserver(geoserver_url, "admin", "geoserver")

    shapefile_path = "./spatial_data/river_network.shp"
    gpd.GeoDataFrame(vector_data, crs="EPSG:4326").to_file(shapefile_path)

    zip_path = "./spatial_data"
    shutil.make_archive(zip_path, "zip", zip_path)

    geo.create_shp_datastore(path=zip_path + ".zip", workspace=workspace, store_name=layer_name)

    return jsonify({"message": "Vector data & TIF file published successfully."})

# Define a route to vectorize the river network raster data, merge it with runoff data, and publish it to GeoServer
@app.route("/publish_river_network", methods=["GET"])
def publish_river_network():
    hdf5_path = "./task3/parameter.h5"
    dataset_name = "reach"
    db_path = "./scenario_2.db3"
    geoserver_url = "http://localhost:9090/geoserver"
    workspace = "ECCCGeoServer"
    layer_name = "river_network"
    username = "admin"
    password = "geoserver"

    river_network = read_river_network(hdf5_path, dataset_name)
    vector_data = raster_to_vector(river_network)
    runoff_data = fetch_runoff_data(db_path)
    vector_with_runoff = add_runoff_to_vector(vector_data, runoff_data)

    return publish_vector_data(
        geoserver_url,
        workspace,
        layer_name,
        vector_with_runoff,
        username,
        password,
    )


# Main entry point of the script
if __name__ == "__main__":
    # Run the Flask app with debug mode enabled
    app.run(debug=True)
