from flask import Flask, jsonify, request
import sqlite3
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

DATABASE = './scenario_2.db3'

def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/ids', methods=['GET'])
def get_ids():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT ID FROM T_RECH_Results')
    rows = cursor.fetchall()
    ids = [row['ID'] for row in rows]
    conn.close()
    return jsonify(ids)

@app.route('/data', methods=['GET'])
def get_data():
    id = request.args.get('id')
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT Time, Q_m3 FROM T_RECH_Results WHERE ID = ?', (id,))
    rows = cursor.fetchall()
    time_series = [row['Time'] for row in rows]
    runoff_series = [row['Q_m3'] for row in rows]
    conn.close()
    return jsonify({'time': time_series, 'runoff': runoff_series})

if __name__ == '__main__':
    app.run(debug=True)
