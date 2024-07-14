const sqlite3 = require('sqlite3').verbose();

const db = new sqlite3.Database('scenario.db3');

db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS T_RECH_Result (
    ID INTEGER,
    Time TEXT,
    Q_m3 REAL
  )`);

  // Insert some sample data
  const stmt = db.prepare(`INSERT INTO T_RECH_Result (ID, Time, Q_m3) VALUES (?, ?, ?)`);
  const data = [
    [1, '2024-01-01', 10.5],
    [2, '2024-01-02', 12.3],
    [3, '2024-01-03', 15.4],
    [4, '2024-01-04', 17.8],
  ];

  for (let i = 0; i < data.length; i++) {
    stmt.run(data[i]);
  }

  stmt.finalize();
});

db.close();
