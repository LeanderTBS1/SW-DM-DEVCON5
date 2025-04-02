import sqlite3, csv ,os
from pathlib import Path

class QueriesHandler:
    def __init__(self, db_path):
        self.db_path = db_path
        db_dir = Path(self.db_path).parent
        if not db_dir.exists():
            db_dir.mkdir(parents=True)
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def get_avg(self, date):
        data = self.cursor.execute("""
            SELECT 
            AVG(temperature) AS avg_temperature
            FROM dht_data
            WHERE DATE(timestamp) = ?;
        """, (date,))
        return data.fetchone()
    
    def get_min(self,date):
        data = self.cursor.execute("""
            SELECT 
            MIN(temperature) AS min_temperature
            FROM dht_data
            WHERE DATE(timestamp) = ?;                           
        """ , (date,))
        return data.fetchone()

    def get_max(self):
         data = self.cursor.execute("""
            SELECT
                MAX(temperature) AS max_temperature
            FROM dht_data
            WHERE DATE(timestamp) = '2022-03-14';                       
        """)
if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(path, "data.db")

    db = QueriesHandler(db_path)
    print(db.get_avg('2022-03-14'))
    print(db.get_max)('2022-03-14')
    print(db.get_min('2022-03-14'))