import sqlite3
from pathlib import Path

class Database():
    def __init__(self):
        self.path = Path().absolute() / 'db.sqlite3'
        self.conn = sqlite3.Connection(self.path)
        self.cursor = self.conn.cursor()

        self.create_tables()

    #DDL: Creating tables of database
    def create_tables(self):
        self.cursor.execute("CREATE TABLE IF NOT EXISTS regions("
                            "id INTEGER NOT NULL,"
                            "acronym TEXT(2) NOT NULL,"
                            "name TEXT NOT NULL,"
                            "CONSTRAINT regions_PK PRIMARY KEY (id))")

        self.cursor.execute("CREATE TABLE IF NOT EXISTS states("
                            "id INTEGER NOT NULL,"
                            "acronym TEXT(2) NOT NULL,"
                            "name TEXT NOT NULL,"
                            "idRegion INTEGER NOT NULL,"
                            "CONSTRAINT states_PK PRIMARY KEY (id),"
                            "CONSTRAINT states_FK FOREIGN KEY (idRegion) REFERENCES regions(id)"
                            "ON DELETE CASCADE ON UPDATE CASCADE)")

        self.cursor.execute("CREATE TABLE IF NOT EXISTS cities("
                            "id INTEGER NOT NULL,"
                            "name TEXT NOT NULL,"
                            "stateId INTEGER NOT NULL, "
                            "CONSTRAINT cities_PK PRIMARY KEY (id),"
                            "CONSTRAINT cities_FK FOREIGN KEY (stateId) REFERENCES states(id)"
                            "ON DELETE CASCADE ON UPDATE CASCADE)")

        self.cursor.execute("CREATE TABLE IF NOT EXISTS districts("
                            "id INTEGER NOT NULL,"
                            "name TEXT NOT NULL,"
                            "cityId INTEGER NOT NULL,"
                            "cityName TEXT NOT NULL,"
                            "CONSTRAINT districts_PK PRIMARY KEY (id),"
                            "CONSTRAINT districts_FK FOREIGN KEY (cityId) REFERENCES cities(id)"
                            "ON DELETE CASCADE ON UPDATE CASCADE)")

        self.conn.commit()



if __name__ == "__main__":
    db = Database()