import sqlite3
from pathlib import Path
import requests

class Database():
    def __init__(self):
        self.path = Path().absolute() / 'db.sqlite3'
        self.conn = sqlite3.Connection(self.path)
        self.cursor = self.conn.cursor()

        self.regionsUrl = "https://servicodados.ibge.gov.br/api/v1/localidades/regioes"
        self.statesUrl = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"


        self.create_tables()
        self.add_regions_to_db()
        self.add_states_to_db()

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

    def add_regions_to_db(self):
        #verificando se ja existem dados na tabela, pra nao dar erro
        self.cursor.execute('SELECT * FROM regions')
        if len(self.cursor.fetchall()) == 0:
            #try/except pra caso dê erro na requisição
            try:
                responseRegions = requests.get(self.regionsUrl).json()
            except:
                print(f'ERROR IN GET -> {self.regionsUrl}')
                return
            #pra cada regiao no JSON que veio da api, ele vai pegar cada campo e depois adicionar na tabela
            for region in responseRegions:
                _id = region['id']
                acronym = region['sigla']
                name = region['nome']

                data = [_id, acronym, name]
                self.cursor.execute('INSERT INTO regions (id, acronym, name) VALUES'
                                '(?, ?, ?)', data)

                self.conn.commit()
            print('REGIONS ADDED SUCESSFULLY TO DATABASE')
            return
        print('ERROR: REGIONS TABLE IS NOT EMPTY')

    def add_states_to_db(self):
        self.cursor.execute('SELECT * FROM STATES')
        if len(self.cursor.fetchall()) == 0:
            try:
                responseStates = requests.get(self.statesUrl).json()
            except:
                print(f'ERROR IN GET -> {self.statesUrl}')
                return
            for state in responseStates:
                _id = state['id']
                acronym = state['sigla']
                name = state['nome']
                idRegion = state['regiao']['id']

                stateData = [_id, acronym, name, idRegion]
                self.cursor.execute('INSERT INTO states (id, acronym, name, idRegion) VALUES '
                                    '(?, ?, ?, ?)', stateData)

                self.conn.commit()
            print('States added sucessfully to database')
        else:
            print('ERROR: STATES TABLE IS NOT EMPTY')
            return


if __name__ == "__main__":
    db = Database()