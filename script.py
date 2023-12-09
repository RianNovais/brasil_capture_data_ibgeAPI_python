import sqlite3
import time
from pathlib import Path
from datetime import datetime
from time import sleep
import requests

#Main class that contains the script's operating methods
class Script():
    def __init__(self):
        self.path = Path().absolute() / 'db.sqlite3'
        self.conn = sqlite3.Connection(self.path)
        self.cursor = self.conn.cursor()

        self.regionsUrl = "https://servicodados.ibge.gov.br/api/v1/localidades/regioes"
        self.statesUrl = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
        self.citiesUrl = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
        self.districtsUrl = "https://servicodados.ibge.gov.br/api/v1/localidades/distritos"

    # Method that starts, showing a start message, and the function calls in order
    def run_script(self):
        currentTime = datetime.strftime(datetime.now(), '%d/%m/%Y - %H:%M')
        print('-' * 20 + currentTime + '-' * 20)
        print('START')
        time.sleep(3)

        self.create_tables()
        self.add_regions_to_db()
        self.add_states_to_db()
        self.add_cities_to_db()
        self.add_districts_to_db()
        self.finish()

    #DDL: Creating tables
    def create_tables(self):
        print('CREATING TABLES IN DATABASE')
        time.sleep(2)
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
        print('TABLES CREATED SUCESSFULLY')
        print('')

        self.conn.commit()

    #DML: Insert Data in tables
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

                regionData = (_id, acronym, name)
                self.cursor.execute('INSERT INTO regions (id, acronym, name) VALUES'
                                '(?, ?, ?)', regionData)

                self.conn.commit()
            print('Regions added sucessfully to database')
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

                stateData = (_id, acronym, name, idRegion)
                self.cursor.execute('INSERT INTO states (id, acronym, name, idRegion) VALUES '
                                    '(?, ?, ?, ?)', stateData)

                self.conn.commit()
            print('States added sucessfully to database')
        else:
            print('ERROR: STATES TABLE IS NOT EMPTY')
            return

    def add_cities_to_db(self):
        self.cursor.execute('SELECT * FROM cities')
        if len(self.cursor.fetchall()) == 0:
            try:
                responseCities = requests.get(self.citiesUrl).json()
            except:
                print(f'ERROR IN GET -> {self.citiesUrl}')
                return
            for city in responseCities:
                _id = city['id']
                name = city['nome']
                stateId = city['microrregiao']['mesorregiao']['UF']['id']
                # stateNome = city['microrregiao']['mesorregiao']['UF']['nome']

                cityData = (_id, name,stateId)
                self.cursor.execute('INSERT INTO cities (id, name, stateId) '
                                    'VALUES (?, ?, ?)', cityData)

                self.conn.commit()

            print('Cities added sucessfully to database')
        else:
            print('ERROR: CITIES TABLE IS NOT EMPTY')
            return

    def add_districts_to_db(self):
        self.cursor.execute('SELECT * FROM districts')
        if len(self.cursor.fetchall()) == 0:
            try:
                response = requests.get(self.districtsUrl).json()
            except:
                print(f'ERROR IN GET -> {self.districtsUrl}')
                return

            for district in response:
                _id = district['id']
                name = district['nome']
                cityId = district['municipio']['id']
                cityName = district['municipio']['nome']

                districtData = (_id, name, cityId, cityName)

                self.cursor.execute('INSERT INTO districts (id, name, cityId, cityName) '
                                    'VALUES (?, ?, ?, ?)', districtData)

                self.conn.commit()
            print('Districts added sucessfully to database')
        else:
            print('ERROR: DISTRICT TABLE IS NOT EMPTY')

    #Finishing the script, showing sucessfull message
    def finish(self):
        print('')
        print('END')
        print(f'database file .sqlite3 saved in {self.path}')
        self.cursor.close()
        self.conn.close()
