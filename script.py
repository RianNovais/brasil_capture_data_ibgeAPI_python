import sqlite3
import time
from pathlib import Path
from datetime import datetime
import time
import requests

#Main class that contains the script's operating methods
class Script():
    def __init__(self):
        self.path = Path().absolute() / 'db.sqlite3'
        self.conn = sqlite3.Connection(self.path)
        self.cursor = self.conn.cursor()
        self.addedRows = 0


        self.regionsUrl = "https://servicodados.ibge.gov.br/api/v1/localidades/regioes"
        self.statesUrl = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
        self.citiesUrl = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
        self.districtsUrl = "https://servicodados.ibge.gov.br/api/v1/localidades/distritos"
        self.mesoregionsUrl = "https://servicodados.ibge.gov.br/api/v1/localidades/mesorregioes"
        self.microregionsUrl = "https://servicodados.ibge.gov.br/api/v1/localidades/microrregioes"

    # Method that starts, showing a start message, and the function calls in order
    def run_script(self):
        start = datetime.now()

        currentTime = datetime.strftime(datetime.now(), '%d/%m/%Y - %H:%M')
        print('-' * 20 + currentTime + '-' * 20)
        print('START---')
        time.sleep(3)

        self.create_tables()

        self.add_regions_to_db()
        self.add_states_to_db()
        self.add_mesoregions_to_db()
        self.add_microregions_to_db()
        self.add_cities_to_db()
        self.add_districts_to_db()

        end = datetime.now()

        self.executionTime = (end - start).total_seconds()
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
                            "regionId INTEGER NOT NULL,"
                            "regionName TEXT NOT NULL, "
                            "CONSTRAINT states_PK PRIMARY KEY (id),"
                            "CONSTRAINT states_FK FOREIGN KEY (regionId) REFERENCES regions(id)"
                            "ON DELETE CASCADE ON UPDATE CASCADE)")

        self.cursor.execute("CREATE TABLE IF NOT EXISTS mesoregions("
                            "id INTEGER NOT NULL,"
                            "name TEXT NOT NULL,"
                            "stateId INTEGER NOT NULL,"
                            "stateName TEXT NOT NULL,"
                            "regionId INTEGER NOT NULL,"
                            "regionName TEXT NOT NULL,"
                            "CONSTRAINT mesoregions_PK PRIMARY KEY (id),"
                            "CONSTRAINT mesoregions_FK_STATE FOREIGN KEY (stateId) REFERENCES states(id)"
                            "ON DELETE CASCADE ON UPDATE CASCADE,"
                            "CONSTRAINT mesoregions_FK_REGION FOREIGN KEY (regionId) REFERENCES regions(id)"
                            "ON DELETE CASCADE ON UPDATE CASCADE)")

        self.cursor.execute("CREATE TABLE IF NOT EXISTS microregions("
                            "id INTEGER NOT NULL,"
                            "name TEXT NOT NULL,"
                            "mesoregionId INTEGER NOT NULL,"
                            "mesoregionName TEXT NOT NULL,"
                            "stateId INTEGER NOT NULL,"
                            "stateName TEXT NOT NULL,"
                            "regionId INTEGER NOT NULL,"
                            "regionName TEXT NOT NULL,"
                            "CONSTRAINT microregions_PK PRIMARY KEY (id),"
                            "CONSTRAINT microregions_FK_MESOREGIONS FOREIGN KEY (mesoregionId) REFERENCES mesoregions(id)"
                            "ON DELETE CASCADE ON UPDATE CASCADE,"
                            "CONSTRAINT microregions_FK_STATE FOREIGN KEY (stateId) REFERENCES states(id)"
                            "ON DELETE CASCADE ON UPDATE CASCADE,"
                            "CONSTRAINT microregions_FK_REGION FOREIGN KEY (regionId) REFERENCES regions(id)"
                            "ON DELETE CASCADE ON UPDATE CASCADE)")

        self.cursor.execute("CREATE TABLE IF NOT EXISTS cities("
                            "id INTEGER NOT NULL,"
                            "name TEXT NOT NULL,"
                            "microregionId INTEGER NOT NULL,"
                            "microregionName TEXT NOT NULL, "
                            "mesoregionId INTEGER NOT NULL,"
                            "mesoregionName TEXT NOT NULL, "
                            "stateId INTEGER NOT NULL,"
                            "stateName TEXT NOT NULL,"
                            "regionId INTEGER NOT NULL,"
                            "regionName TEXT NOT NULL,"
                            "CONSTRAINT cities_PK PRIMARY KEY (id),"
                            "CONSTRAINT cities_FK_STATE FOREIGN KEY (stateId) REFERENCES states(id)"
                            "ON DELETE CASCADE ON UPDATE CASCADE,"
                            "CONSTRAINT cities_FK_MESOREGION FOREIGN KEY (mesoregionId) REFERENCES mesoregions(id)"
                            "ON DELETE CASCADE ON UPDATE CASCADE,"
                            "CONSTRAINT cities_FK_MICROREGION FOREIGN KEY (microregionId) REFERENCES microregions(id)"
                            "ON DELETE CASCADE ON UPDATE CASCADE,"
                            "CONSTRAINT cities_FK_REGION FOREIGN KEY (regionId) REFERENCES regions(id)"
                            "ON DELETE CASCADE ON UPDATE CASCADE)")

        self.cursor.execute("CREATE TABLE IF NOT EXISTS districts("
                            "id INTEGER NOT NULL,"
                            "name TEXT NOT NULL,"
                            "cityId INTEGER NOT NULL,"
                            "cityName TEXT NOT NULL,"
                            "microregionId INTEGER NOT NULL,"
                            "microregionName TEXT NOT NULL,"
                            "mesoregionId INTEGER NOT NULL,"
                            "mesoregionName TEXT NOT NULL,"
                            "stateId INTEGER NOT NULL,"
                            "stateName TEXT NOT NULL,"
                            "regionId INTEGER NOT NULL,"
                            "regionName TEXT NOT NULL,"
                            "CONSTRAINT districts_PK PRIMARY KEY (id),"
                            "CONSTRAINT districts_FK_CITY FOREIGN KEY (cityId) REFERENCES cities(id)"
                            "ON DELETE CASCADE ON UPDATE CASCADE,"
                            "CONSTRAINT districts_FK_MICROREGION FOREIGN KEY (microregionId) REFERENCES microregions(id)"
                            "ON DELETE CASCADE ON UPDATE CASCADE,"
                            "CONSTRAINT districts_FK_MESOREGION FOREIGN KEY (mesoregionId) REFERENCES mesoregions(id)"
                            "ON DELETE CASCADE ON UPDATE CASCADE,"
                            "CONSTRAINT districts_FK_STATE FOREIGN KEY (stateId) REFERENCES states(id)"
                            "ON DELETE CASCADE ON UPDATE CASCADE,"
                            "CONSTRAINT districts_FK_REGION FOREIGN KEY (regionId) REFERENCES regions(id)"
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
                self.addedRows = self.addedRows +1

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
                regionName = state['regiao']['nome']

                stateData = (_id, acronym, name, idRegion, regionName)
                self.cursor.execute('INSERT INTO states (id, acronym, name, regionId, regionName) VALUES '
                                    '(?, ?, ?, ?, ?)', stateData)
                self.addedRows = self.addedRows + 1
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

                microregionId = city['microrregiao']['id']
                microregionName = city['microrregiao']['nome']
                mesoregionId = city['microrregiao']['mesorregiao']['id']
                mesoregionName = city['microrregiao']['mesorregiao']['nome']
                stateId = city['microrregiao']['mesorregiao']['UF']['id']
                stateName = city['microrregiao']['mesorregiao']['UF']['nome']
                regionId = city['microrregiao']['mesorregiao']['UF']['regiao']['id']
                regionName = city['microrregiao']['mesorregiao']['UF']['regiao']['nome']

                # stateNome = city['microrregiao']['mesorregiao']['UF']['nome']

                cityData = (_id, name,microregionId, microregionName, mesoregionId, mesoregionName, stateId, stateName, regionId, regionName)
                self.cursor.execute('INSERT INTO cities (id, name, microregionId, microregionName, mesoregionId, mesoregionName, stateId, stateName, regionId, regionName) '
                                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', cityData)
                self.addedRows = self.addedRows + 1
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
                microregionId = district['municipio']['microrregiao']['id']
                microregionName = district['municipio']['microrregiao']['nome']
                mesoregionId = district['municipio']['microrregiao']['mesorregiao']['id']
                mesoregionName = district['municipio']['microrregiao']['mesorregiao']['nome']
                stateId = district['municipio']['microrregiao']['mesorregiao']['UF']['id']
                stateName = district['municipio']['microrregiao']['mesorregiao']['UF']['nome']
                regionId = district['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['id']
                regionName = district['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['nome']

                districtData = (_id, name, cityId, cityName,microregionId, microregionName,mesoregionId, mesoregionName,
                                stateId, stateName, regionId, regionName)

                self.cursor.execute('INSERT INTO districts (id, name, cityId, cityName, microregionId, microregionName,'
                                    'mesoregionId, mesoregionName, stateId, stateName, regionId, regionName) '
                                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', districtData)
                self.addedRows = self.addedRows + 1
                self.conn.commit()
            print('Districts added sucessfully to database')
        else:
            print('ERROR: DISTRICT TABLE IS NOT EMPTY')

    def add_mesoregions_to_db(self):
        self.cursor.execute('SELECT * from mesoregions')
        if len(self.cursor.fetchall()) == 0:
            try:
                response = requests.get(self.mesoregionsUrl).json()
            except:
                print(f'ERROR IN GET -> {self.mesoregionsUrl}')
                return
            for mesoregion in response:

                _id = mesoregion['id']
                name = mesoregion['nome']
                stateId = mesoregion['UF']['id']
                stateName = mesoregion['UF']['nome']
                regionId = mesoregion['UF']['regiao']['id']
                regionName = mesoregion['UF']['regiao']['nome']

                mesoregionData = (_id, name, stateId, stateName, regionId, regionName)
                self.cursor.execute('INSERT INTO mesoregions (id, name, stateId, stateName, regionId, regionName)'
                                    'VALUES (?, ?, ?, ?, ?, ?)', mesoregionData)
                self.addedRows = self.addedRows + 1
                self.conn.commit()
            print('Mesoregions added sucessfully to database')
        else:
            print('ERROR: MESOREGIONS TABLE IS NOT EMPTY')
            return

    def add_microregions_to_db(self):
        self.cursor.execute('SELECT * FROM microregions')
        if len(self.cursor.fetchall()) == 0:
            try:
                response = requests.get(self.microregionsUrl).json()
            except:
                print(f'ERROR IN GET -> {self.microregionsUrl}')
            for microregion in response:
                _id = microregion['id']
                name = microregion['nome']
                mesoregionId = microregion['mesorregiao']['id']
                mesoregionName = microregion['mesorregiao']['nome']
                stateId = microregion['mesorregiao']['UF']['id']
                stateName = microregion['mesorregiao']['UF']['nome']
                regionId = microregion['mesorregiao']['UF']['regiao']['id']
                regionName = microregion['mesorregiao']['UF']['regiao']['nome']


                microregionData = (_id, name, mesoregionId, mesoregionName, stateId, stateName, regionId, regionName)

                self.cursor.execute('INSERT INTO microregions (id, name, mesoregionId, mesoregionName, stateId, stateName, regionId, regionName)'
                                    'VALUES (?, ?, ?, ?, ?, ?, ? ,?)', microregionData)
                self.addedRows = self.addedRows + 1
                self.conn.commit()
            print('Microregions added sucessfully to database')
        else:
            print('ERROR: MESOREGIONS IS NOT EMPTY')
            return

    #Finishing the script, showing sucessfull message
    def finish(self):
        print('')
        print('END---')
        print(f'time of execution : {self.executionTime:.6f} seconds')
        print(f'Added {self.addedRows} rows in database')
        print(f'database file .sqlite3 saved in {self.path}')
        self.cursor.close()
        self.conn.close()

