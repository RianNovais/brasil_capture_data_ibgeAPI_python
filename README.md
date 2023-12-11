# brasil_capture_data_ibgeAPI_python

Projeto de um script em Python que cria um banco de dados SQLite e insere dados nesse banco em suas respectivas tabelas 
(regiões, estados, mesorregiões, microrregiões, cidades e distritos) através da API do IBGE(Instituto Brasileiro de Geografia e Estatística) capturando 
esses dados utilizando a biblioteca requests do python

Design of a script in Python that creates a SQLite database and inserts data into this database in their 
respective tables (regions, states, mesorregions, microrregions, cities and districts) through the IBGE (Brazilian Institute of Geography and Statistics) API,
capturing this data using the python requests library.



## Como executar (How to run):

### 1. Instalar os requisitos (Install the requirements):

Execute(Run):

```bash
pip install -r requirements.txt
```

### 2.Execute o arquivo main.py (Run main.py)

- Ao executar o script o arquivo do banco de dados na extensão .sqlite será criado na pasta raíz do projeto, será criadas as tabelas e os dados da API serão inseridos de acordo com seu devido relacionamento.
- When executing the script, the database file with the .sqlite extension will be created in the project's root folder, the tables will be created and the API data will be inserted according to their proper relationship


#### Python version 3.11

### IBGE API DOC: https://servicodados.ibge.gov.br/api/docs/
