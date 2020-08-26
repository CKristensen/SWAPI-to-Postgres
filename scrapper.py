import requests 
import json
import pprint
import psycopg2
from psycopg2 import sql



# Write a Python function that retrieves a single resource from the SWAPI given its resource URL, and returns the response object.
def retrieve_source(URL):
    page = page = requests.get(URL)
    return page.json()

# Use the function above to run /schema/ for all resource types in the SWAPI (f.ex. /people/schema/). 
# Create corresponding tables in a new schema in your database catalogue. 
# Note that we want units for each physical measure like height, weight, etc. 
# All descriptive attributes like hair colour should have a an uppercase first letter in the resulting table. 
# Hint: check out a description of schemas here: https://swapi.dev/documentation#schema


def retrieval_all_schemas():
    URL = 'https://swapi.dev/api/'
    page = requests.get(URL)
    dic = page.json()
    jsons = {}
    for d in dic:
        URL = 'https://swapi.dev/api/' + str(d) + '/schema'
        jsons[d] = requests.get(URL).json()
    return jsons



#retrives dictionary with table name as key and column names list as content
def retrieve_tables_columns():
    tables_dic = {}
    jsons = retrieval_all_schemas()
    for j in jsons:
        columns_list = []
        for i in jsons[j]['properties']:
            columns_list.append((i))
            tables_dic[j] = columns_list
    return tables_dic

def connect():
    # Connect to an existing database
    conn = psycopg2.connect(host = "ds-etl-academy.cgbivchwjzle.eu-west-1.rds.amazonaws.com",
    dbname ="---",
    user = "---",
    password = "---",
    port = 5432
    )
    cursor = conn.cursor()
    return conn, cursor

def close(conn, cursor):  
    conn.commit()
    cursor.close()
    conn.close()

def delete_table(name):
    conn, cursor = connect()
    query = sql.SQL("DROP TABLE swapi.{name};").format(
            name=sql.Identifier(name))
    cursor.execute(query)
    close(conn, cursor)

# Write a Python procedure that creates a table in your Postgres catalogue. 
# The name of the table as well as the columns and data types should be inputs. 
# Think carefully of what data type you use to link columns to their data types.
def create_custom_table(name, columns=[]):
    conn, cursor = connect()
    query = sql.SQL("CREATE TABLE IF NOT EXISTS swapi.{name} (id serial primary key);").format(
            name=sql.Identifier(name))
    cursor.execute(query)

    for c in columns:
        query = sql.SQL("ALTER TABLE swapi.{name} ADD COLUMN {c} TEXT DEFAULT ''").format(
            name=sql.Identifier(name),
            c=sql.Identifier(c))
        cursor.execute(query)

    close(conn, cursor)
    
# CREATE A TABLE FOR EACH SCHEMA
def create_swapi_tables():
    table_dic = retrieve_tables_columns()
    for t in table_dic:
        try:
            delete_table(t)
        except:
            pass
        create_custom_table(t, table_dic[t])
        
def insert_film(columns):
    conn, cursor = connect()

    query = sql.SQL('''INSERT INTO swapi.films (title, episode_id, opening_crawl, director, producer, 
    release_date, characters, planets, starships, vehicles, species, created, edited, url) VALUES (%s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s)''')
    cursor.execute(query, columns)
    close(conn, cursor)

#Retrives dictionary with table name as key and column names list as content
def populate_film_table():
    URL = 'https://swapi.dev/api/films/'
    all_film = requests.get(URL).json()['results']
    attr = ['title', 'episode_id', 'opening_crawl', 'director', 'producer', 'release_date', 'characters', 'planets', 'starships', 'vehicles', 'species', 'created', 'edited', 'url']
    for film in all_film:
        columns = [film[col] for col in attr]
        insert_film(columns)

def insert_people(columns):
    conn, cursor = connect()
    query = sql.SQL('''INSERT INTO swapi.people (starships, edited, name, created, url, gender, 
    vehicles, skin_color, hair_color, height, eye_color, mass, films, species, homeworld, birth_year) VALUES (%s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''')
    cursor.execute(query, columns)
    close(conn, cursor)

def populate_people_table():
    URL = 'https://swapi.dev/api/people/'
    all_people = requests.get(URL).json()['results']

    attr = ['starships', 'edited', 'name', 'created', 'url', 'gender', 
    'vehicles', 'skin_color', 'hair_color', 'height',
     'eye_color', 'mass', 'films', 'species', 'homeworld', 'birth_year']
    for people in all_people:
        columns = [people[col] for col in attr]
        insert_people(columns)

# populate_people_table()
URL = 'https://swapi.dev/api/people/'
all_people = requests.get(URL).json()['results']
for people in all_people:
    print(people)
    print('##########¤¤¤¤###########')
