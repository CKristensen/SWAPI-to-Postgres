import db_operations as db
import requests 
import json
from psycopg2 import sql
import re
import logging

PRINT = True

def retrieve_source(URL):
    page = requests.get(URL)
    return page.json()

def insert_film(id):
    # title, episode_id, opening_crawl, director, producer, release_date, characters, planets, starships, vehicles, species, created, edited, url, film_id
    json_person = retrieve_source(f'https://swapi.dev/api/films/{id}/')
    row = [json_person[col] for col in json_person if(col  not in ['characters', 'starships', 'vehicles', 'species', 'planets'])]
    row.append(id)
    conn, cursor = db.connect()

    query = sql.SQL('''INSERT INTO swapi.films (title, episode_id, opening_crawl, director, 
    producer, release_date, created, edited, url, film_id) VALUES (%s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s)''')
    cursor.execute(query, [r if(str(r) != 'unknown') else None for r in row])
    db.close(conn, cursor)

def insert_planet(id):
    # name, rotation_period, orbital_period, diameter, climate, gravity, terrain, surface_water, population, residents, films, created, edited, url, planet_id
    json_person = retrieve_source(f'https://swapi.dev/api/planets/{id}/')
    row = [json_person[col] for col in json_person if(col  not in ['films', 'characters', 'starships', 'vehicles', 'species', 'residents'])]
    row.append(id)
    conn, cursor = db.connect()

    query = sql.SQL('''INSERT INTO swapi.planets (name, rotation_period, orbital_period, 
    diameter, climate, gravity, terrain, surface_water, population, created, edited, url, planet_id) VALUES (%s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s)''')
    cursor.execute(query, [r if(str(r) != 'unknown') else None for r in row])
    db.close(conn, cursor)

def insert_species(id):
    # name, classification, designation, average_height, skin_colors, hair_colors, eye_colors, average_lifespan, homeworld, language, people, films, created, edited, url, species_id
    json_person = retrieve_source(f'https://swapi.dev/api/species/{id}/')
    row = [json_person[col] for col in json_person if(col  not in ['films', 'people', 'starships', 'vehicles', 'species', 'residents'])]
    row.append(id)
    conn, cursor = db.connect()

    query = sql.SQL('''INSERT INTO swapi.species (name, classification, designation, average_height, skin_colors, 
    hair_colors, eye_colors, average_lifespan, homeworld, language, created, edited, url, species_id) VALUES (%s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s)''')
    cursor.execute(query, [r if(str(r) != 'unknown') else None for r in row])
    db.close(conn, cursor)
    
def insert_vehicles(id):
    # name, model, manufacturer, cost_in_credits, length, max_atmosphering_speed, crew, passengers, cargo_capacity, consumables, vehicle_class, pilots, films, created, edited, url, vehicle_id
    json_person = retrieve_source(f'https://swapi.dev/api/vehicles/{id}/')
    row = [json_person[col] for col in json_person if(col  not in ['films', 'pilots'])]
    row.append(id)
    conn, cursor = db.connect()

    query = sql.SQL('''INSERT INTO swapi.vehicles (name, model, manufacturer, cost_in_credits, length, max_atmosphering_speed, crew, passengers, cargo_capacity, 
    consumables, vehicle_class, created, edited, url, vehicle_id) VALUES (%s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''')
    cursor.execute(query, [r if(str(r) != 'unknown') else None for r in row])
    db.close(conn, cursor)

def insert_person(id):
    # [name, height, mass, hair_color, skin_color, eye_color, birth_year, gender, homeworld, created, edited, url, people_id]
    json_person = retrieve_source(f'https://swapi.dev/api/people/{id}/')
    row = [json_person[col] for col in json_person if(col  not in ['films', 'starships', 'vehicles', 'species'])]
    row.append(id)
    conn, cursor = db.connect()

    query = sql.SQL('''INSERT INTO swapi.people (name, height, mass, hair_color,
     skin_color, eye_color, birth_year, gender, homeworld, created, edited, url, people_id) VALUES (%s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s)''')
    cursor.execute(query, [str(r).replace(',', '') if(str(r) != 'unknown') else None for r in row])
    
    db.close(conn, cursor)

def insert_starships(id):
    json_person = retrieve_source(f'https://swapi.dev/api/starships/{id}/')
    row = [json_person[col] for col in json_person if(col  not in ['films', 'starships', 'vehicles', 'pilots'])]
    row.append(id)
    conn, cursor = db.connect()

    query = sql.SQL('''INSERT INTO swapi.starships (name, model, manufacturer, cost_in_credits, length, max_atmosphering_speed, crew, passengers,
	cargo_capacity, consumables, hyperdrive_rating, MGLT, starship_class, created, edited, url, starship_id)
     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''')
    cursor.execute(query, [r if(str(r) != 'unknown') else None for r in row])
    
    db.close(conn, cursor)

def insert_film_vehicles(film_id, vehicle_id):
    conn, cursor = db.connect()
    query = sql.SQL('''INSERT INTO swapi.film_vehicles (film_id, vehicles_id) VALUES (%s, %s)''')
    try:
        cursor.execute(query, (film_id, vehicle_id))
    except:
        pass
    db.close(conn, cursor)

def insert_film_species(film_id, species_id):
    conn, cursor = db.connect()
    query = sql.SQL('''INSERT INTO swapi.film_species (film_id, species_id) VALUES (%s, %s)''')
    try:
        cursor.execute(query, (film_id, species_id))
    except:
        pass
    db.close(conn, cursor)

def insert_film_planet(film_id, planet_id):
    conn, cursor = db.connect()
    
    query = sql.SQL('''INSERT INTO swapi.film_planety (film_id, planet_id) VALUES (%s, %s)''')
    try:
        cursor.execute(query, (film_id, planet_id))
    except:
        pass
    db.close(conn, cursor)

def insert_film_starships(film_id, starships_id):
    conn, cursor = db.connect()
    query = sql.SQL('''INSERT INTO swapi.film_starships (film_id, starships_id) VALUES (%s, %s)''')
    
    try:
        cursor.execute(query, (film_id, starships_id))
    except:
        pass

    db.close(conn, cursor)

def insert_people_starships(people_id, starships_id):
    conn, cursor = db.connect()
    query = sql.SQL('''INSERT INTO swapi.people_starships (people_id, starships_id) VALUES (%s, %s)''')
    
    try:
        cursor.execute(query, (people_id, starships_id))
    except:
        pass
    db.close(conn, cursor)

def insert_people_planet(people_id, planet_id):
    conn, cursor = db.connect()
    query = sql.SQL('''INSERT INTO swapi.people_planet (people_id, planet_id) VALUES (%s, %s)''')
    try:
        cursor.execute(query, (people_id, planet_id))
    except:
        pass
    db.close(conn, cursor)

def insert_people_films(people_id, film_id):
    conn, cursor = db.connect()
    query = sql.SQL('''INSERT INTO swapi.people_film (people_id, film_id) VALUES (%s, %s)''')
    try:
        cursor.execute(query, (people_id, film_id))
    except:
        pass
    db.close(conn, cursor)
    
def get_all_relations(type_, id):
    """
    Will return a list with all the relations of the SW object.
    Args:
        type_ (string): films/people/vehicles/starships/species/planets
        id (int): id of object

    Returns:
        [type]: [description]
    """
    j = retrieve_source('https://swapi.dev/api/'+ type_ + f'/{id}/')
    r_list = []
    for key in j:
        if(type(j[key]) is list):
            for item in j[key]:
                r_list.append(item.split('/')[-3:-1])
    return r_list

def insert_generic(type, id):
    """Inserts a generit SW object into the database.
        Will check the objects type and call the appropriate function.

    Args:
        type (string)
        id (int): id of object
    """    
    if(type == 'films'):
        insert_film(id)
    if(type == 'planets'):
        insert_planet(id)
    if(type == 'species'):
        insert_species(id)
    if(type == 'vehicles'):
        insert_vehicles(id)
    if(type == 'people'):
        insert_person(id)
    if(type=='starships'):
        insert_starships(id)

def insert_relation(rel):
    """Given a list with a relation it will add to the database that relation.
    note that both items must be in the database beforehand!


    Args:
        rel (list): list containing 2 relation-elements. 
        example:
            [['people', 1],['films', 1]]

    Returns:
        nothing
    """    
    conn, cursor = db.connect()
    table1, table2 = 0, 0
    table = [0, 0]
    if(rel[0][0]=='people'):
        table1, table2 = rel[0], rel[1]
    elif(rel[1][0]=='people'):
        table1, table2 = rel[1], rel[0]
    elif(rel[0][0]=='films'):
        table1, table2 = rel[0], rel[1]
    elif(rel[1][0]=='films'):
        table1, table2 = rel[1], rel[0]
    elif(rel[0][0]=='planet'):
        table1, table2 = rel[0], rel[1]
    elif(rel[1][0]=='planet'):
        table1, table2 = rel[1], rel[0]
    else:
        return 0


    table1[0] = re.sub(r's$', '', table1[0]) if(table1[0] not in ('species')) else 'species'
    table2[0] = re.sub(r's$', '', table2[0]) if(table2[0] not in ('species')) else 'species'
    
    table = re.sub(r'[\[\]\']', '', str(table1[0])) + '_' + re.sub(r'[\[\]\']', '', str(table2[0]))
    query = sql.SQL('''INSERT INTO swapi.{table} ({table1}, {table2}) VALUES (%s, %s)''').format(
            table=sql.Identifier(table),
            table1=sql.Identifier(table1[0]+'_id'),
            table2=sql.Identifier(table2[0]+'_id'))
    try:
        cursor.execute(query, (table1[1], table2[1]))
        if(PRINT):
            logging.info(f'Inserted relations between:{table1} and {table2}')       
    except:
        if(PRINT):
            logging.info(f'Error while inserting relation, {table} {table1[0]} {table2[0]}')
    db.close(conn, cursor)

def six_degrees_from_luke():
    """[Main function that will populate the Postgres Database]
        - This function will populate the Database starting with Luke Skywalker and then 
        - adding all his connections plus their connections and so on.
        - This way we do not need to have access to the hosts database in order to scrape the whole data.
        - This was done with that challenge in mind. There are more efficient ways to populate the database
        - if we access the page with all the id's of the films/people/species/... in the SW universe.
        -
        - If you run this a second time then uncomment the first line of this function to reset the database.
        -
        - May the force be with you!s
    """
    # db.delete_swapi_db()
    db.create_swapi_db()
    luke = ['people', 1]
    pipeline = get_all_relations(luke[0], luke[1])
    relations_pipeline = [(luke, rel) for rel in pipeline]
    try:
        insert_generic(luke[0], luke[1])
        if(PRINT):
            logging.info(f'Inserted: {luke[0]}/{luke[1]}')
    except:
        pass

    inserted = [luke]
    while(pipeline != []):
        item = pipeline.pop(0)
        try:
            insert_generic(item[0],item[1])
            if(PRINT):
                logging.info(f'Inserted: {item[0]}/{item[1]}')
        except :
            logging.info(f'failed inserting {item[0]}, {item[1]}!')
        inserted.append(item)
        new_rel = get_all_relations(item[0], item[1])
        for rel in new_rel:
            if((([item[0], item[1]], rel) not in relations_pipeline) or (rel, [item[0], item[1]]) not in relations_pipeline):
                relations_pipeline.append((item, rel))
                
        for new in new_rel:
            if((new in inserted) or (new in pipeline)):
                continue
            else:
                pipeline.append(new)
    for rel in relations_pipeline:
        insert_relation(rel)


if __name__ == "__main__":
    six_degrees_from_luke()
