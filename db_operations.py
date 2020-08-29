import psycopg2
from psycopg2 import sql
import getpass 
import os

PASS = os.environ['ACADEMY_DB_PASS']
USER = os.environ['ACADEMY_USER_NAME']
DBNAME = os.environ['ACADEMY_USER_NAME']
HOST = os.environ['ACADEMY_DB']


def connect():
    """Connects to postgresdatabase

    Args:
        dbname (string): name of your catalog
        user (string): name of your username
        password (password): your password

    Returns:
        [type]: [description]

    """    
    conn = psycopg2.connect(host = "ds-etl-academy.cgbivchwjzle.eu-west-1.rds.amazonaws.com",
    dbname = DBNAME,
    user = USER,
    password = PASS,
    port = 5432
    )
    cursor = conn.cursor()
    return conn, cursor

def close(conn, cursor):  
    #closes connection to database
    conn.commit()
    cursor.close()
    conn.close()

def create_swapi_db():
    conn, cur = connect()
    
    cur.execute('''CREATE TABLE IF NOT EXISTS swapi.vehicles(
									vehicle_id int primary key,
									name text,
									model text,
									manufacturer text,
									cost_in_credits int,
									length float,
									max_atmosphering_speed int,
									crew int,
									passengers int,
									cargo_capacity text,
									consumables text,
									vehicle_class text,
									created text,
									edited text,
									url text);''')
    
    cur.execute('''CREATE TABLE IF NOT EXISTS swapi.people (people_id int primary key, name text,
								  height int,
								  mass real,
								  hair_color text,
								  skin_color text,
								  eye_color text,
								  birth_year text,
								  gender text,
								  homeworld text,
								  url text,
								  created text,
								  edited text);''')

    cur.execute('''CREATE TABLE IF NOT exists swapi.planets	( 		
											planet_id int primary key,
											name text,
											rotation_period text,
											orbital_period text,
											diameter text,
											climate text,
											gravity text,
											terrain text,
											surface_water text,
											population text,
											created text,
											edited text,
											url text);''')

    cur.execute('''CREATE TABLE IF NOT EXISTS swapi.starships (
											starship_id int primary key,
											name text,
											model text,
											manufacturer text,
											cost_in_credits text,
											length text,
											max_atmosphering_speed text,
											crew text,
											passengers text,
											cargo_capacity text,
											consumables text,
											hyperdrive_rating text,
											MGLT text,
											starship_class text,
											created text,
											edited text,
											url text);''')

    cur.execute('''CREATE TABLE IF NOT EXISTS swapi.species	(
											species_id int primary key,
											name text,
											classification text,
											designation text,
											average_height text,
											average_lifespan text,
											hair_colors text,
											skin_colors text,
											eye_colors text,
											homeworld text,
											language text,
											url text,
											created text,
											edited text);''')


    cur.execute('''CREATE TABLE IF NOT EXISTS swapi.films   (
											film_id int primary key,
											title text,
											episode_id int,
											opening_crawl text,
											director text,
											producer text,
											release_date text,
											url text,
											created text,
											edited text);''')

    cur.execute('''CREATE TABLE IF NOT EXISTS swapi.people_film 	(people_id int references swapi.people(people_id), film_id int references swapi.films(film_id));
                    CREATE TABLE IF NOT EXISTS swapi.people_starship (people_id int references swapi.people(people_id), starship_id int references swapi.starships(starship_id));
                    CREATE TABLE IF NOT EXISTS swapi.people_planet 	(people_id int references swapi.people(people_id), planet_id int references swapi.planets(planet_id));
                    CREATE TABLE IF NOT EXISTS swapi.people_species 	(people_id int references swapi.people(people_id), species_id int references swapi.species(species_id));
                    CREATE TABLE IF NOT EXISTS swapi.people_vehicle 	(people_id int references swapi.people(people_id), vehicle_id int references swapi.vehicles(vehicle_id));	

                    CREATE TABLE IF NOT EXISTS swapi.film_planet 	(film_id int references swapi.films(film_id), planet_id int references swapi.planets(planet_id));
                    CREATE TABLE IF NOT EXISTS swapi.film_starship 	(film_id int references swapi.films(film_id), starship_id int references swapi.starships(starship_id));
                    CREATE TABLE IF NOT EXISTS swapi.film_vehicle 	(film_id int references swapi.films(film_id), vehicle_id int references swapi.vehicles(vehicle_id));
                    CREATE TABLE IF NOT EXISTS swapi.film_species 	(film_id int references swapi.films(film_id), species_id int references swapi.species(species_id));'''
    )

    close(conn, cur)


def delete_swapi_db():
	conn, cur = connect()
	try:
		cur.execute('''	
					drop table swapi.film_planet;
					drop table swapi.film_species;
					drop table swapi.film_starship;
					drop table swapi.film_vehicle;
					drop table swapi.people_planet;
					drop table swapi.people_film;
					drop table swapi.people_species;
					drop table swapi.people_starship ;
					drop table swapi.people_vehicle ;
					drop table swapi.planet_species;''')

	cur.execute('''	drop table swapi.films;
					drop table swapi.people;
					drop table swapi.planets;
					drop table swapi.species;
					drop table swapi.starships ;
					drop table swapi.vehicles;''')
	except:
		pass
	close(conn, cur)
	


