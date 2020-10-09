# Scrapping SWAPI using six degrees of separation approach.
Interacting with the Star Wars API

![swlogo](/swlogo.png)

Do you want to have a Star Wars Database inside a Docker Postgres Container in your computer?
Then look no further!
-------------------
To make it work simply clone the repo and run:
docker-compose up -d --build

It will run 2 containers, one with a postgres Database and another with a python script that will scrappe the SWAPI.
------------------

In this mini-project I wanted to get all information from the Star Wars API using a six degrees of seperation approach.
The program will start with Luke Skywalker and get all his connections to films, starships, planets, etc.
Then for each connection I will get their connections and so on and so forth.
This way I will end up with all the elements and their details from the Star Wars API with needing to know them beforehand.

The result of this project is a PostgreSQL Database with all the films/characters/species/vehicles/starships of the SWAPI and all their connections!

If you do not have a postgres database I would suggest checking this turtorial out so you can install it and create your personal Star Wars Database :D
https://www.postgresqltutorial.com/install-postgresql/

To connect to your database edit the db_operations.py file line 6 to 9.

May the force be with you!

Carl K
