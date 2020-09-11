# Scrapping SWAPI using six degrees of separation approach.
Interacting with the Star Wars API

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
