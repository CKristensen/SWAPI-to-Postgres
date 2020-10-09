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

May the force be with you!

Carl K

Special thank to Snorre for this great idea


To connect to the Database you can use DBeaver (dbeaver.io) and use the following credentials:
----------------------------
PASSWORD: "pass"

USER: postgres

DBNAME: postgres

HOSTD: localhost:5432
