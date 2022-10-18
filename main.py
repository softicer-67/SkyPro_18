from settings import DB_PASS
import psycopg2
from psycopg2 import Error


def connect_db():
    try:
        # Подключиться к существующей базе данных
        connection = psycopg2.connect(user="postgres",
                                      # пароль, который указали при установке PostgreSQL
                                      password=DB_PASS,
                                      host="127.0.0.1",
                                      port="5432",
                                      database="aaa")
        connection.autocommit = True
        print("Connection DONE")
        return connection
    except Error as err:
        print("Connection Failed", err)
        return False


def create_table(cursor):
    req = """ 

    CREATE TABLE IF NOT EXISTS animal_type (
        id_type integer NOT NULL,
        animal_type character varying(50) NOT NULL,
        CONSTRAINT animal_type_pkey PRIMARY KEY (id_type)
    );

    CREATE TABLE IF NOT EXISTS breed_dict (
        id_breed integer NOT NULL,
        breed_type character varying(100) NOT NULL,
        CONSTRAINT breed_dict_pkey PRIMARY KEY (id_breed)
    );
    
    CREATE TABLE IF NOT EXISTS colour_dict (
        id_colour integer NOT NULL,
        name_colour character varying(100),
        CONSTRAINT colour_dict_pkey PRIMARY KEY (id_colour)
    );
    
    CREATE TABLE IF NOT EXISTS main_animals (
        index integer,
        age_upon_outcome text,
        animal_id text,
        name text,
        date_of_birth text,
        outcome_month integer,
        outcome_year integer,
        id_breed integer,
        animal integer,
        id_colour integer,
        id_outcome_type integer,
        id_outcome_subtype integer
    );
    
    CREATE TABLE IF NOT EXISTS outcome_subtype (
        id_outcome_subtype integer NOT NULL,
        name_outcome_subtype character varying(100) NOT NULL,
        CONSTRAINT outcome_subtype_pkey PRIMARY KEY (id_outcome_subtype)
    );
    
    CREATE TABLE IF NOT EXISTS outcome_type (
        id_outcome_type integer NOT NULL,
        name_outcome_type character varying(100) NOT NULL,
        CONSTRAINT outcome_type_pkey PRIMARY KEY (id_outcome_type)
    );
    
    CREATE TABLE IF NOT EXISTS shelter_info (
        id_shelter_info integer NOT NULL,
        fk_animal_id integer NOT NULL,
        fk_id_outcome_subtype integer NOT NULL,
        outcome_month integer,
        outcome_year integer,
        fk_outcome_type integer,
        age_upon_outcome character varying(20),
        CONSTRAINT shelter_info_pkey PRIMARY KEY (id_shelter_info)
    );
    
    CREATE TABLE IF NOT EXISTS animal_dict (
        animal_id integer NOT NULL,
        fk_animal_type integer NOT NULL,
        name character varying(100),
        fk_breed integer,
        fk_color1 integer,
        fk_color2 integer,
        date_of_bird timestamp with time zone,
        CONSTRAINT animal_dict_pkey PRIMARY KEY (animal_id)
    );
    
    SELECT age_upon_outcome, animal_id, name, date_of_birth, outcome_month, outcome_year, breed_type, name_outcome_type, name_outcome_subtype, animal_type, name_colour FROM main_animals 
    JOIN breed_dict on main_animals.id_breed = breed_dict.id_breed
    JOIN animal_type on main_animals.animal = animal_type.id_type
    JOIN colour_dict on main_animals.id_colour = colour_dict.id_colour
    JOIN outcome_type on main_animals.id_outcome_type = outcome_type.id_outcome_type
    JOIN outcome_subtype on main_animals.id_outcome_subtype = outcome_subtype.id_outcome_subtype

    """
    cursor.execute(req)
    

if __name__ == "__main__":
    connection = connect_db()
    cursor = connection.cursor()
    create_table(cursor)
