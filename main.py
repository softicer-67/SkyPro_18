import psycopg2
from psycopg2 import Error


def connect_db():
    try:
        connection = psycopg2.connect(user="postgres",
                                      # пароль, который указали при установке PostgreSQL
                                      password='autobahan',
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

    CREATE TABLE  IF NOT EXISTS outcome_subtype(
        id SERIAL PRIMARY KEY,
        outcome_subtype VARCHAR(255)
    );
    
    CREATE TABLE IF NOT EXISTS outcome_type(
        id SERIAL PRIMARY KEY,
        outcome_type VARCHAR(255)
    );
    
    CREATE TABLE IF NOT EXISTS breed(
        id SERIAL PRIMARY KEY,
        breed VARCHAR(100)
    );
    
    CREATE TABLE IF NOT EXISTS color(
        id SERIAL PRIMARY KEY,
        color VARCHAR(100)
    );
    
    CREATE TABLE IF NOT EXISTS animal_type(
        id SERIAL PRIMARY KEY,
        animal_type VARCHAR(100)
    );
    
    CREATE TABLE IF NOT EXISTS animals(
        animal_id VARCHAR(100) PRIMARY KEY NOT NULL,
        fk_animal_type INT REFERENCES animal_type(id),
        name VARCHAR(50),
        fk_breed INT REFERENCES breed(id),
        fk_color1 INT REFERENCES color(id) NOT NULL,
        fk_color2 INT REFERENCES color(id),
        date_of_birth varchar(100)
    );
    
    CREATE TABLE IF NOT EXISTS shelter_info(
        id INT PRIMARY KEY,
        age_upon_outcome VARCHAR(50),
        animal_id VARCHAR(100) REFERENCES animals(animal_id),
        fk_id_outcome_subtype INT REFERENCES outcome_subtype(id),
        fk_id_outcome_type INT REFERENCES outcome_type(id),
        outcome_month INT,
        outcome_year INT
    );
    """
    cursor.execute(req)


def write_outcome_subtype(cursor):
    req = """
    INSERT INTO outcome_subtype (outcome_subtype)
        SELECT DISTINCT outcome_subtype FROM main_animals
        WHERE outcome_subtype <> ''
    """
    cursor.execute(req)


def write_outcome_type(cursor):
    req = f"""
    INSERT INTO outcome_type (outcome_type)
        SELECT DISTINCT outcome_type FROM main_animals
        WHERE outcome_type <> ''
    """
    cursor.execute(req)


def write_breed(cursor):
    req = f"""
    INSERT INTO breed (breed)
        SELECT DISTINCT breed FROM main_animals
        WHERE breed <> ''
    """
    cursor.execute(req)


def write_color(cursor):
    req = f"""
        INSERT INTO color (color)
        SELECT DISTINCT color1 FROM main_animals
        WHERE color1 <> ''
    """
    cursor.execute(req)


def write_animal_type(cursor):
    req = """
    INSERT INTO animal_type (animal_type)
        SELECT DISTINCT animal_type FROM main_animals
        WHERE animal_type <> ''
    """
    cursor.execute(req)


def write_animals(cursor):
    req = f"""
    INSERT INTO animals
        SELECT animal_id, animal_type.id, name, breed.id, c1.id, c2.id, date_of_birth 
            FROM main_animals
            LEFT JOIN animal_type ON main_animals.animal_type = animal_type.animal_type
            LEFT JOIN breed ON main_animals.breed = breed.breed
            LEFT JOIN color as c1 ON main_animals.color1 = c1.color
            LEFT JOIN color as c2 ON main_animals.color2 = c2.color
        GROUP BY animal_id, animal_type.id, name, breed.id, c1.id, c2.id, date_of_birth 
    """
    cursor.execute(req)


def write_shelters(cursor):
    req = f"""
    INSERT INTO shelter_info
            SELECT index, age_upon_outcome, animal_id, outcome_subtype.id, outcome_type.id, outcome_month, outcome_year
            FROM main_animals
            LEFT JOIN outcome_subtype ON main_animals.outcome_subtype = outcome_subtype.outcome_subtype
            LEFT JOIN outcome_type ON main_animals.outcome_type = outcome_type.outcome_type
    """
    cursor.execute(req)


def make_boss(cursor):
    req = """
       CREATE ROLE user WITH PASSWORD '123456';
       GRANT CONNECT ON DATABASE aaa TO user;
       GRANT USAGE ON ALL TABLES IN SCHEMA animals_db TO user;
       GRANT SELECT ON ALL TABLES IN SCHEMA aaa TO user

       CREATE ROLE user_boss WITH PASSWORD '654321';
       GRANT CONNECT ON DATABASE animals_db TO user_boss; 
       GRANT INSERT ON ALL TABLES IN SCHEMA aaa TO user_boss;
       GRANT UPDATE ON ALL TABLES IN SCHEMA aaa TO user_boss;
       """

    # посмотреть всех пользователей
    # select * from pg_user
    cursor.execute(req)


if __name__ == "__main__":
    connection = connect_db()
    cursor = connection.cursor()
    create_table(cursor)

    write_outcome_subtype(cursor)
    write_outcome_type(cursor)
    write_breed(cursor)
    write_color(cursor)
    write_animal_type(cursor)
    write_animals(cursor)
    write_shelters(cursor)

    # make_boss(cursor)

