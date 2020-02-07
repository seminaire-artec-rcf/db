import psycopg2
import sys
import csv
import os
from dotenv import load_dotenv
from os.path import join, dirname
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT



def connect():



  load_dotenv()
  #grab the environment variables
  user = os.environ.get("USRNM")
  pwd = os.environ.get("PASSWORD")
  db = os.environ.get("DBNAME")
  host = os.environ.get("HOST")

  #check if the DB already exists
  #if not, make it
  c1 = psycopg2.connect(user=user, password=pwd, host=host)
  cur1 = c1.cursor()
  try:
      c2 = psycopg2.connect(dbname=db, user=user, password=pwd, host=host)
  except psycopg2.OperationalError:
      c1.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
      cur1.execute('CREATE DATABASE {};'.format(db))
      c2 = psycopg2.connect(dbname=db, user=user, password=pwd, host=host)

  return c2



def insert_values(target, connection):
    c = connection.cursor()
    with open('./tableaux/' + target+ '.csv', 'r') as f:
        # Notice that we don't need the `csv` module.
        next(f) # Skip the header row.
        c.copy_from(f, target, sep="|", null='')
        connection.commit()



def main():
    cnx = connect()
    tables = [file[:-4] for file in os.listdir('./tableaux')]
    cur = cnx.cursor()

    for name in tables:
        cur.execute("DROP TABLE IF EXISTS {} CASCADE;".format(name))

    cnx.commit()
    '''
    Make all the tables
    There is surely a much better/shorter/more automated way to do this, but for the sake of clarity/just getting things done, here we are
    '''
    cur.execute("""
        CREATE TABLE auteurs (
            id int PRIMARY KEY,
            prenom varchar(255),
            nom varchar(255),
            pseudonyme varchar(255),
            naissance int,
            mort int,
            notes_bnf text,
            pseudos_alternatifs text,
            liens_info text[],
            liens_icono text[],
            genre varchar(2)

        );
    """)
    insert_values("auteurs", cnx)


    cur.execute("""
        CREATE TABLE comediens (
            id int PRIMARY KEY,
            pseudonyme varchar(128),
            numero_pseudo int,
            titre varchar(128),
            prenom varchar(128),
            nom varchar(128),
            alias varchar(128),
            statut varchar(64),
            entree int,
            societariat int,
            depart int,
            debut int[],
            dates varchar(128),
            notes text
        );
    """)

    insert_values("comediens", cnx)

    cur.execute("""
        CREATE TABLE registres (
            id int PRIMARY KEY,
            "date" date,
            jour varchar(64),
            saison varchar(128),
            recettes int,
            semainier varchar(128),
            notes text,
            ouverture boolean,
            cloture boolean,
            page_de_gauche varchar(255)

        );
    """)

    insert_values("registres", cnx)

    #so that we can start with foreign key constraints
    cnx.commit()

    #author ids should have a foreign key constraint but it's not implemented for array elements
    #I think I understand why they did the authorships table now....
    cur.execute("""
        CREATE TABLE pieces (
            id int PRIMARY KEY,
            id_auteur int[],
            titre text,
            genre varchar(128),
            actes int,
            prose_vers varchar(64),
            prologue boolean,
            musique_danse_machine boolean,
            titre_alternatif text,
            date_de_creation date

        );
    """)

    insert_values("pieces", cnx)

    cur.execute("""
        CREATE TABLE images_registres (
            id int PRIMARY KEY,
            id_registre int REFERENCES registres(id),
            url varchar(255),
            orientation varchar(64)
        );
    """)

    insert_values("images_registres", cnx)

    #ugh again multiple authorships
    cur.execute("""
        CREATE TABLE documents_lagrange (
            id varchar(64),
            "type" varchar(128),
            titre text,
            titre_alternatif varchar(128),
            soustitre text,
            url varchar(128),
            id_auteur int REFERENCES auteurs(id),
            PRIMARY KEY(id, id_auteur)
        );
    """)

    insert_values("documents_lagrange", cnx)

    #the ticket sale table is WILD so no keys on this one
    cur.execute("""
        CREATE TABLE ventes (
            id_registre int REFERENCES registres(id),
            id_place int,
            description varchar(128),
            loge boolean,
            billets_vendus int,
            prix_par_billet int,
            recettes int
        );
    """)

    insert_values("ventes", cnx)

    #for a second round of dependancies
    cnx.commit()

    cur.execute("""
        CREATE TABLE pieces_registres (
            id int PRIMARY KEY,
            id_registre int REFERENCES registres(id),
            id_piece int REFERENCES pieces(id),
            debut boolean,
            reprise boolean,
            ordre int,
            gratuit boolean,
            notes_public text,
            notes_lieu text,
            notes_representation text
        );
    """)

    insert_values("pieces_registres", cnx)

    #clean up empty strings
    #would have been nice to do this on import but the copy method is finnicky

    #auteurs
    cur.execute('update auteurs set pseudos_alternatifs = null where pseudos_alternatifs = \'""\';')

    #comediens
    cur.execute('update comediens set notes = null where notes = \'""\';')
    cur.execute('update comediens set dates = trim(dates, \'"\')')

    #documents_lagrange
    cur.execute('update documents_lagrange set titre_alternatif = null where titre_alternatif = \'""\';')
    cur.execute('update documents_lagrange set soustitre = null where soustitre = \'""\';')

    #registres
    cur.execute('update registres set saison = trim(saison, \'"\');')
    cur.execute('update registres set semainier = null where semainier = \'""\';')
    cur.execute('update registres set notes = trim(notes, \'"\');')
    cur.execute("update registres set notes = null where notes = '';")

    cnx.commit()


main()
