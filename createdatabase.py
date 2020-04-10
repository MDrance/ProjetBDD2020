import pandas as pd
import numpy as np
import csv
import psycopg2 as ps2

try:
    #Ouverture de la connexion avec la base de données
    connection = ps2.connect(host= "127.0.0.1", port="5432", dbname="myDB", user="myID", password="myPW")
    cursor = connection.cursor()

    #Création/remplissage de le table departements
    cursor.execute("""create table departements(
        dep varchar(10) primary key, 
        reg varchar(10) not null, 
        cheflieu varchar(10) not null, 
        tncc varchar(10) not null, 
        ncc varchar(50) not null, 
        nccenr varchar(50) not null, 
        libelle varchar(50) not null)""")
    with open('departement2020.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute("insert into departements values (%s, %s, %s, %s, %s, %s, %s)", row)

    #Création/remplissage de la table regions
    cursor.execute("""create table regions(
        reg varchar(10) primary key, 
        cheflieu varchar(10) not null, 
        tncc varchar(10) not null, 
        ncc varchar(50) not null, 
        nccenr varchar(50) not null, 
        libelle varchar(50) not null)""")
    with open('region2020.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute("insert into regions values (%s, %s, %s, %s, %s, %s)", row)

    #Création/remplissage de la table regionsocial
    cursor.execute("""create table regionsocial(
        num varchar(10) primary key, 
        reg varchar(50), 
        pauvrete float, 
        jeunesnoninseres2014 float, 
        jeunesjoninseres2009 float, 
        poidssocial float)""")
    xls = pd.ExcelFile('DD-indic-reg-dep_janv2018.xls')
    df1 = pd.read_excel(xls, sheet_name= "Social", usecols= "A:F", skiprows= 3, nrows= 21, header= None)
    df1.replace(["nd", "nd ", "nc", "nc "], np.NaN, inplace = True)
    for i in range (len(df1)):
        rows_region = df1.iloc[i]
        cursor.execute("insert into regionsocial values (%s, %s, %s, %s, %s, %s)", rows_region) 
        
    #Création/remplissage de la table departementsocial
    cursor.execute("""create table departementsocial(
        num varchar(10) primary key, 
        dep varchar(50), 
        esph15 float, 
        esph10 float, 
        espf15 float, 
        espf10 float,
        pop7min float,
        popinon13 float,
        popinon08 float)""")
    xls = pd.ExcelFile('DD-indic-reg-dep_janv2018.xls')
    df2 = pd.read_excel(xls, sheet_name= "Social", usecols= "A:I", skiprows= 28, nrows= 104 , header= None)
    df2.replace(["nd", "nd ", "nc", "nc "], np.NaN, inplace = True)
    for i in range (len(df2)):
        rows_departement = df2.iloc[i]
        cursor.execute("insert into departementsocial values (%s, %s, %s, %s, %s, %s, %s, %s, %s)", rows_departement)

    #Création/remplissage de la table departementenvironnement
    cursor.execute("""create table departementenvironnement(
        num varchar(10) primary key, 
        dep varchar(50), 
        valoorga13 float, 
        valoorda09 float, 
        surfacearti12 float, 
        surfacearti06 float,
        agribio16 float,
        agribio10 float,
        prodgranu14 float,
        prodgranu09 float,
        eolien15 float,
        eolien10 float,
        voltaique15 float,
        voltaique10 float,
        autre15 float,
        autre10 float)""")
    xls = pd.ExcelFile('DD-indic-reg-dep_janv2018.xls')
    df3 = pd.read_excel(xls, sheet_name= "Environnement", usecols= "A:P", skiprows= 3, nrows= 104 , header= None)
    df3.replace(["nd", "nd ", "nc", "nc "], np.NaN, inplace = True)
    for i in range (len(df3)):
        rows_departement_env = df3.iloc[i]
        cursor.execute("insert into departementenvironnement values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", rows_departement_env)

    connection.commit()

except(Exception, ps2.Error) as error:     # gestion des erreurs
    print("Erreur lors de la création de la base de donnée :", error)

finally:
    if(connection):
        cursor.close()
        connection.close()
        print("Base de donnée prête à l'utilisation.")