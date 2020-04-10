import psycopg2 as ps2

#Ouverture de la connexion avec la base de données
connection = ps2.connect(host= "127.0.0.1", port="5432", dbname="insee", user="martindrance", password="dcf1e82603")
cursor = connection.cursor()

#QUESTION 1
def liste_regions():
    cursor.execute("select libelle from regions;")
    print ("Les régions contenues dans la base de données sont :")
    for reg in cursor.fetchall():
        print(reg[0])

#QUESTION 2
def liste_departements():
    cursor.execute("select libelle from departements;")
    print ("Les departements contenus dans la base de données sont :")
    for dep in cursor.fetchall():
        print(dep[0])

#QUESTION 3
def choisir_region():
    choix = input ("Choisissez une région pour afficher ses informations : ")
    cursor.execute("select * from regions where libelle = %s;", (choix,))
    for infos in cursor.fetchall():
        print("reg :", infos[0])
        print("cheflieu :", infos[1])
        print("TNCC :", infos[2])
        print("NCC :", infos[3])
        print("NCCENR :", infos[4])

#QUESTION 4
def dep_infos():
    choix_dep = input ("Choisissez un département pour afficher ses informations : ")
    choix_type = input ("Souhaitez-vous consulter les données sociales ou environnementales (s/e) : ")
    if choix_type == "s":
        cursor.execute("select * from departementsocial where dep = %s;", (choix_dep,))
        for infos in cursor.fetchall():
            print("Numéro : ", infos[0])
            print("Département : ", infos[1])
            print("Esp. vie hommes 2015 : ", infos[2])
            print("Esp. vie hommes 2010 : ", infos[3])
            print("Esp. vie femmes 2015 : ", infos[4])
            print("Esp. vie femmes 2015 : ", infos[5])
            print("% population à plus de 7min des services de santé : ", infos[6])
            print("% population zone inondable 2013 : ", infos[7])
            print("% population zone inondable 2008 : ", infos[8])
    elif choix_type == "e":
        cursor.execute("select * from departementenvironnement where dep = %s;", (choix_dep,))
        for infos in cursor.fetchall():
            print("Numéro : ", infos[0])
            print("Département : ", infos[1])
            print("% valorisation matière orga 2013 : ", infos[2])
            print("% valorisation matière orga 2009 : ", infos[3])
            print("% Surfaces artificialisées 2012 : ", infos[4])
            print("% Surfaces artificialisées 2006 : ", infos[5])
            print("% Agriculture bio sur la surface totale 2016 : ", infos[6])
            print("% Agriculture bio sur la surface totale 2010 : ", infos[7])
            print("Prod. granulats (tonnes) 2014 : ", infos[8])
            print("Prod. granulats (tonnes) 2009 : ", infos[9])
            print("% Eolien 2015 : ", infos[10])
            print("% Eolien 2010 : ", infos[11])
            print("% Photovoltaique 2015 : ", infos[12])
            print("% Photovoltaique 2010 : ", infos[13])
            print("% Autres énergies 2015 : ", infos[14])
            print("% Autres énergies 2010 : ", infos[15])


#QUESTION 5
def energie_info():
    choix = input ("Choisissez un type d'énergie (eolien, photovoltaique ou autres) à consulter (e/p/a) : ")
    while choix not in ["e", "p", "a"]:
        choix = input("Mauvaise saisie, choisissez entre e/p/a : ")
    if choix == "e":
        cursor.execute("select dep, eolien10, eolien15 from departementenvironnement where eolien10 < eolien15 order by eolien15 - eolien10 desc")
        for i in cursor:
            print ("Département : ",i[0])
            print ("Eolien 2010 : ", i[1])
            print ("Eolien 2015 :", i[2])
            print ("Augmentation de :", round(i[2]-i[1], 2), "points\n")
    elif choix == "p":
        cursor.execute("select dep, voltaique10, voltaique15 from departementenvironnement where voltaique10 < voltaique15 order by voltaique15 - voltaique10 desc")
        for i in cursor:
            print ("Département : ",i[0])
            print ("Photovoltaique 2010 : ", i[1])
            print ("Photovoltaique 2015 :", i[2])
            print ("Augmentation de :", round(i[2]-i[1], 2), "points\n")
    elif choix == "a":
        cursor.execute("select dep, autre10, autre15 from departementenvironnement where autre10 < autre15 order by autre15 - autre10 desc")
        for i in cursor:
            print ("Département : ",i[0])
            print ("Autres énergies 2010 : ", i[1])
            print ("Autres énergies 2015 :", i[2])
            print ("Augmentation de :", round(i[2]-i[1], 2), "points\n")

#QUESTION 6
def prod_granulats():
    cursor.execute("""select D1.nccenr from departements D1 where D1.reg in 
                (select R1.reg from departements D1 join departementenvironnement E1 on D1.nccenr = E1.dep join regions R1 on R1.reg = D1.reg 
                group by R1.reg having sum(prodgranu14) > 25000000);""")
    print ("Départements dont la région a produit plus de 25 000 000 tonnes de granulats en 2014 :")
    for i in cursor.fetchall():
        print("Département : ",i[0])
        # print("Production : ", i[1])

#QUESTION 7
def max_eolienne():
    cursor.execute("""select dep, eolien15 from departementenvironnement where eolien15 is not null order by eolien15 desc limit 5""")
    print ("Les 5 départements avec le plus grand taux d'énergie eolienne en 2015 sont :")
    for i in cursor:
        print(i[0], "avec", i[1],"%")

max_eolienne()