import psycopg2 as ps2
import sys

#Ouverture de la connexion avec la base de données
connection = ps2.connect(host= "127.0.0.1", port="5432", dbname="insee", user="martindrance", password="dcf1e82603")
cursor = connection.cursor()

#QUESTION 1
def liste_regions():
    cursor.execute("select libelle from regions;")
    print ("Les régions contenues dans la base de données sont : ")
    for reg in cursor.fetchall():
        print(reg[0])

#QUESTION 2
def liste_departements():
    cursor.execute("select libelle from departements;")
    print ("Les departements contenus dans la base de données sont : ")
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
        cursor.execute("select dep, eolien10, eolien15 from departementenvironnement where eolien10 < eolien15 order by eolien15 - eolien10 desc;")
        for i in cursor:
            print ("Département : ",i[0])
            print ("Eolien 2010 : ", i[1])
            print ("Eolien 2015 :", i[2])
            print ("Augmentation de :", round(i[2]-i[1], 2), "points\n")
    elif choix == "p":
        cursor.execute("select dep, voltaique10, voltaique15 from departementenvironnement where voltaique10 < voltaique15 order by voltaique15 - voltaique10 desc;")
        for i in cursor:
            print ("Département : ",i[0])
            print ("Photovoltaique 2010 : ", i[1])
            print ("Photovoltaique 2015 :", i[2])
            print ("Augmentation de :", round(i[2]-i[1], 2), "points\n")
    elif choix == "a":
        cursor.execute("select dep, autre10, autre15 from departementenvironnement where autre10 < autre15 order by autre15 - autre10 desc;")
        for i in cursor:
            print ("Département : ",i[0])
            print ("Autres énergies 2010 : ", i[1])
            print ("Autres énergies 2015 :", i[2])
            print ("Augmentation de :", round(i[2]-i[1], 2), "points\n")

#QUESTION 6
def prod_granulats():
    cursor.execute("""select D1.libelle from departements D1 where D1.reg in 
                (select R1.reg from departements D1 join departementenvironnement E1 on D1.nccenr = E1.dep join regions R1 on R1.reg = D1.reg 
                group by R1.reg having sum(prodgranu14) > 25000000);""")
    print ("Départements dont la région a produit plus de 25 000 000 tonnes de granulats en 2014 :")
    for i in cursor.fetchall():
        print("Département : ",i[0])

#QUESTION 7
def max_eolienne():
    cursor.execute("select dep, eolien15 from departementenvironnement where eolien15 < 101 order by eolien15 desc limit 5;")
    print ("Les 5 départements avec le plus grand taux d'énergie eolienne en 2015 sont :")
    for i in cursor:
        print(i[0], "avec", i[1], "%")

#QUESTION 8
def faible_taux_orga():
    cursor.execute("""select R1.libelle from regions R1 join departements D1 on R1.reg = D1.reg join 
                    departementenvironnement E1 on D1.libelle = E1.dep order by valoorga13 asc limit 1;""")
    for i in cursor.fetchall():
        print("Le département qui à le plus faible taux de valorisation matière et organique en 2013 se situe en", i[0])

#QUESTION 9
def agribiosante():
    cursor.execute("""select E1.agribio16, E1.dep, S1.pop7min from departementenvironnement E1 join
                     departementsocial S1 on E1.dep = S1.dep where S1.pop7min < 101 order by S1.pop7min desc limit 1;""")
    for i in cursor:
        print("Part de l'agriculture biologique dans le departement ayant le pourcentage le plus élevé de population éloigné de 7min des services de santé en 2016 :")
        print("Departement :",i[1])
        print("Population à plus de 7min des services de santé :", i[2], "%")
        print("Agriculture biologique :", i[0], "%")

#QUESTION 10
def pauvrejeunes():
    cursor.execute("""select reg, jeunesnoninseres2014, pauvrete from regionsocial where jeunesnoninseres2014 > 30 and jeunesnoninseres2014 < 101 and pauvrete < 101;""")
    for i in cursor:
        print("Région :",i[0])
        print("Jeunes non insérés :", round(i[1], 2), "%")
        print("Taux pauvreté 2014 :", round(i[2], 2))
        
#QUESTION 11
def ecosoc():
    cursor.execute("""select R1.libelle, R2.poidssocial from departementenvironnement E1 join departements D1 on D1.nccenr = E1.dep join regions R1 on R1.reg = D1.reg join
    regionsocial R2 ON R1.nccenr = R2.reg group by R1.libelle, R2.poidssocial having (sum(E1.voltaique15)/count(E1.dep1)) > 10 and
    (sum(E1.agribio16)/count(E1.dep)) > 5;""")
    for i in cursor:
        print ("Régions : ", i[0])
        print ("Poids de l'économie sociale :", i[1])

#MENU
def menu():
    print("\n*** Menu ***")
    print("0: Quitter")
    print("1: Afficher les régions")
    print("2: Afficher les départements")
    print("3: Afficher les informations d'une région")
    print("4: Afficher les informations sociales ou environnementales d'un département")
    print("5: Afficher la progression d'un type d'énergie d'un département en fonction du type d'énergie")
    print("6: Afficher les départements dont la région a eu une production de granulats supérieure à 25 millions de tonnes en 2014")
    print("7: Afficher les 5 départements avec le plus grand taux d’énergie éolienne comme source de la puissance électrique en 2015")
    print("8: Afficher la région qui a le departement avec le plus faible taux de valorisation matière et organique en 2013")
    print("9: Afficher la part de l'agriculture bio du département dont le pourcentage de personnes à plus de 7 minutes des services de santé est le plus grand")
    print("10: Afficher les taux de pauvreté en 2014 dans les régions où la part de jeunes non insérés est supérieure à 30%")
    print("""11: Afficher le poids de l'économie sociale dans les emplois salariés de la région dont la source de la puissance électrique en énergies renouvelables
    provenait à au moins 10% de l’énergie photovoltaïque et dont la part de l’agriculture biologique dans la surface agricole totale était d’au moins 5%\n""")

#MAIN
while(True):
    menu()
    choix = int(input("Quelle action effectuer ?\n"))
    if(choix == 0):
        sys.exit(0)
    elif(choix == 1):
        liste_regions()
    elif(choix == 2):
        liste_departements()
    elif(choix == 3):
        choisir_region()
    elif(choix == 4):
        dep_infos()
    elif(choix == 5):
        energie_info()
    elif(choix == 6):
        prod_granulats()
    elif(choix == 7):
        max_eolienne()
    elif(choix == 8):
        faible_taux_orga()
    elif(choix == 9):
        agribiosante()
    elif(choix == 10):
        pauvrejeunes()
    elif(choix == 11):
        ecosoc()