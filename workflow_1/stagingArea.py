import pyodbc
import csv

# Paramètres de connexion à la base de données SQL Server
serveur = 'DESKTOP-J1LJSLQ\SQLEXPRESS'
base_de_donnees = 'test2'
utilisateur = 'taha'
mot_de_passe = 'tahataha'

# Établir une connexion avec le serveur SQL
connexion = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};\
                          SERVER='+serveur+';\
                          DATABASE='+base_de_donnees+';\
                          UID='+utilisateur+';\
                          PWD='+ mot_de_passe)

curseur = connexion.cursor()

# Chemin du fichier CSV
chemin_fichier_csv = r'C:\Users\havet\Desktop\data\combined_data.csv'
chemin_fichier_csv = r'C:\Users\havet\Desktop\workflow_1\data\combined_data.csv'

# Lire le fichier CSV et insérer chaque ligne dans la table SQL Server
with open(chemin_fichier_csv, 'r') as fichier:
    lecteur_csv = csv.reader(fichier)
    entete = next(lecteur_csv)  # Ignorer la ligne d'en-tête

    # Supposer que votre table a les mêmes noms de colonnes que l'en-tête du CSV
    definition_colonnes = ', '.join([f'{colonne} VARCHAR(255)' for colonne in entete])
    
    # Vérifier si la table StagingTable existe et la supprimer si c'est le cas
    if curseur.tables(table='StagingTable').fetchone():
        curseur.execute('DROP TABLE StagingTable')
        connexion.commit()
        print("Table de staging supprimée.")

    # Créer la table StagingTable
    requete_creation_table_staging = f'''
        CREATE TABLE StagingTable (
            {definition_colonnes}
        )
    '''

    curseur.execute(requete_creation_table_staging)
    connexion.commit()
    print("Table de staging créée.")

    # Insérer des données dans StagingTable
    requete_insertion = f"INSERT INTO StagingTable ({', '.join(entete)}) VALUES ({', '.join(['?'] * len(entete))})"
    for ligne in lecteur_csv:
        curseur.execute(requete_insertion, ligne)
        connexion.commit()

print("Données du fichier CSV insérées dans la table StagingTable avec succès.")

# Fermer le curseur et la connexion
curseur.close()
connexion.close()
