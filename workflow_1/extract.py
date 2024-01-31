import xml.etree.ElementTree as ET
import pandas as pd
import json

# Analyser le fichier XML
arbre = ET.parse('./data/sales.xml')
racine = arbre.getroot()

# Initialiser des listes vides pour stocker les données
liste_de_donnees = []

# Parcourir les éléments 'table'
for elem_table in racine.findall('.//table'):
    dict_ligne = {'table_name': elem_table.get('name')}  

    # Parcourir les éléments 'column'
    for elem_colonne in elem_table.findall('.//column'):
        dict_ligne[elem_colonne.get('name')] = elem_colonne.text  

    liste_de_donnees.append(dict_ligne)

# Convertir la liste de dictionnaires en DataFrame Pandas
df_xml = pd.DataFrame(liste_de_donnees)

df_xml = df_xml.drop(columns=['table_name'])

# Charger le fichier JSON dans un objet Python
with open('./data/sales.json') as fichier_json:
    donnees_json = json.load(fichier_json)

# Convertir en DataFrame Pandas
df_json = pd.DataFrame(donnees_json)

# Lire le fichier CSV
df_csv = pd.read_csv('./data/sales.csv')

# Concaténer les DataFrames
combined_df = pd.concat([df_xml, df_json, df_csv], axis=0, ignore_index=True)

# Supprimer les colonnes 'created_at' et 'updated_at'
combined_df = combined_df.drop('created_at', axis=1)
combined_df = combined_df.drop('updated_at', axis=1)

# Enregistrer dans un fichier CSV
combined_df.to_csv('./data/combined_data.csv', index=False)
