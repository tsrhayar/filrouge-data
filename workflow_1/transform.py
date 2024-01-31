import pandas as pd
from faker import Faker
from cryptography.fernet import Fernet
import random

# Charger les données
chemin_fichier = './data/combined_data.csv'
donnees = pd.read_csv(chemin_fichier)

# Remplir les valeurs manquantes dans 'product_name'
donnees['product_name'].fillna("Category:" + donnees['category'] + donnees['subcategory'], inplace=True)

# Catégoriser et encoder les colonnes non sensibles
donnees['user_id'] = donnees['name'].astype('category').cat.codes + 1
donnees['product_id'] = donnees['product_name'].astype('category').cat.codes + 1
donnees['date_purchase_id'] = donnees['date_purchase'].astype('category').cat.codes + 1
donnees['comment_id'] = donnees['comment'].astype('category').cat.codes + 1

# Gérer les valeurs manquantes et incorrectes
donnees['price'] = donnees['price'].apply(lambda x: abs(x) if x < 0 else x)
colonnes_date = ['date_purchase']
for col in colonnes_date:
    donnees[col] = pd.to_datetime(donnees[col], errors='coerce')

def generer_age_aleatoire():
    return random.randint(18, 70)

# Appliquer la fonction à la colonne 'age'
donnees['age'] = donnees['age'].apply(lambda x: generer_age_aleatoire() if x < 18 or x > 120 or pd.isnull(x) else x)

# Convertir les colonnes 'price' et 'quantity' en types numériques
donnees['price'] = pd.to_numeric(donnees['price'], errors='coerce')
donnees['quantity'] = pd.to_numeric(donnees['quantity'], errors='coerce')

# Calculer la colonne 'total' en multipliant 'price' et 'quantity'
donnees['total'] = donnees['price'] * donnees['quantity']

# Supprimer les doublons
donnees = donnees.drop_duplicates()

# Compter les achats futurs
achats_futurs = donnees[donnees['date_purchase'] > pd.to_datetime('today')].shape[0]

# Chiffrer les données sensibles
cle = Fernet.generate_key()
suite_cipher = Fernet(cle)

fake = Faker()

noms_chiffres = [suite_cipher.encrypt(fake.name().encode()) for _ in range(len(donnees))]
adresses_chiffrees = [suite_cipher.encrypt(fake.address().replace('\n', ', ').encode()) for _ in range(len(donnees))]
cartes_credit_chiffrees = [suite_cipher.encrypt(str(cart).encode()) for cart in donnees['credit_card']]

# Remplacer les colonnes originales par les données chiffrées
donnees['name'] = noms_chiffres
donnees['full_address'] = adresses_chiffrees
donnees['credit_card'] = cartes_credit_chiffrees

# Sauvegarder les données propres dans un fichier CSV
donnees.to_csv('./data/clean_data.csv', index=False)
