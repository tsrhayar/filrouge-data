import pandas as pd

# Charger le fichier CSV fourni pour comprendre sa structure
chemin_fichier = './data/clean_data.csv'
donnees = pd.read_csv(chemin_fichier)

# Afficher les premières lignes du dataframe pour avoir un aperçu de son contenu
donnees.head()

# Afficher les noms des colonnes
donnees.columns

# Sélectionner les colonnes de la table de faits
table_de_faits = donnees[['quantity', 'rating', 'user_id', 'product_id', 'date_purchase_id', 'comment_id']]

# Création de la table de dimension Utilisateur
dimension_utilisateur = donnees[['user_id', 'name', 'age', 'gender', 'city', 'country', 'credit_card', 'full_address', 'user_source_type', 'user_status']].drop_duplicates(subset='user_id')

# Création de la table de dimension Produit
dimension_produit = donnees[['product_id', 'product_name', 'category', 'subcategory', 'price']].drop_duplicates(subset='product_id')

# Création de la table de dimension Commentaire
dimension_commentaire = donnees[['comment_id', 'comment']].drop_duplicates(subset='comment_id')

# Création de la table de dimension Date
dimension_date = donnees[['date_purchase_id', 'date_purchase']].drop_duplicates(subset='date_purchase_id')

# Sauvegarder la table de faits dans un fichier CSV
table_de_faits.to_csv('./data/fact_table.csv', index=False)

# Sauvegarder la table de dimension Utilisateur dans un fichier CSV
dimension_utilisateur.to_csv('./data/user_dimension.csv', index=False)

# Sauvegarder la table de dimension Produit dans un fichier CSV
dimension_produit.to_csv('./data/product_dimension.csv', index=False)

# Sauvegarder la table de dimension Commentaire dans un fichier CSV
dimension_commentaire.to_csv('./data/comment_dimension.csv', index=False)

# Sauvegarder la table de dimension Date dans un fichier CSV
dimension_date.to_csv('./data/date_dimension.csv', index=False)
