import requests
from confluent_kafka import Producer
import time 

from cryptography.fernet import Fernet

# Configuration Kafka
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'ecom_topic'

# URL de l'API randomuser.me
api_url = 'http://data_laravel.test/sales'

# Crée une instance du producteur Kafka
producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})

# Génère une clé de chiffrement Fernet
encryption_key = Fernet.generate_key()
cipher = Fernet(encryption_key)

# Fonction pour récupérer et publier des données e-commerce aléatoires
def fetch_and_produce_ecom_data():
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()['data']

            producer.produce(kafka_topic, value=str(data))

            print(f"Données publiées sur le topic '{kafka_topic}'")
            print(data)

        else:
            print(f"Échec de la récupération des données depuis l'API. Code d'état : {response.status_code}")
    except Exception as e:
        print(f"Une erreur s'est produite : {str(e)}")

# Point d'entrée du script
if __name__ == '__main__':
    while True:
        fetch_and_produce_ecom_data()
        time.sleep(10) 
