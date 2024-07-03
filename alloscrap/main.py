import psycopg2
import os
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Récupérer les informations de connexion à partir des variables d'environnement
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')

try:
    # Établir la connexion
    connection = psycopg2.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database
    )
    
    # Vérifier la connexion
    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("Vous êtes connecté à - ", record, "\n")
    
    # Fermer la connexion
    cursor.close()
    connection.close()
except Exception as error:
    print("Erreur lors de la connexion à PostgreSQL", error)
