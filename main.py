import psycopg2
from outils import dictionnaire

try:
    with psycopg2.connect(**dictionnaire) as connexion:
        with connexion.cursor() as cursor:
            cursor.execute("SET search_path TO source;")
            cursor.execute("SELECT * FROM olist_geolocation_dataset LIMIT 100;")
            resultats = cursor.fetchall()
            for ligne in resultats:
                print(ligne)

except Exception as e:
    print(f"Une erreur est survenue : {e}")
    

