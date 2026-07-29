import datetime
from pathlib import Path
import psycopg2

# Configuration de la connexion PostgreSQL et des dossiers cibles
DB_PARAMS = "dbname='sourcea' user='postgres' password='HSMLdata' host='localhost' port='5432'"
SCHEMA_NAME = "source"
FOLDER_NAME = "source1"

def exécuter_extraction_bronze(conn):
    """
    Parcourt le dossier source1 et charge chaque fichier CSV dans une table 
    portant le même nom dans le schéma Bronze.
    """
    # Détermination et validation du chemin du dossier source
    script_path = Path(__file__).resolve()
    parent_dir = script_path.parent
    target_folder = parent_dir / FOLDER_NAME

    if not target_folder.exists() or not target_folder.is_dir():
        raise FileNotFoundError(f"❌ Le dossier spécifié n'existe pas dans le parent : {target_folder}")

    print(f"📂 Dossier cible trouvé : {target_folder}")
    total_rows_inserted = 0

    with conn.cursor() as cursor:
        # Création du schéma s'il n'existe pas (idempotence)
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_NAME}";')

        # Récupération de tous les fichiers CSV
        csv_files = list(target_folder.glob("*.csv"))
        if not csv_files:
            print("⚠️ Aucun fichier CSV trouvé dans le dossier.")
            return 0

        for file_path in csv_files:
            # Filtrage des fichiers temporaires ou cachés
            if file_path.name.startswith("~$") or file_path.name.startswith("."):
                print(f"⏭️ Fichier temporaire ignoré : {file_path.name}")
                continue

            # Le nom de la table correspond au nom du fichier sans extension
            table_name = file_path.stem
            print(f"⏳ Traitement du fichier : {file_path.name} -> Table : {SCHEMA_NAME}.{table_name}")

            # 'utf-8-sig' permet d'ignorer le BOM Excel si présent
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                header = f.readline().strip()

                # Nettoyage et extraction des noms de colonnes
                columns = [col.strip().replace('"', '').replace("'", "") for col in header.split(',')]
                columns = [col for col in columns if col]

                # Réinitialisation de la table (mode Overwrite propre à la couche Bronze)
                cursor.execute(f'DROP TABLE IF EXISTS "{SCHEMA_NAME}"."{table_name}";')

                # Typage global en TEXT pour stocker les données brutes sans risque de rejet
                create_cols = ", ".join([f'"{col}" TEXT' for col in columns])
                create_table_sql = f'CREATE TABLE "{SCHEMA_NAME}"."{table_name}" ({create_cols});'
                cursor.execute(create_table_sql)

                # Repositionne le pointeur au début du fichier avant le COPY
                f.seek(0)

                # Importation de masse ultra-rapide via la commande COPY
                sql_copy = f"""
                    COPY "{SCHEMA_NAME}"."{table_name}" 
                    FROM STDIN 
                    WITH (FORMAT CSV, HEADER true, DELIMITER ',', QUOTE '"');
                """
                cursor.copy_expert(sql_copy, f)

                # Comptage des lignes insérées pour le suivi
                cursor.execute(f'SELECT COUNT(*) FROM "{SCHEMA_NAME}"."{table_name}";')
                rows_count = cursor.fetchone()[0]
                total_rows_inserted += rows_count

                print(f"✅ Table `{SCHEMA_NAME}`.`{table_name}` créée avec succès ({rows_count} lignes).")

    return total_rows_inserted

# Point d'entrée pour l'exécution autonome du script
if __name__ == "__main__":
    try:
        print("🧪 Test autonome du script Extract.py...")
        conn_test = psycopg2.connect(DB_PARAMS)
        
        lignes = exécuter_extraction_bronze(conn_test)
        
        # Validation définitive des modifications dans la base
        conn_test.commit()
        print(f"🏁 Test réussi ! Total lignes chargées : {lignes}")

    except Exception as error:
        print(f"❌ Erreur lors du test : {error}")

    finally:
        # Fermeture de la connexion pour libérer les ressources du serveur
        if 'conn_test' in locals() and conn_test:
            conn_test.close()