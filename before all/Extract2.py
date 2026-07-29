import io
from pathlib import Path
import pandas as pd
import psycopg2

# Définition du dossier source contenant les CSV (sous-dossier de ce script)
DOSSIER_SOURCE = "source2"


# 1. Récupération de la liste des chemins de fichiers CSV dans le sous-dossier cible
def files_names():
    chemin_dossier = Path(__file__).resolve().parent / DOSSIER_SOURCE

    # Sécurité : Si le dossier n'existe pas, on renvoie une liste vide pour éviter le crash
    if not chemin_dossier.exists() or not chemin_dossier.is_dir():
        print(f"⚠️ Le dossier {chemin_dossier} est introuvable.")
        return []

    # On récupère les chemins absolus de tous les fichiers CSV
    return list(chemin_dossier.glob("*.csv"))


# 2. Connexion à PostgreSQL et injection de masse
def extract_data():

    print(
        "================== Extraction des données depuis les fichiers CSV vers PostgreSQL ================="
    )
    print(
        "==============================================================================================="
    )
    print("====== Connecting to the PostgreSQL database...")
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="sourceb",
            user="postgres",
            password="HSMLdata",
        )
    except Exception as e:
        conn.close()
        print("💥💥💥💥💥💥💥💥")

    nom_schema = "source"
    liste_fichiers = files_names()

    if not liste_fichiers:
        print("⚠️ Aucun fichier à traiter. Fin du script.")
        conn.close()
        return

    
    try:
        # Ouverture du curseur (nommé 'cursor')
        with conn.cursor() as cursor:

            # On parcourt chaque chemin de fichier trouvé
            # On parcourt chaque chemin de fichier trouvé
            for chemin_fichier_csv in liste_fichiers:

                nom_fichier = chemin_fichier_csv.name
                print(f"⏳ Traitement du fichier : {nom_fichier}")

                df = pd.read_csv(chemin_fichier_csv, encoding="latin-1")

                df.columns = (
                    df.columns.str.lower()
                    .str.replace(" ", "_")
                    .str.replace("(", "")
                    .str.replace(")", "")
                )

                nom_table = chemin_fichier_csv.stem

                # =================================================================
                # AJOUT : CRÉATION AUTOMATIQUE DE LA TABLE AVANT LE TRANSFERT
                # =================================================================
                # 1. On crée la liste des colonnes suivies du type TEXT
                definitions_colonnes = ", ".join([f'"{col}" TEXT' for col in df.columns])
                
                # 2. On prépare la requête SQL (DROP puis CREATE pour réinitialiser la table proprement)
                requete_drop = f'DROP TABLE IF EXISTS "{nom_schema}"."{nom_table}";'
                requete_create = f'CREATE TABLE "{nom_schema}"."{nom_table}" ({definitions_colonnes});'
                
                # 3. On exécute la création dans PostgreSQL
                cursor.execute(requete_drop)
                cursor.execute(requete_create)
                # =================================================================

                # Transformation du DataFrame en flux de texte CSV (en mémoire RAM)
                flux_memoire = io.StringIO()
                df.to_csv(
                    flux_memoire,
                    index=False,
                    header=False,
                    sep=";",
                    encoding="latin-1",
                )
                flux_memoire.seek(0)

                # Préparation de la commande COPY avec protection des colonnes
                colonnes = ", ".join([f'"{col}"' for col in df.columns])
                requete_copy = f'COPY "{nom_schema}"."{nom_table}" ({colonnes}) FROM STDIN WITH (FORMAT CSV, DELIMITER ";")'

                try:
                    cursor.copy_expert(requete_copy, flux_memoire)
                    print(
                        f"   ✅ Table '{nom_schema}.{nom_table}' créée et injectée : {len(df)} lignes insérées."
                    )

                except Exception as e:
                    conn.rollback()  # Annulation de la transaction en cas d'erreur
                    print(
                        f"   ❌ Erreur lors de l'injection du fichier '{nom_fichier}': {e}"
                    )
        conn.commit()  # Validation de la transaction si tout s'est bien passé

    except Exception as e:
        # Annulation globale en cas de rupture de connexion ou de panne majeure
        conn.rollback()
        print(f"💥💥 Erreur critique lors de l'extraction des données : {e}")

    finally:
        # Fermeture systématique de la session pour libérer la base de données
        conn.close()
        print("扫 Connexion à la base de données fermée proprement.")


if __name__ == "__main__":
    extract_data()