import os
import sys
import time
import traceback
from datetime import timedelta
from pathlib import Path
import psycopg2

# ==============================================================================
# RACINE DU PROJET (FIXATION DIRECTE & SIMPLE)
# ==============================================================================
# main_pipeline_etl.py EST à la racine, donc son dossier parent est la RACINE.
RACINE_PROJET = Path(__file__).resolve().parent

# On inscrit la racine au tout début de sys.path
str_racine = str(RACINE_PROJET)
if str_racine in sys.path:
    sys.path.remove(str_racine)
sys.path.insert(0, str_racine)

# Verification visuelle au lancement
print(f"📁 Racine du projet : {RACINE_PROJET}")

# ==============================================================================
# IMPORTS DES MODULES DE LA RACINE
# ==============================================================================
from config import data_warehouse, construire_uri_psycopg2

try:
    from journaux import (
        configurer_journaux,
        log_demarrage,
        log_arret,
        log_etape,
        log_succes,
        log_erreur,
        log_critique
    )
    logger, quality_logger = configurer_journaux()
except ImportError as e:
    print(f"\n💥 [ERREUR CRITIQUE] Impossible d'importer 'journaux.py' depuis {RACINE_PROJET}")
    print(f"Détail : {e}")
    print("Vérifie bien que le fichier 'journaux.py' existe à la racine du projet.")
    sys.exit(1)

# ==============================================================================
# IMPORTS DES SOUS-MODULES (BRONZE, SILVER, GOLD)
# ==============================================================================
for sous_dossier in ["Bronze", "Silver", "Gold"]:
    chemin_d = str(RACINE_PROJET / sous_dossier)
    if chemin_d not in sys.path:
        sys.path.append(chemin_d)

try:
    from main_bronze import run_bronze
    from main_silver import run_silver
    from main_gold import run_gold
except ImportError as e:
    print(f"\n💥 [ERREUR CRITIQUE] Impossible d'importer les sous-modules ETL : {e}")
    traceback.print_exc()
    sys.exit(1)

# ==============================================================================
# SÉQUENCE DES MODULES
# ==============================================================================
PIPELINE_MODULES = [
    {"nom": "Bronze (Extraction & Réplication)", "fonction": run_bronze},
    {"nom": "Silver (Nettoyage & Normalisation)", "fonction": run_silver},
    {"nom": "Gold (Modélisation & Agrégation)", "fonction": run_gold}
]

# ==============================================================================
# ORCHESTRATEUR TRANSACTIONNEL
# ==============================================================================
def main():
    temps_debut_global = time.time()
    
    print("\n" + "█"*80)
    print("🚀  ORCHESTRATEUR PRINCIPAL DU DATA WAREHOUSE (ATOMIC PIPELINE)  🚀")
    print("█"*80)
    
    log_demarrage(logger, "Orchestrateur Data Warehouse (Bronze -> Silver -> Gold)")
    uri_psycopg2 = construire_uri_psycopg2(data_warehouse)
    total_modules = len(PIPELINE_MODULES)

    print("\n📡 Ouverture de la transaction globale avec le Data Warehouse...")
    
    try:
        with psycopg2.connect(uri_psycopg2) as conn:
            print("🔒 Transaction active initialisée (Mode Atomique : Tout-ou-Rien).\n")

            for idx, module in enumerate(PIPELINE_MODULES, start=1):
                nom_module = module["nom"]
                fonction_module = module["fonction"]

                log_etape(logger, f"Exécution étape {idx}/{total_modules} : {nom_module}", idx, total_modules)
                
                print("="*80)
                print(f"▶️  LANCEMENT DU MODULE [{idx}/{total_modules}] : {nom_module.upper()}")
                print("="*80)

                temps_debut_step = time.time()
                fonction_module(conn=conn)
                
                duree_step = timedelta(seconds=round(time.time() - temps_debut_step))
                log_succes(logger, f"Module [{nom_module}] terminé", f"Durée : {duree_step}")
                print(f"\n✅  [SUCCÈS] Module [{nom_module}] terminé en {duree_step}.\n")

            print("\n" + "⭐"*80)
            print("💾 VALIDATION FINALE DE LA TRANSACTION (COMMIT GLOBAL)...")
            print("⭐"*80)
            
            conn.commit()
            print("✨ TOUTES LES DONNÉES ONT ÉTÉ COMMITÉES AVEC SUCCÈS !")

    except Exception as err_global:
        duree_totale = timedelta(seconds=round(time.time() - temps_debut_global))
        print("\n" + "🛑"*40)
        print("⛔  ÉCHEC DU PIPELINE : ANNULATION GLOBALE (ROLLBACK AUTOMATIQUE)")
        print("🛑"*40)
        print(f"💥  Raison du crash : {err_global}")
        traceback.print_exc()

        log_critique(logger, "Pipeline interrompu - Rollback transactionnel global exécuté", str(err_global))
        log_arret(logger, succes=False, message=f"Pipeline échoué après {duree_totale}")
        sys.exit(1)

    duree_totale = timedelta(seconds=round(time.time() - temps_debut_global))
    print("\n" + "█"*80)
    print("🏁🏁🏁  BILAN GLOBAL DU PIPELINE DATA WAREHOUSE  🏁🏁🏁")
    print("█"*80)
    print(f"⏱️   Durée totale d'exécution : {duree_totale}")
    print("🎉   Pipeline atomique terminé avec succès sans aucune erreur !\n")
    log_arret(logger, succes=True, message=f"Pipeline exécuté avec succès en {duree_totale}")

if __name__ == "__main__":
    main()