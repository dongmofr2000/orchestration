import sqlite3
import pandas as pd
import os
import re

# --- Configuration ---
DATABASE_NAME = 'vins_data.db'
SQL_SCRIPT = 'ca_calculation.sql'

# Fichiers CSV (Les noms des fichiers doivent correspondre à ceux de votre répertoire)
CSV_FILES = {
    'erp': 'Fichier_erp.csv',
    'web': 'Fichier_web.csv',
    'liaison': 'fichier_liaison.csv'
}
# --- Fin Configuration ---


def load_csv_to_db(conn, table_name, csv_path):
    """Charge un fichier CSV dans une table SQLite, gère les séparateurs décimaux."""
    try:
        # Lecture du CSV avec le séparateur ';' et l'encodage latin-1 (pour la compatibilité)
        df = pd.read_csv(csv_path, sep=';', encoding='latin-1')
        
        # Nettoyage des noms de colonnes : suppression des espaces et mise en minuscules
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        
        # Correction spécifique pour la colonne 'price' du fichier ERP : 
        # remplace la virgule décimale par un point pour la conversion numérique.
        if table_name == 'erp':
            df['price'] = df['price'].astype(str).str.replace(',', '.', regex=False)
            df['price'] = pd.to_numeric(df['price'], errors='coerce') # 'coerce' met NaN si échec de conversion

        # Écriture dans la base de données SQLite
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"-> Table '{table_name}' chargée ({len(df)} lignes).")
    except FileNotFoundError:
        print(f"ERREUR: Fichier CSV '{csv_path}' non trouvé.")
        raise
    except Exception as e:
        print(f"ERREUR lors du chargement de '{csv_path}' dans '{table_name}': {e}")
        raise


def execute_sql_script(conn, script_path):
    """
    Crée une table temporaire pour la jointure et exécute les requêtes finales
    pour obtenir le CA détaillé et le CA global.
    """
    if not os.path.exists(script_path):
        # Le fichier SQL est juste une référence maintenant, la logique est ci-dessous.
        pass 

    print(f"\nExécution de la logique de calcul du Chiffre d'Affaires...")
    
    # Nouvelle requête SQL avec le filtre t1.post_type = 'product'
    create_temp_table_query = f"""
    CREATE TEMPORARY TABLE JointureCA_TEMP AS 
    WITH ProduitsWebUniques AS (
        SELECT
            CAST(t2.product_id AS INTEGER) AS ID_ERP,
            t1.total_sales,
            t3.price
        FROM web AS t1
        INNER JOIN liaison AS t2 ON t1.sku = t2.id_web
        INNER JOIN erp AS t3 ON t2.product_id = t3.product_id
        WHERE t1.post_type = 'product'  -- <-- FILTRE CLÉ AJOUTÉ ICI
          AND t1.total_sales > 0
          AND t3.onsale_web = 1
          AND t3.price IS NOT NULL
    )
    SELECT
        ID_ERP,
        price,
        total_sales,
        (price * total_sales) AS CA_Par_Produit
    FROM ProduitsWebUniques;
    """
    
    # 1. Exécution de la création de la table temporaire
    try:
        cur = conn.cursor()
        print("-> Étape 1/2 : Calcul du CA par produit dans une table temporaire (JointureCA_TEMP)...")
        # Exécuter l'instruction qui crée et remplit la table temporaire
        cur.executescript(create_temp_table_query) 
        conn.commit()
    except sqlite3.Error as e:
        print(f"ERREUR SQL (SQLite) lors de la création de la table temporaire: {e}")
        return None, None
        
    # 2. Exécution des deux SELECTs finaux en utilisant la table temporaire
    
    # Requête 1: CA Détaillé (Top 5)
    query_ca_detail = """
    SELECT
        ID_ERP,
        price AS Prix_Unitaire,
        total_sales AS Quantité_Vendue,
        CA_Par_Produit
    FROM JointureCA_TEMP
    ORDER BY CA_Par_Produit DESC
    LIMIT 5;
    """
    
    # Requête 2: CA Global
    query_ca_total = "SELECT SUM(CA_Par_Produit) AS Chiffre_Affaires_Global FROM JointureCA_TEMP;"

    try:
        # Récupération du CA Détaillé
        ca_detail_df = pd.read_sql_query(query_ca_detail, conn)
        
        # Récupération du CA Global
        ca_total_df = pd.read_sql_query(query_ca_total, conn)
        
        print("-> Étape 2/2 : Récupération des résultats terminés avec succès.")
        return ca_detail_df, ca_total_df
        
    except sqlite3.Error as e:
        print(f"ERREUR SQL (SQLite) lors de l'exécution des SELECTs finaux: {e}")
        return None, None


if __name__ == '__main__':
    # Suppression de la base de données précédente si elle existe
    if os.path.exists(DATABASE_NAME):
        os.remove(DATABASE_NAME)
        
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        
        print(f"Initialisation de la base de données '{DATABASE_NAME}' et chargement des CSV...")
        
        # 1. Chargement des données
        for table, path in CSV_FILES.items():
            load_csv_to_db(conn, table, path)
            
        # 2. Exécution du script SQL
        ca_detail_df, ca_total_df = execute_sql_script(conn, SQL_SCRIPT)
        
        if ca_detail_df is not None and ca_total_df is not None:
            
            # --- Affichage des Résultats ---
            
            # CA Détaillé
            print("\n" + "="*80)
            print("--- RÉSULTAT 1 : TOP 5 des Produits par Chiffre d'Affaires ---")
            print("="*80)
            # Affichage des résultats en markdown pour une bonne lisibilité
            print(ca_detail_df.head().to_markdown(index=False, floatfmt=".2f"))

            # CA Global
            ca_total = ca_total_df['Chiffre_Affaires_Global'].iloc[0]
            
            print("\n" + "="*80)
            print(f"--- RÉSULTAT 2 : CHIFFRE D'AFFAIRES GLOBAL (CA) ---")
            print("="*80)
            # Utilisation de la mise en forme de milliers
            # Note: Le résultat attendu était 70 568.60. Le nouveau calcul devrait le confirmer.
            print(f"CA Total de l'entreprise : {ca_total:,.2f} €")
            print("="*80 + "\n")

    except Exception as e:
        print(f"\nUne erreur critique s'est produite: {e}")
        
    finally:
        if conn:
            conn.close()
