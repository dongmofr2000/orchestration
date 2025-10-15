import pandas as pd
import numpy as np
import sys
from scipy.stats import zscore

# --- CONFIGURATION & PATHS ---
WEB_FILE = 'Fichier_web.csv'
ERP_FILE = 'Fichier_erp.csv'
LIAISON_FILE = 'fichier_liaison.csv'
EXPECTED_CA = 70568.60 # Valeur attendue pour le Chiffre d'Affaires total
CSV_DELIMITER = ';'

# --- FONCTIONS UTILITAIRES ---

def clean_id_column(df, col):
    """Nettoie les clés de jointure (supprime espaces et remplace vides/nan par np.nan)."""
    # Enlève les espaces et convertit en string
    df[col] = df[col].astype(str).str.strip().replace('nan', np.nan)
    # Remplace les chaînes vides (issues de strip() sur des cellules vides) par NaN
    df[col] = df[col].replace('', np.nan)
    return df

# --- ÉTAPE 1: Chargement et Nettoyage Robuste ---

def load_and_clean_data():
    """
    Charge les fichiers CSV avec gestion de l'encodage et effectue le nettoyage initial.
    """
    print("--- ÉTAPE 1: Chargement et Nettoyage Robuste des 3 Fichiers ---")

    # 1. Chargement robuste des fichiers avec l'encodage 'latin1'
    try:
        df_web = pd.read_csv(WEB_FILE, sep=CSV_DELIMITER, encoding='latin1')
        print(f"[+] {len(df_web)} lignes chargées dans la table 'web'.")
        df_erp = pd.read_csv(ERP_FILE, sep=CSV_DELIMITER, encoding='latin1')
        print(f"[+] {len(df_erp)} lignes chargées dans la table 'erp'.")
        df_liaison = pd.read_csv(LIAISON_FILE, sep=CSV_DELIMITER, encoding='latin1')
        print(f"[+] {len(df_liaison)} lignes chargées dans la table 'liaison'.")
    except Exception as err:
        print(f"[ERREUR CRITIQUE] Impossible de charger un ou plusieurs fichiers CSV: {err}")
        sys.exit(1)

    # 2. Nettoyage des clés de jointure (SKU / ID)
    print("[*] Nettoyage agressif des espaces blancs et des valeurs nulles sur les clés de jointure (SKU/ID)...")

    df_web = clean_id_column(df_web, 'sku')
    df_liaison = clean_id_column(df_liaison, 'id_web')
    df_erp = clean_id_column(df_erp, 'product_id')
    df_liaison = clean_id_column(df_liaison, 'product_id')

    # Conversion des clés 'product_id' en type numérique (Int64) pour la jointure
    try:
        df_liaison['product_id'] = pd.to_numeric(df_liaison['product_id'], errors='coerce').astype('Int64')
        df_erp['product_id'] = pd.to_numeric(df_erp['product_id'], errors='coerce').astype('Int64')
        print("[*] Conversion des clés 'product_id' en type numérique (Int64) effectuée.")
    except Exception as err:
        print(f"[WARNING] Impossible de convertir 'product_id' en Int64: {err}. Utilisation de la chaîne (str).")
        df_liaison['product_id'] = df_liaison['product_id'].astype(str)
        df_erp['product_id'] = df_erp['product_id'].astype(str)

    # 3. Nettoyage et conversion des colonnes de CA et de stock
    df_erp['price'] = df_erp['price'].astype(str).str.replace(',', '.', regex=False)
    df_erp['price'] = pd.to_numeric(df_erp['price'], errors='coerce')
    print("[*] Nettoyage de la colonne 'price': Conversion du séparateur décimal (virgule -> point) effectuée.")

    df_web['total_sales'] = pd.to_numeric(df_web['total_sales'], errors='coerce').fillna(0).astype('Int64')
    print("[*] Nettoyage et conversion de la colonne 'total_sales' en Int64 effectuée.")

    df_erp['stock_quantity'] = pd.to_numeric(df_erp['stock_quantity'], errors='coerce').fillna(0).astype('Int64')
    print("[*] Conversion agressive des stocks en 'Int64' effectuée.")

    print("[+] Fichiers CSV chargés, nettoyés et prêts pour la transformation.")

    return df_web, df_erp, df_liaison


# --- ÉTAPE 2: Transformation et Jointure Triple ---

def transform_and_join(df_web, df_erp, df_liaison):
    """
    Filtre les données Web, nettoie la table de liaison et effectue la jointure finale.
    """
    print("\n--- ÉTAPE 2: Transformation et Jointure Triple ---")
    
    # Prétraitement
    df_web_products = df_web[df_web['post_type'] == 'product'].copy()
    print(f"[*] Fichier 'web': Filtré sur 'post_type' = 'product'. Réduit à {len(df_web_products)} lignes.")

    df_liaison_clean = df_liaison.dropna(subset=['id_web', 'product_id']).copy()
    print(f"[*] Fichier 'liaison': {len(df_liaison)} lignes initiales. Après nettoyage des ID invalides: {len(df_liaison_clean)} relations retenues.")

    # Jointure 1: ERP + Liaison
    df_merged_erp_liaison = pd.merge(df_erp, df_liaison_clean, 
                                     on='product_id', 
                                     how='inner',
                                     suffixes=('_erp', '_liaison'))
    print(f"[+] Jointure ERP + Liaison effectuée. Nombre de lignes: {len(df_merged_erp_liaison)}")

    # Jointure 2: Finale avec Web
    df_final = pd.merge(df_merged_erp_liaison, df_web_products, 
                        left_on='id_web', 
                        right_on='sku', 
                        how='inner', 
                        suffixes=('_erp_liaison', '_web_final'))
    
    print(f"[+] Jointure triple complétée. Le jeu de données final contient {len(df_final)} produits.")
    
    # Renommer la colonne product_id de l'ERP
    df_final.rename(columns={'product_id': 'product_id_erp'}, inplace=True)
    
    return df_final, df_web_products, df_erp, df_liaison_clean


# --- ÉTAPE 3: Tests de Qualité des Données (DQ Checks) ---

def run_data_quality_checks(df_final):
    """
    Exécute les tests de qualité des données (CA et nombre de lignes).
    Retrait du diagnostic CA pour éviter l'erreur d'affichage.
    """
    print("\n--- ÉTAPE 3: Tests de Qualité des Données (DQ Checks) ---")
    
    # 1. Calcul du Chiffre d'Affaires (CA)
    df_final['CA'] = df_final['price'] * df_final['total_sales']
    calculated_ca = df_final['CA'].sum()
    
    # TEST A1/A3 (Count Check)
    final_count = len(df_final)
    expected_count = 714
    status_count = "SUCCÈS" if final_count == expected_count else "ÉCHEC"
    print(f"[!] TEST A1/A3 (Fusionné Final) : {status_count}. Attendu {expected_count}, obtenu {final_count}.")

    # TEST A4 (CA Total Check)
    is_ca_success = np.isclose(calculated_ca, EXPECTED_CA, atol=0.01)
    status_ca = "SUCCÈS" if is_ca_success else "ÉCHEC"
    print(f"[!] TEST A4 (CA Total) : {status_ca}.")
    print(f"    Calculé {calculated_ca:,.2f}€, attendu {EXPECTED_CA:,.2f}€.")
    ca_diff = calculated_ca - EXPECTED_CA
    print(f"[!!!] Écart de CA (Chiffre d'Affaires): {ca_diff:,.2f}€")
        
    # On ne continue que si les tests critiques (A1/A3 et A4) passent
    return status_count == "SUCCÈS" and is_ca_success

def run_advanced_dq_checks(df_final, df_web_products, df_erp, df_liaison_clean):
    """
    Exécute les tests d'intégrité (doublons, jointures) et d'anomalies (Z-score).
    """
    print("\n--- ÉTAPE 3: Tests d'Intégrité et d'Anomalie ---")
    
    # 1. TEST : Absence de Doublons (Clé Primaire)
    print("--- 1. Tests d'Absence de Doublons ---")
    
    erp_duplicates = df_erp['product_id'].duplicated().sum()
    print(f"[!] TEST Doublons (ERP) : {'SUCCÈS' if erp_duplicates == 0 else 'ÉCHEC'}. {erp_duplicates} doublons trouvés sur 'product_id'.")

    web_duplicates = df_web_products['sku'].duplicated().sum()
    print(f"[!] TEST Doublons (Web) : {'SUCCÈS' if web_duplicates == 0 else 'ÉCHEC'}. {web_duplicates} doublons trouvés sur 'sku'.")
        
    # 2. TEST : Cohérence des Jointures
    print("\n--- 2. Tests de Cohérence des Jointures ---")
    
    erp_without_web = df_erp[~df_erp['product_id'].isin(df_liaison_clean['product_id'])]
    print(f"[!] TEST Jointure (ERP -> WEB) : AVERTISSEMENT. {len(erp_without_web)} produits ERP ne sont pas dans la table de liaison.")
        
    web_without_erp = df_web_products[~df_web_products['sku'].isin(df_liaison_clean['id_web'])]
    print(f"[!] TEST Jointure (WEB -> ERP) : AVERTISSEMENT. {len(web_without_erp)} produits WEB ne sont pas dans la table de liaison.")
    
    # 3. TEST : Détection d'Anomalies (Z-score sur le Prix)
    print("\n--- 3. Test du Z-score sur le Prix des Vins ---")
    
    prices = df_final['price'].replace([np.inf, -np.inf], np.nan).dropna()
    z_scores = zscore(prices)
    outlier_threshold = 3
    
    # On utilise l'index de prices pour sélectionner les lignes correspondantes dans df_final
    outlier_indices = prices.index[np.abs(z_scores) > outlier_threshold]
    outliers = df_final.loc[outlier_indices].copy()
    
    if len(outliers) == 0:
        print(f"[!] TEST Z-SCORE : SUCCÈS. Aucun outlier (Z-score > {outlier_threshold}) détecté sur le prix.")
    else:
        print(f"[!] TEST Z-SCORE : AVERTISSEMENT. {len(outliers)} produits sont des outliers de prix (Z-score > {outlier_threshold}).")
        
        # Affichage des outliers
        outliers['Z_Score'] = z_scores[np.abs(z_scores) > outlier_threshold]
        outliers_display = outliers[['product_id_erp', 'post_title', 'price', 'Z_Score']].sort_values(by='Z_Score', ascending=False).head(5)
        outliers_display.columns = ['ID ERP', 'Nom du Vin', 'Prix', 'Z-Score']
        
        print("\n    Les 5 prix les plus anormaux (élevés) sont:")
        print(outliers_display.to_string(index=False, float_format="%.2f"))
        
    return web_duplicates, web_without_erp


# --- ÉTAPE 4: Gestion des Anomalies et Nettoyage Final ---

def handle_anomalies(df_final, df_web_products, web_duplicates_count, web_without_erp_df):
    """
    Corrige les anomalies de doublons et de jointure critique avant la migration.
    """
    print("\n--- ÉTAPE 4: Gestion des Anomalies et Nettoyage Final ---")
    
    # Correction 1 : Doublon dans la table Web (SKU)
    if web_duplicates_count > 0:
        # On suppose que le doublon Web est une ligne non-produit/métadonnée, ou que l'on garde le premier.
        initial_len = len(df_web_products)
        df_web_products_cleaned = df_web_products.drop_duplicates(subset=['sku'], keep='first')
        dropped_count = initial_len - len(df_web_products_cleaned)
        print(f"[+] CORRECTION 1: Suppression de {dropped_count} doublon(s) sur SKU (Web).")
    else:
        df_web_products_cleaned = df_web_products
        print("[*] CORRECTION 1: Aucun doublon SKU à corriger.")

    # Correction 2 : Produits Web sans correspondance ERP (critique)
    # Ces produits ne peuvent être migrés sans prix/stock. On les exclut.
    critical_unlinked_skus = web_without_erp_df['sku'].tolist()
    if len(critical_unlinked_skus) > 0:
        # Filtrer la table de liaison pour exclure les id_web orphelins (qui n'auraient pas été exclus avant la jointure finale)
        df_final_cleaned = df_final[~df_final['sku'].isin(critical_unlinked_skus)].copy()
        dropped_count = len(df_final) - len(df_final_cleaned)
        print(f"[+] CORRECTION 2: Exclusion de {dropped_count} ligne(s) dans le DF final car SKU non lié à l'ERP (SKUs: {critical_unlinked_skus}).")
    else:
        df_final_cleaned = df_final.copy()
        print("[*] CORRECTION 2: Aucun produit Web critique non lié à l'ERP à exclure.")
    
    print(f"[+] Le jeu de données final contient maintenant {len(df_final_cleaned)} produits prêts pour la migration.")
    return df_final_cleaned


# --- PIPELINE ETL PRINCIPAL ---

def etl_pipeline():
    """
    Orchestre le processus ETL complet.
    """
    try:
        # ÉTAPE 1: Chargement et Nettoyage
        df_web, df_erp, df_liaison = load_and_clean_data()
        
        # ÉTAPE 2: Transformation et Jointure
        df_final, df_web_products, df_erp_cleaned, df_liaison_cleaned = transform_and_join(df_web, df_erp, df_liaison)
        
        # ÉTAPE 3: Contrôles de Qualité
        if not run_data_quality_checks(df_final):
            print("\n[!!!] Exécution arrêtée. Migration MongoDB non démarrée en raison d'échecs critiques (CA/Count).")
            return
            
        web_duplicates_count, web_without_erp_df = run_advanced_dq_checks(df_final, df_web_products, df_erp_cleaned, df_liaison_cleaned)

        # ÉTAPE 4: Gestion des Anomalies
        df_migratable = handle_anomalies(df_final, df_web_products, web_duplicates_count, web_without_erp_df)

        # ÉTAPE 5: Préparation et Migration MongoDB
        print("\n--- ÉTAPE 5: Préparation et Migration MongoDB ---")
        print(f"[+] Les contrôles de qualité sont passés. Le jeu de données ({len(df_migratable)} produits) est prêt pour la migration MongoDB.")
        
    except Exception as err:
        print(f"\n[ERREUR GÉNÉRALE] Une erreur CRITIQUE est survenue lors du pipeline ETL : {err}")
        # Afficher la ligne exacte où l'erreur s'est produite (pour le débogage)
        import traceback
        traceback.print_exc()

    print("\n--- FIN DU PIPELINE ETL ---")


if __name__ == "__main__":
    etl_pipeline()


