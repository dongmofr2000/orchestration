import pandas as pd
import numpy as np
from scipy.stats import zscore
import os

# --- Configuration des Fichiers ---
ERP_FILE = 'Fichier_erp.csv'
WEB_FILE = 'Fichier_web.csv'
LIAISON_FILE = 'fichier_liaison.csv'

# Paramètres de lecture communs aux CSV
READ_PARAMS = {
    'sep': ';',
    'encoding': 'latin-1',
    'low_memory': False 
}

# --- Phase 1: Ingestion & Jointure des Données ---

def charger_et_joindre_donnees():
    """Charge les trois fichiers CSV et effectue les jointures nécessaires."""
    print("--- Phase 1: Ingestion & Nettoyage des Données ---")
    try:
        # 1. Chargement des données
        # ATTENTION : Les fichiers sont dans le même répertoire que le script dans Kestra (défaut)
        df_erp = pd.read_csv(ERP_FILE, **READ_PARAMS)
        df_web = pd.read_csv(WEB_FILE, **READ_PARAMS)
        df_liaison = pd.read_csv(LIAISON_FILE, **READ_PARAMS)

        # 2. Nettoyage initial des colonnes pour la jointure
        df_web.rename(columns={'sku': 'id_web'}, inplace=True)
        
        df_erp['product_id'] = pd.to_numeric(df_erp['product_id'], errors='coerce').astype('Int64')
        df_liaison['product_id'] = pd.to_numeric(df_liaison['product_id'], errors='coerce').astype('Int64')
        df_liaison['id_web'] = pd.to_numeric(df_liaison['id_web'], errors='coerce').astype('Int64')
        df_web['id_web'] = pd.to_numeric(df_web['id_web'], errors='coerce').astype('Int64')

        # 3. Jointure des données (ERP <-> Liaison <-> Web)
        df_merged = pd.merge(df_erp, df_liaison, on='product_id', how='left')
        df_web_sales = df_web[['id_web', 'total_sales']]
        df_final = pd.merge(df_merged, df_web_sales, on='id_web', how='left')

        # 4. Nettoyage des données jointes
        df_final['price'] = df_final['price'].astype(str).str.replace(',', '.', regex=False)
        df_final['price'] = pd.to_numeric(df_final['price'], errors='coerce')
        df_final['total_sales'] = df_final['total_sales'].fillna(0)
        df_final['total_sales'] = pd.to_numeric(df_final['total_sales'], errors='coerce')
        
        df_final.dropna(subset=['price'], inplace=True)

        return df_final

    except FileNotFoundError as e:
        print(f"ERREUR: Fichier source introuvable. ({e})")
        return None
    except Exception as e:
        print(f"ERREUR: {e}")
        return None

# --- Phase 2: Transformation & Calcul du Z-Score ---

def transformer_et_enrichir_donnees(df_coherent):
    """Calcule le CA par produit et identifie les vins millésimés (Z-Score > 2)."""
    print("--- Phase 2: Transformation (CA & Z-Score) ---")

    # 1. Calcul du Chiffre d'Affaires (CA) par produit
    df_coherent['CA_produit'] = df_coherent['price'] * df_coherent['total_sales']

    # 2. Calcul du Z-Score
    std_price = df_coherent['price'].std()

    if std_price == 0:
        df_coherent['Z_Score_Prix'] = 0
    else:
        df_coherent['Z_Score_Prix'] = zscore(df_coherent['price'])

    # 3. Identification des vins millésimés (Outliers, Z-Score > 2)
    df_coherent['est_millésime'] = df_coherent['Z_Score_Prix'] > 2
    
    print(f"Nombre de vins identifiés comme 'millésimés' : {df_coherent['est_millésime'].sum()}")

    return df_coherent

# --- Phase 3: Export des Résultats (MISE À JOUR) ---

def exporter_rapports(df_enriched):
    """Génère tous les rapports et livrables finaux requis."""
    print("--- Phase 3: Exportation des Livrables & Rapports ---")
    
    seuil_millésime = 2

    # A. Filtrer Vins Premium et Ordinaires
    df_premium = df_enriched[df_enriched['Z_Score_Prix'] > seuil_millésime].copy()
    df_ordinaire = df_enriched[df_enriched['Z_Score_Prix'] <= seuil_millésime].copy()
    
    # Colonnes pour les exports
    cols_export = ['product_id', 'id_web', 'price', 'total_sales', 'CA_produit', 'Z_Score_Prix']
    
    # --- 1. Export du Chiffre d'Affaires par Produit en .XLSX (LIVRABLE)
    df_ca_produit = df_enriched[['product_id', 'price', 'total_sales', 'CA_produit']].copy()
    output_ca_xlsx = 'rapport_ca_produit.xlsx' 
    df_ca_produit.to_excel(output_ca_xlsx, index=False)
    print(f"✅ Export : CA par Produit (.xlsx) généré : {output_ca_xlsx}")


    # --- 2. Export de la Liste des Vins Premium en .CSV (LIVRABLE)
    output_premium_csv = 'vins_premium.csv'
    df_premium[cols_export].to_csv(output_premium_csv, index=False, sep=';', encoding='utf-8')
    print(f"✅ Export : Vins Premium (.csv) généré : {output_premium_csv}")


    # --- 3. Export des Vins Ordinaires en .CSV (LIVRABLE)
    output_ordinaire_csv = 'vins_ordinaires.csv'
    df_ordinaire[cols_export].to_csv(output_ordinaire_csv, index=False, sep=';', encoding='utf-8')
    print(f"✅ Export : Vins Ordinaires (.csv) généré : {output_ordinaire_csv}")


    # --- 4. Rapport des vins millésimés (original, conservé pour les logs)
    df_premium[cols_export].to_excel('rapport_vins_millésimés.xlsx', index=False)
    
    # --- 5. Rapport des produits incohérents (sans lien web, conservé)
    df_incohérents = df_enriched[df_enriched['id_web'].isna()].copy()
    incohérents_cols = ['product_id', 'price', 'stock_quantity', 'stock_status']
    df_incohérents = df_incohérents[incohérents_cols].sort_values(by='product_id')
    output_incohérents = 'produits_sans_lien_web.xlsx'
    df_incohérents.to_excel(output_incohérents, index=False)
    print(f"✅ Rapport : Produits sans lien Web généré : {output_incohérents}")


# --- Fonction Principale (Point d'Entrée) ---

if __name__ == '__main__':
    df_coherent = charger_et_joindre_donnees()
    
    if df_coherent is not None:
        df_enriched = transformer_et_enrichir_donnees(df_coherent)
        exporter_rapports(df_enriched)
        print("\n--- Processus ETL des Vins Terminé ---")