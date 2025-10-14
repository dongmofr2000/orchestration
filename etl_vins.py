import pandas as pd
import numpy as np
from scipy.stats import zscore
import os

# --- Configuration des Fichiers ---
ERP_FILE = 'Fichier_erp.csv'
WEB_FILE = 'Fichier_web.csv'
LIAISON_FILE = 'fichier_liaison.csv'

# Paramètres de lecture communs aux CSV
# Utilisation de l'encodage 'latin-1' pour gérer les caractères accentués français et le séparateur ';'
READ_PARAMS = {
    'sep': ';',
    'encoding': 'latin-1',
    'low_memory': False 
}

# --- Phase 1: Ingestion & Jointure des Données (avec correction d'encodage) ---

def charger_et_joindre_donnees():
    """Charge les trois fichiers CSV et effectue les jointures nécessaires."""
    print("--- Phase 1: Ingestion & Nettoyage des Données ---")
    try:
        # 1. Chargement des données
        df_erp = pd.read_csv(ERP_FILE, **READ_PARAMS)
        df_web = pd.read_csv(WEB_FILE, **READ_PARAMS)
        df_liaison = pd.read_csv(LIAISON_FILE, **READ_PARAMS)

        # 2. Nettoyage initial des colonnes pour la jointure
        # Renommer la colonne 'sku' en 'id_web' dans df_web pour la jointure avec df_liaison
        df_web.rename(columns={'sku': 'id_web'}, inplace=True)
        
        # S'assurer que les clés de jointure sont du bon type (important pour les IDs)
        df_erp['product_id'] = pd.to_numeric(df_erp['product_id'], errors='coerce').astype('Int64')
        df_liaison['product_id'] = pd.to_numeric(df_liaison['product_id'], errors='coerce').astype('Int64')
        df_liaison['id_web'] = pd.to_numeric(df_liaison['id_web'], errors='coerce').astype('Int64')
        df_web['id_web'] = pd.to_numeric(df_web['id_web'], errors='coerce').astype('Int64')

        # 3. Jointure des données (ERP <-> Liaison <-> Web)
        
        # Jointure 1: ERP et Liaison sur 'product_id' (Clé principale du produit)
        df_merged = pd.merge(df_erp, df_liaison, on='product_id', how='left')
        
        # Jointure 2: Fusion avec les données Web sur 'id_web'
        # Sélection des colonnes utiles du web (id_web et total_sales)
        df_web_sales = df_web[['id_web', 'total_sales']]
        df_final = pd.merge(df_merged, df_web_sales, on='id_web', how='left')

        # 4. Nettoyage des données jointes
        
        # A. Nettoyage des prix (remplacement de la virgule décimale par un point)
        df_final['price'] = df_final['price'].astype(str).str.replace(',', '.', regex=False)
        df_final['price'] = pd.to_numeric(df_final['price'], errors='coerce')

        # B. Remplacer les ventes nulles par 0
        df_final['total_sales'] = df_final['total_sales'].fillna(0)
        df_final['total_sales'] = pd.to_numeric(df_final['total_sales'], errors='coerce')
        
        # Supprimer les lignes où le prix est NaN après nettoyage
        df_final.dropna(subset=['price'], inplace=True)

        return df_final

    except FileNotFoundError as e:
        print(f"ERREUR lors du chargement des fichiers: Un ou plusieurs fichiers sources sont introuvables. Vérifiez le chemin et le nom des fichiers. ({e})")
        return None
    except Exception as e:
        print(f"ERREUR lors du chargement des fichiers: {e}")
        return None

# --- Phase 2: Transformation & Calcul du Z-Score ---

def transformer_et_enrichir_donnees(df_coherent):
    """Calcule le CA par produit et identifie les vins millésimés (Z-Score > 2)."""
    print("--- Phase 2: Transformation (CA & Z-Score) ---")

    # 1. Calcul du Chiffre d'Affaires (CA) par produit
    df_coherent['CA_produit'] = df_coherent['price'] * df_coherent['total_sales']

    # 2. Calcul du Z-Score pour l'identification des Outliers de prix
    
    # Nous calculons le Z-Score sur la colonne 'price'
    # Le Z-Score mesure combien d'écarts-types un point de donnée est éloigné de la moyenne.
    
    # Étape A: Calcul de la moyenne et de l'écart-type
    mean_price = df_coherent['price'].mean()
    std_price = df_coherent['price'].std()

    # Vérification pour éviter la division par zéro (si tous les prix sont identiques)
    if std_price == 0:
        print("Avertissement: Écart-type nul, Z-Score non calculé.")
        df_coherent['Z_Score_Prix'] = 0
    else:
        # Étape B: Application de la formule du Z-Score
        df_coherent['Z_Score_Prix'] = zscore(df_coherent['price'])
        
        # Vérification alternative avec la formule manuelle (pour le débogage)
        # df_coherent['Z_Score_Prix'] = (df_coherent['price'] - mean_price) / std_price
        print(f"Moyenne des prix: {mean_price:.2f} €")
        print(f"Écart-type des prix: {std_price:.2f} €")


    # 3. Identification des vins millésimés (Outliers)
    
    # Un vin millésimé est défini comme ayant un Z-Score > 2
    seuil_millésime = 2
    df_coherent['est_millésime'] = df_coherent['Z_Score_Prix'] > seuil_millésime
    
    print(f"Nombre de vins identifiés comme 'millésimés' (Z-Score > {seuil_millésime}): {df_coherent['est_millésime'].sum()}")

    return df_coherent

# --- Phase 3: Export des Résultats ---

def exporter_rapports(df_enriched):
    """Génère les rapports Excel requis."""
    print("--- Phase 3: Exportation des Rapports ---")
    
    # 1. Rapport des Vins Millésimés (Z-Score > 2)
    df_millésimes = df_enriched[df_enriched['est_millésime']].copy()
    
    # Colonnes à inclure dans le rapport final des millésimes
    millésimes_cols = [
        'product_id', 
        'id_web', 
        'price', 
        'total_sales', 
        'CA_produit',
        'Z_Score_Prix'
    ]
    df_millésimes = df_millésimes[millésimes_cols].sort_values(by='Z_Score_Prix', ascending=False)
    
    # Sauvegarde du rapport Millésimes
    output_millésimes = 'rapport_vins_millésimés.xlsx'
    df_millésimes.to_excel(output_millésimes, index=False)
    print(f"✅ Rapport 1 (Vins Millésimés) généré : {output_millésimes}")

    # 2. Rapport des produits non cohérents (ceux présents dans ERP mais sans ID WEB)
    # Dans ce contexte, on considère non cohérents les produits ayant un prix mais pas de vente
    # ou dont l'id_web est manquant (qui indique qu'il n'y a pas de lien avec le web)
    df_incohérents = df_enriched[df_enriched['id_web'].isna()].copy()
    
    # Inclure le prix, l'état du stock, et l'id du produit ERP
    incohérents_cols = ['product_id', 'price', 'stock_quantity', 'stock_status']
    df_incohérents = df_incohérents[incohérents_cols].sort_values(by='product_id')

    # Sauvegarde du rapport Incohérents
    output_incohérents = 'produits_sans_lien_web.xlsx'
    df_incohérents.to_excel(output_incohérents, index=False)
    print(f"✅ Rapport 2 (Produits sans lien Web) généré : {output_incohérents}")

# --- Fonction Principale ---

if __name__ == '__main__':
    # 1. Charger et nettoyer
    df_coherent = charger_et_joindre_donnees()
    
    if df_coherent is not None:
        # 2. Transformer et enrichir
        df_enriched = transformer_et_enrichir_donnees(df_coherent)
        
        # 3. Exporter
        exporter_rapports(df_enriched)
        
        print("\n--- Processus ETL des Vins Terminé ---")
