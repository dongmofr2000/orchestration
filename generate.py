import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import os
import re

# --- Configuration et Dépendances ---
FICHIER_RH = "donnees_rh.csv"
FICHIER_SPORTIF = "donnees_sportives.csv"
FICHIER_SIMULE = "activites_simulees.csv"

# Options pour lire les fichiers CSV français (Point-virgule et encodage)
CSV_OPTIONS = {
    'encoding': 'latin-1',
    'sep': ';',
    'on_bad_lines': 'skip' # Ignore les lignes mal formées
}

FAKE = Faker('fr_FR')
NB_MOIS_HISTORIQUE = 12

# Fonction de nettoyage agressive des noms de colonnes
def sanitize_columns(df):
    cols_map = {}
    for col in df.columns:
        # Nettoyage : Minuscules, retirer les caractères spéciaux (sauf _), remplacer espace par _
        new_col = col.lower().strip()
        new_col = re.sub(r'[^\w\s]', '', new_col)
        new_col = new_col.replace(' ', '_')
        cols_map[col] = new_col
    df = df.rename(columns=cols_map)
    return df

# --- 1. Chargement des Références (avec sécurisation) ---
try:
    # 1.1 Chargement des fichiers
    df_rh = pd.read_csv(FICHIER_RH, **CSV_OPTIONS) 
    df_sportif = pd.read_csv(FICHIER_SPORTIF, **CSV_OPTIONS)

    # 1.2 NETTOYAGE DES EN-TÊTES (STANDARDISATION)
    df_rh = sanitize_columns(df_rh)
    df_sportif = sanitize_columns(df_sportif)

    # MAPPING des noms attendus après nettoyage
    # Ces noms sont basés sur le format des données RH
    df_rh = df_rh.rename(columns={'id_salarié': 'id_salarie', 'salaire_brut': 'salaire_brut', 'moyen_de_déplacement': 'moyen_de_deplacement'})
    df_sportif = df_sportif.rename(columns={'pratique_dun_sport': 'type_sport_declare'})
    
    # Validation finale des colonnes (évite un crash si le nettoyage a raté un nom)
    REQUIRED_RH_COLS = ['id_salarie', 'salaire_brut']
    if not all(col in df_rh.columns for col in REQUIRED_RH_COLS):
        raise ValueError(f"Colonnes critiques manquantes après nettoyage. Assurez-vous des en-têtes exacts du fichier RH. Colonnes trouvées: {df_rh.columns.tolist()}")

    # Conversion en liste native Python pour la génération
    list_salarie_ids = df_rh['id_salarie'].unique().tolist() 

    # Récupération des types d'activité
    ACTIVITE_TYPES = df_sportif['type_sport_declare'].dropna().unique().tolist()
    
    if not ACTIVITE_TYPES:
        ACTIVITE_TYPES = ['Course à pied', 'Vélo', 'Marche', 'Natation']
    
    print(f"✅ Fichiers sources chargés. {len(list_salarie_ids)} salariés détectés.")

except FileNotFoundError:
    print(f"⚠️ Erreur : Le fichier '{FICHIER_RH}' ou '{FICHIER_SPORTIF}' n'a pas été trouvé. Assurez-vous qu'ils sont bien présents.")
    # Fallback corrigé (list_salarie_ids est une liste simple)
    list_salarie_ids = [59019, 19841, 56482, 21886, 81001]
    ACTIVITE_TYPES = ['Course à pied', 'Vélo', 'Marche']
except Exception as e:
    print(f"❌ Erreur critique lors du chargement ou du nettoyage des données. Erreur: {e}")
    exit()

# --- 2. Génération de l'Historique Simulé (12 derniers mois) ---
activites = []
end_date = datetime.now().date()
start_date = end_date - timedelta(days=365)

NB_ELIGIBLES_CIBLES = 20
# Pas de .tolist() nécessaire ici car list_salarie_ids est déjà une liste
eligible_ids = random.sample(list_salarie_ids, min(NB_ELIGIBLES_CIBLES, len(list_salarie_ids))) 

for salarie_id in list_salarie_ids:
    
    if salarie_id in eligible_ids:
        nb_activites_a_generer = random.randint(15, 30)
    else:
        nb_activites_a_generer = random.randint(0, 12) 

    for _ in range(nb_activites_a_generer):
        random_date = start_date + timedelta(days=random.randint(0, 365))
        
        activites.append({
            'ID activité': FAKE.uuid4(),
            'ID salarié': salarie_id,
            'Date': random_date.strftime('%Y-%m-%d'),
            'Type d\'activité': random.choice(ACTIVITE_TYPES),
            'Durée (min)': random.randint(20, 120),
            'Distance (km)': round(random.uniform(2, 25), 2),
            'Calories (kcal)': random.randint(150, 1200),
            'Description': FAKE.sentence(nb_words=6)
        })

df_activites = pd.DataFrame(activites)

# --- 3. Enregistrement des Données Simuléés ---
df_activites.to_csv(FICHIER_SIMULE, index=False, encoding='utf-8')
print(f"✅ Fichier '{FICHIER_SIMULE}' généré avec succès ({len(df_activites)} activités).")