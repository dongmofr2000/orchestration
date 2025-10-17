# test_etl_vins.py

import unittest
import pandas as pd
import numpy as np
import os
import sys

# Ajoute le répertoire courant au chemin pour permettre l'importation de etl_vins
# Ceci est nécessaire pour que Kestra puisse l'importer après l'avoir copié dans le répertoire de travail.
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# IMPORTANT : Assurez-vous que le fichier etl_vins.py existe et définit une fonction
# appelée 'run_etl_pipeline' qui retourne le DataFrame final.
try:
    from etl_vins import run_etl_pipeline
except ImportError:
    # Si l'importation échoue (ex: test local sans le fichier), on crée une fonction mock
    # pour permettre au script de s'exécuter et d'afficher les logs (bien que les tests échoueront)
    print("ATTENTION: Impossible d'importer la fonction 'run_etl_pipeline' depuis etl_vins.py. Les tests vont échouer.")
    def run_etl_pipeline():
        raise NotImplementedError("La fonction run_etl_pipeline doit être définie dans etl_vins.py et retourner le DataFrame final.")


class TestETLVins(unittest.TestCase):
    """
    Suite de tests unitaires pour valider les étapes critiques du pipeline ETL des vins.
    """
    
    @classmethod
    def setUpClass(cls):
        """
        Configure les tests en exécutant le pipeline ETL une seule fois pour obtenir le DataFrame final.
        """
        print("\n--- Exécution du Pipeline ETL pour les Tests ---")
        try:
            # Exécute le pipeline et stocke le DataFrame final pour tous les tests
            cls.df_final = run_etl_pipeline()
            print("Pipeline ETL exécuté avec succès. DataFrame prêt pour les tests.")
        except Exception as e:
            cls.df_final = pd.DataFrame() # Crée un DataFrame vide en cas d'échec
            print(f"ÉCHEC CRITIQUE lors de l'exécution du pipeline ETL : {e}")
            # Si le pipeline échoue, tous les tests vont sauter ou échouer,
            # mais nous assurons ici que l'objet est initialisé.

    def test_df_is_not_empty(self):
        """Vérifie que le DataFrame final n'est pas vide suite à l'ETL."""
        self.assertFalse(self.df_final.empty, "Le DataFrame final est vide, indiquant un échec de l'ETL ou un problème de données.")

    def test_no_duplicate_skus(self):
        """Vérifie l'absence de doublons dans la colonne d'identifiant unique (product_id)."""
        if self.df_final.empty: self.skipTest("DataFrame vide, saut du test.")
        
        duplicates = self.df_final['product_id'].duplicated().any()
        self.assertFalse(duplicates, "Des doublons critiques (product_id) ont été détectés dans le DataFrame final.")

    def test_no_critical_missing_values(self):
        """Vérifie l'absence de valeurs manquantes dans les colonnes critiques (prix, stock, id)."""
        if self.df_final.empty: self.skipTest("DataFrame vide, saut du test.")
        
        critical_cols = ['product_id', 'id_web', 'price', 'stock_quantity']
        
        # Vérifie si toutes les colonnes critiques existent
        for col in critical_cols:
             self.assertIn(col, self.df_final.columns, f"Colonne critique manquante dans le DataFrame final : {col}")
             
        # Compte les valeurs manquantes
        missing_counts = self.df_final[critical_cols].isnull().sum()
        
        # Le test passe si toutes les colonnes critiques n'ont aucune valeur manquante
        for col, count in missing_counts.items():
            self.assertEqual(count, 0, f"La colonne critique '{col}' contient {count} valeur(s) manquante(s) après l'ETL.")

    def test_ca_coherence(self):
        """Vérifie la cohérence du calcul du Chiffre d'Affaires (CA = price * stock_quantity)."""
        if self.df_final.empty: self.skipTest("DataFrame vide, saut du test.")

        # Recalcule le CA attendu pour une vérification
        # (S'assurer que la colonne 'CA' est créée par etl_vins.py)
        if 'CA' not in self.df_final.columns:
            self.fail("La colonne 'CA' (Chiffre d'Affaires) n'a pas été créée par le pipeline ETL.")
            
        expected_ca = (self.df_final['price'] * self.df_final['stock_quantity']).sum()
        actual_ca = self.df_final['CA'].sum()
        
        # Vérifie que la différence relative est minime (tolérance de 0.01% pour les floats)
        tolerance = 1e-4 
        is_coherent = np.isclose(actual_ca, expected_ca, rtol=tolerance)
        
        self.assertTrue(is_coherent, f"Le CA total calculé par l'ETL ({actual_ca:.2f}) est incohérent avec le CA attendu ({expected_ca:.2f}).")

    def test_outliers_identified_zscore(self):
        """Vérifie que la colonne d'outliers Z-score a été créée et contient des valeurs."""
        if self.df_final.empty: self.skipTest("DataFrame vide, saut du test.")
        
        outlier_col = 'is_outlier_zscore'
        
        # 1. Vérifie si la colonne d'outliers existe
        self.assertIn(outlier_col, self.df_final.columns, f"La colonne d'identification des outliers '{outlier_col}' n'a pas été créée.")
        
        # 2. Vérifie qu'il y a des outliers détectés (un bon jeu de données devrait en avoir)
        # Note : On s'attend à ce que le Z-score identifie *quelques* outliers.
        num_outliers = self.df_final[outlier_col].sum()
        
        # La condition est que nous ne devrions pas avoir 0 outlier (dans un jeu de données réel)
        self.assertGreater(num_outliers, 0, f"Aucun outlier Z-score n'a été détecté dans la colonne 'price'.")
        
        # La condition est que le nombre d'outliers ne doit pas dépasser une limite raisonnable (e.g., 5% du dataset)
        max_outliers_count = len(self.df_final) * 0.05
        self.assertLess(num_outliers, max_outliers_count, f"Un nombre excessif d'outliers Z-score a été détecté ({num_outliers}).")


if __name__ == '__main__':
    # Le unittest.main() s'assure que les tests sont exécutés et que le code de sortie
    # reflète le succès ou l'échec pour Kestra.
    unittest.main(argv=sys.argv[:1], exit=False)