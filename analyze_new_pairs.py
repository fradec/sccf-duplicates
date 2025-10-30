#!/usr/bin/env python3
"""
analyze_pairs.py

Analyse des fichiers *_new_pairs.csv dans ./out/ pour compter les doublons par année de création.
"""

import os
import glob
import pandas as pd

OUT_DIR = 'out'
ANALYSIS_DIR = 'analysis'
MIN_YEAR = 2021  # année de production / regroupement

os.makedirs(ANALYSIS_DIR, exist_ok=True)

# Recherche tous les fichiers *_new_pairs.csv
pattern = os.path.join(OUT_DIR, '*_new_pairs.csv')
files = glob.glob(pattern)

if not files:
    print("Aucun fichier *_new_pairs.csv trouvé dans 'out/'.")
    exit(0)

all_data = []

for f in files:
    # Déduction de la règle depuis le nom de fichier : <RULE>_YYYY_MM_DD_new_pairs.csv
    rule_name = os.path.basename(f).split('_')[0]
    df = pd.read_csv(f, dtype=str, keep_default_na=False)

    if df.empty:
        continue

    # on prend l'année de DoublonCreatedDate
    def get_year(d):
        if not d or d.strip() == '':
            return None
        try:
            y = pd.to_datetime(d).year
            return y if y > MIN_YEAR else MIN_YEAR
        except Exception:
            return None

    df['Year'] = df['DoublonCreatedDate'].apply(get_year)
    # on ignore les lignes sans année valide
    df = df[df['Year'].notna()]
    counts = df.groupby('Year').size().reset_index(name='NewPairs')
    counts['Rule'] = rule_name
    all_data.append(counts)

if not all_data:
    print("Aucune donnée valide trouvée dans les fichiers *_new_pairs.csv.")
    exit(0)

result = pd.concat(all_data, ignore_index=True)
# réorganisation des colonnes
result = result[['Rule','Year','NewPairs']]
# tri
result = result.sort_values(['Rule','Year'])

out_csv = os.path.join(ANALYSIS_DIR, 'millesime.csv')
result.to_csv(out_csv, index=False)
print(f"Analyse terminée, fichier écrit dans : {out_csv}")
