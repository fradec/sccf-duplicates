#!/usr/bin/env python3
"""
analyze_pairs.py

Analyse des fichiers *_new_pairs.csv dans ./out/ :
1) Comptage annuel des doublons par règle
2) Croisement global des doublons (redondance inter-règles)
3) Résumé du fichier cross_analysis.csv (avec filtre optionnel sur le nombre minimal de règles)
"""

import os
import glob
import pandas as pd
import ast

OUT_DIR = 'out'
ANALYSIS_DIR = 'analysis'
MIN_YEAR = 2021
os.makedirs(ANALYSIS_DIR, exist_ok=True)


def get_year_safe(date_str):
    if not date_str or date_str.strip() == '':
        return None
    try:
        y = pd.to_datetime(date_str).year
        return y if y > MIN_YEAR else MIN_YEAR
    except Exception:
        return None


def analyze_by_year():
    pattern = os.path.join(OUT_DIR, '*_new_pairs.csv')
    files = glob.glob(pattern)
    if not files:
        print("Aucun fichier *_new_pairs.csv trouvé dans 'out/'.")
        return

    all_data = []
    for f in files:
        rule_name = os.path.basename(f).split('_')[0]
        df = pd.read_csv(f, dtype=str, keep_default_na=False)
        if df.empty:
            continue

        df['Year'] = df['DoublonCreatedDate'].apply(get_year_safe)
        df = df[df['Year'].notna()]
        counts = df.groupby('Year').size().reset_index(name='NewPairs')
        counts['Rule'] = rule_name
        all_data.append(counts)

    if not all_data:
        print("Aucune donnée valide trouvée.")
        return

    result = pd.concat(all_data, ignore_index=True)
    result = result[['Rule', 'Year', 'NewPairs']].sort_values(['Rule', 'Year'])
    out_csv = os.path.join(ANALYSIS_DIR, 'millesime.csv')
    result.to_csv(out_csv, index=False)
    print(f"Analyse annuelle terminée → {out_csv}")


def analyze_cross_rules():
    pattern = os.path.join(OUT_DIR, '*_new_pairs.csv')
    files = glob.glob(pattern)
    if not files:
        print("Aucun fichier *_new_pairs.csv trouvé.")
        return

    all_pairs = []
    for f in files:
        rule_name = os.path.basename(f).split('_')[0]
        df = pd.read_csv(f, dtype=str, keep_default_na=False)
        if df.empty:
            continue

        df['Year'] = df['DoublonCreatedDate'].apply(get_year_safe)
        df = df[df['Year'].notna()]
        df_sub = df[['Principal', 'Doublon', 'Year']].copy()
        df_sub['Rule'] = rule_name
        all_pairs.append(df_sub)

    if not all_pairs:
        print("Aucune donnée valide trouvée pour analyse croisée.")
        return

    df_all = pd.concat(all_pairs, ignore_index=True)
    grouped = (
        df_all.groupby(['Principal', 'Doublon', 'Year'])
        .agg(
            Occurrences=('Rule', 'count'),
            Rules=('Rule', lambda x: sorted(list(set(x))))
        )
        .reset_index()
    )

    out_csv = os.path.join(ANALYSIS_DIR, 'cross_analysis.csv')
    grouped.to_csv(out_csv, index=False)
    print(f"Analyse croisée terminée → {out_csv}")
    print(f"{len(grouped)} paires uniques consolidées.")


def summarize_cross_analysis(min_rules: int = 1):
    """Résumé global : volume annuel, % du total, et pondération par occurrences, avec filtre sur le nombre minimal de règles."""
    cross_file = os.path.join(ANALYSIS_DIR, 'cross_analysis.csv')
    if not os.path.exists(cross_file):
        print("Fichier cross_analysis.csv introuvable. Lance d'abord l'option 2.")
        return

    df = pd.read_csv(cross_file, dtype={'Year': int, 'Occurrences': int})
    if df.empty:
        print("Fichier vide.")
        return

    # convertir la colonne 'Rules' en liste réelle (si présente sous forme de texte)
    if 'Rules' in df.columns and isinstance(df.iloc[0]['Rules'], str):
        try:
            df['Rules'] = df['Rules'].apply(ast.literal_eval)
        except Exception:
            pass

    # calculer le nombre de règles distinctes par ligne si pas présent
    if 'Rules' in df.columns:
        df['NbRules'] = df['Rules'].apply(lambda x: len(x) if isinstance(x, (list, set)) else 0)
    else:
        df['NbRules'] = df['Occurrences']

    # appliquer le filtre
    before = len(df)
    df = df[df['NbRules'] >= min_rules]
    after = len(df)
    print(f"Filtre appliqué : paires avec au moins {min_rules} règle(s) ({after}/{before} conservées)")

    # Résumé par année
    summary = (
        df.groupby('Year')
        .agg(
            PairesUniques=('Occurrences', 'size'),
            TotalOccurrences=('Occurrences', 'sum')
        )
        .reset_index()
    )

    total_pairs = summary['PairesUniques'].sum()
    summary['%Paires'] = (summary['PairesUniques'] / total_pairs * 100).round(2)
    summary['RedondanceMoy'] = (summary['TotalOccurrences'] / summary['PairesUniques']).round(2)

    out_csv = os.path.join(ANALYSIS_DIR, f'cross_summary_min{min_rules}.csv')
    summary.to_csv(out_csv, index=False)
    print(f"Résumé global écrit dans : {out_csv}")
    print(summary.to_string(index=False))


def print_menu():
    print("\n=== Menu d'analyse ===")
    print("1) Comptage annuel des doublons par règle")
    print("2) Analyse croisée des doublons")
    print("3) Résumé du fichier cross_analysis.csv (avec filtre sur le nombre minimal de règles)")
    print("Q) Quitter")
    print("========================")


def main():
    while True:
        print_menu()
        choice = input("Choix : ").strip().lower()
        if choice == '1':
            analyze_by_year()
        elif choice == '2':
            analyze_cross_rules()
        elif choice == '3':
            val = input("Nombre minimal de règles à considérer (par défaut = 1) : ").strip()
            min_rules = int(val) if val.isdigit() else 1
            summarize_cross_analysis(min_rules)
        elif choice == 'q':
            print("Au revoir.")
            break
        else:
            print("Choix invalide.")


if __name__ == '__main__':
    main()
