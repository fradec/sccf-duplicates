#!/usr/bin/env python3
"""
duplicate_analysis_interactive.py

Usage:
 - Place contacts.csv and doublons.csv in the same folder as this script.
 - Run: python duplicate_analysis_interactive.py
 - Use the interactive menu to normalize / run dedupe rules / quit.

Behavior highlights:
 - Normalized base written as normalized_contacts.csv in the script folder.
 - Outputs go to ./out/ with filenames: <RULE>_<YYYY_MM_DD>_new_pairs.csv and <RULE>_<YYYY_MM_DD>_summary.csv
 - Address-empty rows are excluded for address-based rules.
 - Empty source fields remain empty strings after normalization (no NaN).
"""

import os
import sys
import csv
import itertools
from collections import defaultdict, Counter
from datetime import datetime, date

import pandas as pd
import unicodedata
import re
import gzip
import argparse

# ---------------- configuration ----------------
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
CONTACTS_FILE = os.path.join(SCRIPT_DIR, 'contacts.csv')
DOUBLONS_FILE = os.path.join(SCRIPT_DIR, 'doublons.csv')
NORMALIZED_BASE = os.path.join(SCRIPT_DIR, 'normalized_contacts.csv')
OUT_DIR = os.path.join(SCRIPT_DIR, 'out')
CHUNK_SIZE = 200000
GROUP_THRESHOLD_DEFAULT = 200

# ---------------- helpers ----------------
RE_KEEP_ALNUM_SPACE = re.compile(r'[^0-9A-Z\s]')
RE_SPACE = re.compile(r'\s+')

def remove_accents_and_upper(s: str) -> str:
    s = '' if s is None else str(s)
    s = s.strip()
    if s == '':
        return ''
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = s.upper()
    s = RE_KEEP_ALNUM_SPACE.sub(' ', s)
    s = RE_SPACE.sub(' ', s).strip()
    return s

def normalize_field_for_matching(val):
    if val is None:
        return ''
    v = str(val)
    if v.strip() == '':
        return ''
    return remove_accents_and_upper(v)

def normalize_email(val):
    if val is None:
        return ''
    v = str(val).strip()
    return v

def normalize_phone_digits(val):
    if val is None:
        return ''
    s = str(val)
    digits = re.sub(r'\D+', '', s)
    return digits

# ---------------- rules ----------------
RULES = {
    # --- A : based on SF duplicate rule ---
    'A0': { 'cols': ['SAL', 'FN', 'LN', 'ST3', 'ST4', 'PC', 'CITY'],
            'name': "Individu × Adresse suffisante" },
    'A1': { 'cols': ['LN', 'ST3', 'ST4', 'PC', 'CITY'],
            'name': "Foyer × Adresse suffisante" },

    # --- B : individual variants ---
    'B0': { 'cols': ['SAL', 'FN', 'LN', 'ST1', 'ST2', 'ST3', 'ST4', 'PC', 'CITY'],
            'name': "Individu × Adresse complète" },
    'B1': { 'cols': ['SAL', 'FN', 'LN', 'ST3', 'PC', 'CITY'],
            'name': "Individu × Adresse minimale" },
    'B2': { 'cols': ['SAL', 'FN', 'LN', 'ST3', 'ST4', 'PC', 'CITY', 'EMAIL'],
            'name': "Individu × Adresse suffisante × Email" },
    'B3': { 'cols': ['SAL', 'FN', 'LN', 'ST3', 'ST4', 'PC', 'CITY', 'MOBILE'],
            'name': "Individu × Adresse suffisante × Mobile" },
    'B4': { 'cols': ['SAL', 'FN', 'LN', 'ST3', 'ST4', 'PC', 'CITY', 'EMAIL', 'MOBILE'],
            'name': "Individu × Adresse suffisante × Email × Mobile" },
    'B5': { 'cols': ['SAL', 'FN', 'LN', 'EMAIL'],
            'name': "Individu × Email seul" },
    'B6': { 'cols': ['SAL', 'FN', 'LN', 'MOBILE'],
            'name': "Individu × Mobile seul" },
    'B7': { 'cols': ['SAL', 'FN', 'LN', 'MOBILE', 'HOME'],
            'name': "Individu × Mobile et Home phone" },
    'B8': { 'cols': ['SAL', 'FN', 'LN', 'EMAIL', 'MOBILE'],
            'name': "Individu × Email + Mobile" },
    'B9': { 'cols': ['SAL', 'FN', 'LN', 'EMAIL', 'MOBILE', 'HOME'],
            'name': "Individu × Email + Mobile + Home phone" },

    # --- C : Household variants ---
    'C0': { 'cols': ['LN', 'ST1', 'ST2', 'ST3', 'ST4', 'PC', 'CITY'],
            'name': "Foyer × Adresse complète" },
    'C1': { 'cols': ['LN', 'ST3', 'PC', 'CITY'],
            'name': "Foyer × Adresse minimale" },
    'C2': { 'cols': ['LN', 'ST3', 'ST4', 'PC', 'CITY', 'EMAIL'],
            'name': "Foyer × Adresse suffisante × Email" },
    'C3': { 'cols': ['LN', 'ST3', 'ST4', 'PC', 'CITY', 'MOBILE'],
            'name': "Foyer × Adresse suffisante × Mobile" },
    'C4': { 'cols': ['LN', 'ST3', 'ST4', 'PC', 'CITY', 'EMAIL', 'MOBILE'],
            'name': "Foyer × Adresse suffisante × Email × Mobile" },
    'C5': { 'cols': ['LN', 'EMAIL'],
            'name': "Foyer × Email seul" },
    'C6': { 'cols': ['LN', 'MOBILE'],
            'name': "Foyer × Mobile seul" },
    'C7': { 'cols': ['LN', 'MOBILE', 'HOME'],
            'name': "Foyer × Mobile et Home phone" },
    'C8': { 'cols': ['LN', 'EMAIL', 'MOBILE'],
            'name': "Foyer × Email + Mobile" },
    'C9': { 'cols': ['LN', 'EMAIL', 'MOBILE', 'HOME'],
            'name': "Foyer × Email + Mobile + Home phone" }
}

RULE_ORDER = [
    'A0', 'A1',
    'B0', 'B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B9',
    'C0', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9'
]

# ---------------- IO helpers ----------------
def ensure_outdir():
    os.makedirs(OUT_DIR, exist_ok=True)

def date_token():
    return date.today().strftime('%Y_%m_%d')

def write_csv(df, path, compress=False):
    if compress:
        gz = path + '.gz'
        with gzip.open(gz, 'wt', newline='') as f:
            df.to_csv(f, index=False)
        return gz
    else:
        df.to_csv(path, index=False)
        return path

# ---------------- normalization ----------------
def normalize_contacts_to_base(contacts_csv=CONTACTS_FILE, out_base=NORMALIZED_BASE, chunk_size=CHUNK_SIZE):
    if not os.path.exists(contacts_csv):
        raise FileNotFoundError(f'contacts file not found: {contacts_csv}')
    # read in chunks, ensure empty stays empty (keep_default_na=False)
    reader = pd.read_csv(contacts_csv, dtype=str, chunksize=chunk_size, keep_default_na=False)
    first = True
    written = 0
    for i, chunk in enumerate(reader):
        # ensure columns exist even if missing in some file variants
        # parse CreatedDate
        chunk['CreatedDate_parsed'] = pd.to_datetime(chunk.get('CreatedDate', pd.NaT), errors='coerce', utc=True)
        # normalized fields (including ST1/ST2 for future use)
        chunk['LN'] = chunk.get('LastNameSearchable__c', '').apply(normalize_field_for_matching)
        chunk['FN'] = chunk.get('FirstNameSearchable__c', '').apply(normalize_field_for_matching)
        chunk['ST1'] = chunk.get('MailingStreet1__c', '').apply(normalize_field_for_matching)
        chunk['ST2'] = chunk.get('MailingStreet2__c', '').apply(normalize_field_for_matching)
        chunk['ST3'] = chunk.get('MailingStreet3__c', '').apply(normalize_field_for_matching)
        chunk['ST4'] = chunk.get('MailingStreet4__c', '').apply(normalize_field_for_matching)
        # postal code and city
        chunk['PC'] = chunk.get('MailingPostalCode', '').apply(lambda x: str(x).strip() if x is not None else '')
        chunk['CITY'] = chunk.get('MailingCity', '').apply(normalize_field_for_matching)
        chunk['EMAIL'] = chunk.get('Email', '').apply(normalize_email)
        chunk['MOBILE'] = chunk.get('MobilePhone', '').apply(normalize_phone_digits)
        chunk['HOME'] = chunk.get('HomePhone', '').apply(normalize_phone_digits)
        chunk['SAL'] = chunk.get('Salutation', '').apply(lambda x: str(x).strip() if x is not None else '')
        # turn CreatedDate_parsed to ISO string or empty string
        chunk['CreatedDate_parsed'] = chunk['CreatedDate_parsed'].apply(lambda dt: dt.isoformat() if pd.notna(dt) else '')
        # ensure no NaN remain in normalized cols (make them empty strings)
        norm_cols = ['LN','FN','ST1','ST2','ST3','ST4','PC','CITY','EMAIL','MOBILE','HOME','SAL','CreatedDate_parsed']
        for c in norm_cols:
            if c not in chunk.columns:
                chunk[c] = ''
            chunk[c] = chunk[c].fillna('').astype(str)
        # Write incrementally
        if first:
            chunk.to_csv(out_base, index=False, quoting=csv.QUOTE_MINIMAL)
            first = False
        else:
            chunk.to_csv(out_base, index=False, mode='a', header=False, quoting=csv.QUOTE_MINIMAL)
        written += len(chunk)
        print(f'Normalized chunk {i+1}, cumulative rows: {written}')
    print(f'Normalization done, total rows: {written}')
    return out_base

# ---------------- doublons loader ----------------
def load_doublons(doublons_csv=DOUBLONS_FILE):
    if not os.path.exists(doublons_csv):
        raise FileNotFoundError(f'doublons file not found: {doublons_csv}')
    df = pd.read_csv(doublons_csv, dtype=str, keep_default_na=False)
    pair_status_counts = defaultdict(Counter)
    for _, r in df.iterrows():
        a = r.get('ContactPrincipal__c', '').strip()
        b = r.get('ContactDoublon__c', '').strip()
        statut = r.get('Statut__c', '').strip()
        if a == '' or b == '':
            continue
        pair_status_counts[(a,b)][statut] += 1
    return pair_status_counts

# ---------------- matching helpers ----------------
def build_match_key(row, cols):
    parts = []
    for c in cols:
        v = row.get(c, '')
        v = '' if v is None else str(v).strip()
        # treat literal 'NAN' (case-insensitive) as empty
        if v.upper() == 'NAN' or v == '':
            continue
        # normalize and remove all spaces for compact key
        try:
            norm = remove_accents_and_upper(v).replace(' ', '')
        except Exception:
            norm = re.sub(r'\s+', '', v.upper())
        if norm == '':
            continue
        parts.append(norm)
    # join parts without delimiter to produce compact key
    return ''.join(parts)

def has_address(row):
    # address present if any ST1/ST2/ST3/ST4/PC/CITY non-empty
    for c in ('ST1','ST2','ST3','ST4','PC','CITY'):
        v = row.get(c, '')
        if v is None:
            continue
        if str(v).strip() != '':
            return True
    return False

def has_email(row):
    v = row.get('EMAIL', '')
    return isinstance(v, str) and v.strip() != ''

def has_mobile(row):
    v = row.get('MOBILE', '')
    return isinstance(v, str) and v.strip() != ''

def has_home_phone(row):
    v = row.get('HOME', '')
    return isinstance(v, str) and v.strip() != ''


# ---------------- rule processing ----------------
def process_rule(rule_key, normalized_base=NORMALIZED_BASE, doublons_map=None,
                 group_threshold=GROUP_THRESHOLD_DEFAULT, write_contacts=False, compress=False):
    if doublons_map is None:
        doublons_map = {}
    cols = RULES[rule_key]['cols']
    name = RULES[rule_key]['name']
    print(f'Running rule {rule_key}: {name}')
    df = pd.read_csv(normalized_base, dtype=str, keep_default_na=False)
    # ensure normalized cols exist
    for c in ['LN','FN','ST1','ST2','ST3','ST4','PC','CITY','EMAIL','MOBILE','HOME','SAL','CreatedDate_parsed','Id']:
        if c not in df.columns:
            df[c] = ''
    # build match key
    df['match_key'] = df.apply(lambda r: build_match_key(r, cols), axis=1)

    # --- pre-filtering according to the types of data used by the rule ---
    df_rule = df.copy()

    # Si la règle inclut des éléments d'adresse, on exclut ceux sans adresse
    if any(c in cols for c in ('ST1','ST2','ST3','ST4','PC','CITY')):
        df_rule['has_address'] = df_rule.apply(has_address, axis=1)
        df_rule = df_rule[df_rule['has_address']].copy()

    # If the rule uses EMAIL
    if 'EMAIL' in cols:
        df_rule = df_rule[df_rule.apply(has_email, axis=1)].copy()

    # If the rule uses MOBILE
    if 'MOBILE' in cols:
        df_rule = df_rule[df_rule.apply(has_mobile, axis=1)].copy()

    # If the rule uses HOME
    if 'HOME' in cols:
        df_rule = df_rule[df_rule.apply(has_home_phone, axis=1)].copy()

    # Exclude lines without a match key
    df_rule = df_rule[df_rule['match_key'] != ''].copy()

    # Nothing to do
    if df_rule.empty:
        print(f'No records eligible for rule {rule_key}.')
        summary = {
            'rule': rule_key, 'name': name, 'contacts': 0, 'pairs': 0,
            'already_declared': 0, 'new_pairs': 0, 'status_distribution': {}, 'groups_too_large': 0
        }
        filename = f'{rule_key}_{date_token()}_summary.csv'
        write_csv(pd.DataFrame([summary]), os.path.join(OUT_DIR, filename), compress=compress)
        return summary

    contacts_involved = df_rule['Id'].unique()
    n_contacts = len(contacts_involved)

    grouped = df_rule.groupby('match_key')
    pairs = []
    groups_too_large = []
    for k, g in grouped:
        gsize = len(g)
        if gsize < 2:
            continue
        if gsize > group_threshold:
            groups_too_large.append({'match_key': k, 'group_size': gsize})
            continue
        rows = g.to_dict('records')
        # select single principal: oldest CreatedDate_parsed (tie-breaker: Id lexicographic)
        def _get_time(r):
            d = r.get('CreatedDate_parsed', '')
            try:
                return pd.to_datetime(d) if d != '' else pd.NaT
            except Exception:
                return pd.NaT
        principal_row = None
        principal_time = pd.NaT
        for r in rows:
            t = _get_time(r)
            if principal_row is None:
                principal_row = r
                principal_time = t
                continue
            if pd.isna(principal_time) and pd.isna(t):
                if r.get('Id', '') < principal_row.get('Id', ''):
                    principal_row = r
                    principal_time = t
            elif pd.isna(principal_time):
                principal_row = r
                principal_time = t
            elif pd.isna(t):
                continue
            else:
                if t < principal_time:
                    principal_row = r
                    principal_time = t
        pr_id = principal_row.get('Id', '')
        pr_created = principal_row.get('CreatedDate_parsed', '')
        for r in rows:
            rid = r.get('Id', '')
            if rid == pr_id:
                continue
            du_id = rid
            du_created = r.get('CreatedDate_parsed', '')
            pairs.append((pr_id, du_id, k, gsize, pr_created, du_created))

    n_pairs = len(pairs)
    already_declared = 0
    declared_status_counter = Counter()
    new_pairs = []
    for pr, du, key, gsize, pdts, ddts in pairs:
        status_counter = doublons_map.get((pr, du), None)
        if status_counter is not None and sum(status_counter.values()) > 0:
            already_declared += 1
            for st, c in status_counter.items():
                declared_status_counter[st] += c
            continue
        inv = doublons_map.get((du, pr), None)
        if inv is not None and sum(inv.values()) > 0:
            already_declared += 1
            declared_status_counter['declared_inverse'] += sum(inv.values())
            continue
        new_pairs.append({'Principal': pr, 'Doublon': du, 'Rule': rule_key, 'MatchKey': key, 'GroupSize': gsize,
                          'PrincipalCreatedDate': pdts, 'DoublonCreatedDate': ddts})

    # write outputs according to naming convention
    ensure_outdir()
    dtok = date_token()
    pairs_fname = f'{rule_key}_{dtok}_new_pairs.csv'
    summary_fname = f'{rule_key}_{dtok}_summary.csv'

    if new_pairs:
        df_new = pd.DataFrame(new_pairs)
        write_csv(df_new, os.path.join(OUT_DIR, pairs_fname), compress=False)
    else:
        write_csv(pd.DataFrame(columns=['Principal','Doublon','Rule','MatchKey','GroupSize','PrincipalCreatedDate','DoublonCreatedDate']),
                  os.path.join(OUT_DIR, pairs_fname), compress=False)

    summary_obj = {
        'rule': rule_key,
        'name': name,
        'contacts': int(n_contacts),
        'pairs': int(n_pairs),
        'already_declared': int(already_declared),
        'new_pairs': int(len(new_pairs)),
        'status_distribution': dict(declared_status_counter),
        'groups_too_large': len(groups_too_large)
    }
    write_csv(pd.DataFrame([summary_obj]), os.path.join(OUT_DIR, summary_fname), compress=False)
    # optionally write contacts per rule (disabled by default)
    if write_contacts:
        contacts_fname = f'{rule_key}_{dtok}_contacts.csv'
        # select columns useful for inspection
        cols_to_write = ['Id','CreatedDate','CreatedDate_parsed','LN','FN','ST1','ST2','ST3','ST4','PC','CITY','EMAIL','MOBILE','HOME','SAL','match_key']
        df_rule.to_csv(os.path.join(OUT_DIR, contacts_fname), index=False)

    # if groups too large, write a file for review
    if groups_too_large:
        gfname = f'{rule_key}_{dtok}_groups_too_large.csv'
        write_csv(pd.DataFrame(groups_too_large), os.path.join(OUT_DIR, gfname), compress=False)

    return summary_obj

# ---------------- interactive menu ----------------
def print_main_menu():
    print('\n=== Menu ===')
    print('1) Préparation : normaliser les contacts')
    print('2) Jouer tout A : duplicate rule SF')
    print('3) Jouer tout B : variantes individu)')
    print('4) Jouer tout C : variantes foyer')
    print('5) Jouer A, B et C')
    print('Q) Quitter')
    print('==============================')

def run_rule_group(prefixes, args):
    """Run all rules whose keys start with one of the given prefixes (A, B, C...)."""
    if not os.path.exists(NORMALIZED_BASE):
        print('Base normalisée introuvable. Veuillez exécuter d\'abord l\'option 1 (ou placer le fichier normalized_contacts.csv à côté du script).')
        return
    if not os.path.exists(DOUBLONS_FILE):
        print(f'Fichier doublons manquant: {DOUBLONS_FILE}. Arrêt.')
        return

    doublons_map = load_doublons()
    selected = [rk for rk in RULE_ORDER if any(rk.startswith(p) for p in prefixes)]
    if not selected:
        print(f'Aucune règle trouvée pour les préfixes {prefixes}')
        return

    print(f"Lancement des règles : {', '.join(selected)}")
    for rk in selected:
        s = process_rule(
            rk,
            normalized_base=NORMALIZED_BASE,
            doublons_map=doublons_map,
            group_threshold=args.group_threshold,
            write_contacts=args.write_contacts,
            compress=False
        )
        print(f'Rule {rk} summary: contacts={s["contacts"]} pairs={s["pairs"]} '
              f'new_pairs={s["new_pairs"]} already_declared={s["already_declared"]}')
    print('Done running selected rules.\n')

def main_loop():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--write-contacts', action='store_true', help='Write per-rule contacts files into out/')
    parser.add_argument('--group-threshold', type=int, default=GROUP_THRESHOLD_DEFAULT, help='Group threshold')
    args, _ = parser.parse_known_args()

    ensure_outdir()

    while True:
        print_main_menu()
        choice = input('Choix : ').strip().lower()

        if choice == 'q':
            print('Au revoir.')
            break

        elif choice == '1':
            try:
                print('Normalisation des contacts...')
                normalize_contacts_to_base()
                print(f'Base normalisée écrite dans : {NORMALIZED_BASE}')
            except FileNotFoundError as e:
                print('ERREUR :', e)

        elif choice == '2':
            run_rule_group(['A'], args)

        elif choice == '3':
            run_rule_group(['B'], args)

        elif choice == '4':
            run_rule_group(['C'], args)

        elif choice == '5':
            run_rule_group(['A', 'B', 'C'], args)

        else:
            print('Choix invalide, réessaie.')


if __name__ == '__main__':
    main_loop()
