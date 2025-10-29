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
    'A': { 'cols': ['LN', 'FN', 'ST3', 'ST4', 'PC', 'CITY'], 'name': "Individu x Adresse (ST3+ST4+PC+City)" },
    'B': { 'cols': ['LN', 'FN', 'ST3', 'PC', 'CITY'], 'name': "Individu x Adresse (sans Street4)" },
    'C': { 'cols': ['LN', 'ST3', 'ST4', 'PC', 'CITY'], 'name': "Foyer x Adresse (nom foyer + adresse)" },
    'D': { 'cols': ['LN', 'ST3', 'ST4', 'PC', 'CITY', 'EMAIL'], 'name': "Foyer x Adresse x Email" },
    'E': { 'cols': ['SAL', 'LN', 'ST3', 'ST4', 'PC', 'CITY', 'EMAIL'], 'name': "Foyer+Cvl x Adresse x Email" },
    'F': { 'cols': ['SAL', 'LN', 'ST3', 'ST4', 'PC', 'CITY', 'EMAIL', 'MOBILE'], 'name': "Foyer+Cvl x Adresse x Email x Mobile" },
    'G': { 'cols': ['EMAIL'], 'name': "Email seul" },
    'H': { 'cols': ['MOBILE'], 'name': "Mobile seul" },
    'I': { 'cols': ['MOBILE', 'HOME'], 'name': "Mobile et home phone"}
}
RULE_ORDER = ['A','B','C','D','E','F','G','H','I']

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
    for c in ['LN','FN','ST1','ST2','ST3','ST4','PC','CITY','EMAIL','MOBILE','SAL','CreatedDate_parsed','Id']:
        if c not in df.columns:
            df[c] = ''
    # build match key
    df['match_key'] = df.apply(lambda r: build_match_key(r, cols), axis=1)
    # if rule uses address elements, exclude rows with no address
    address_rule = any(c in cols for c in ('ST1','ST2','ST3','ST4','PC','CITY'))
    if address_rule:
        df['has_address'] = df.apply(has_address, axis=1)
        df_rule = df[(df['match_key'] != '') & (df['has_address'])].copy()
    else:
        df_rule = df[df['match_key'] != ''].copy()
    # email/mobile only rules require presence
    if rule_key == 'G':
        df_rule = df_rule[df_rule['EMAIL'] != ''].copy()
    if rule_key == 'H':
        df_rule = df_rule[df_rule['MOBILE'] != ''].copy()
    if rule_key == 'I':
        df_rule = df_rule[(df_rule['MOBILE'] != '') & (df_rule['HOME'] != '')].copy()

    if df_rule.empty:
        print(f'No records eligible for rule {rule_key}.')
        # write empty summary file
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
        # generate pairs principal -> others
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
    print('\n=== Duplicate analysis menu ===')
    print('1) Normalize contacts -> produce normalized_contacts.csv')
    print('2) Run deduplication rules (choose which rules to run)')
    print('3) Run all rules')
    print('Q) Quit')
    print('==============================')

def choose_rules_interactive():
    print('\nChoose one or multiple rules (comma separated), or ALL to run all rules:')
    for k in RULE_ORDER:
        print(f'  {k}: {RULES[k]["name"]}')
    s = input('Rules (e.g. A,E or ALL): ').strip()
    if s.upper() == 'ALL':
        return RULE_ORDER
    keys = [x.strip().upper() for x in s.split(',') if x.strip()]
    keys = [k for k in keys if k in RULES]
    return keys

def main_loop():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--write-contacts', action='store_true', help='Write per-rule contacts files into out/')
    parser.add_argument('--group-threshold', type=int, default=GROUP_THRESHOLD_DEFAULT, help='Group threshold')
    args, _ = parser.parse_known_args()

    ensure_outdir()

    while True:
        print_main_menu()
        choice = input('Choice: ').strip()
        if choice.lower() == 'q':
            print('Bye.')
            break
        if choice == '1':
            # normalization
            try:
                print('Normalizing contacts...')
                normalize_contacts_to_base()
                print(f'Normalized base written to: {NORMALIZED_BASE}')
            except FileNotFoundError as e:
                print('ERROR:', e)
        elif choice == '2':
            # run chosen rules
            # check files exist
            if not os.path.exists(NORMALIZED_BASE):
                print('Normalized base not found. Please run option 1 first (or put normalized_contacts.csv next to script).')
                continue
            if not os.path.exists(DOUBLONS_FILE):
                print(f'doublons source missing: {DOUBLONS_FILE}. Aborting rule run.')
                continue
            rules = choose_rules_interactive()
            if not rules:
                print('No valid rules selected.')
                continue
            doublons_map = load_doublons()
            for rk in rules:
                s = process_rule(rk, normalized_base=NORMALIZED_BASE, doublons_map=doublons_map,
                                 group_threshold=args.group_threshold, write_contacts=args.write_contacts, compress=False)
                print(f'Rule {rk} summary: contacts={s["contacts"]} pairs={s["pairs"]} new_pairs={s["new_pairs"]} already_declared={s["already_declared"]}')
            print('Done running selected rules.')
        elif choice == '3':
            # all rules
            if not os.path.exists(NORMALIZED_BASE):
                print('Normalized base not found. Please run option 1 first (or put normalized_contacts.csv next to script).')
                continue
            if not os.path.exists(DOUBLONS_FILE):
                print(f'doublons source missing: {DOUBLONS_FILE}. Aborting rule run.')
                continue
            doublons_map = load_doublons()
            for rk in RULE_ORDER:
                s = process_rule(rk, normalized_base=NORMALIZED_BASE, doublons_map=doublons_map,
                                 group_threshold=args.group_threshold, write_contacts=args.write_contacts, compress=False)
                print(f'Rule {rk} summary: contacts={s["contacts"]} pairs={s["pairs"]} new_pairs={s["new_pairs"]} already_declared={s["already_declared"]}')
            print('Done running all rules.')
        else:
            print('Unknown choice, try again.')

if __name__ == '__main__':
    main_loop()
