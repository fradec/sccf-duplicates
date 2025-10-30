# Duplicate Analysis

## Objectif

Ce script permet d’analyser et de détecter les doublons dans un fichier **contacts.csv** à partir d’un fichier de référence **doublons.csv**.
Il repose sur un processus en deux étapes :

1. **Normalisation** : nettoyage et homogénéisation des données de contact.
2. **Évaluation des règles de déduplication** : comparaison des enregistrements selon différents critères.


## Setup

Télécharger les contacts depuis salesforce dans `contacts.csv`:

```SQL
SELECT Id, CreatedDate, IdPersonne__c, AccountId, Salutation, FirstName, LastName, FirstNameSearchable__c, LastNameSearchable__c, MailingStreet1__c, MailingStreet2__c, MailingStreet3__c, MailingStreet4__c, MailingPostalCode, MailingCity, MailingCountry, HomePhone, MobilePhone, Email, Est_un_doublon__c, TECH_IsMerged__c, TECH_SFIdPrincipal__c, InformationDonateur__c, Sphere__c, TypeActeurs__c, IdSCCFContact__c, Statut__c
FROM Contact
WHERE RecordType.DeveloperName = 'Acteurs' AND IdPersonne__c LIKE 'P%'
```

Télécharger les doublons potentiels depuis salesforce dans `doublons.csv`:
```SQL
SELECT Id, CoherenceDesDoublons__c, CreatedDate, Statut__c, DateDeDernierTraitement__c, ContactPrincipal__c, ContactDoublon__c
FROM Doublon_potentiel__c
WHERE ContactPrincipal__c != null
	AND ContactDoublon__c != null
```


## Utilisation

### Commande

run `python3 duplicate_analysis.py`


## Étape 1 — Normalisation

Le script crée un fichier `normalized_contacts.csv` contenant les champs nettoyés et enrichis.
La normalisation inclut :

| Champ    | Source                   | Normalisation                                      |
| -------- | ------------------------ | -------------------------------------------------- |
| `LN`     | `LastNameSearchable__c`  | Majuscules, accents supprimés, ponctuation retirée |
| `FN`     | `FirstNameSearchable__c` | Idem                                               |
| `ST1`    | `MailingStreet1__c`      | Idem                                               |
| `ST2`    | `MailingStreet2__c`      | Idem                                               |
| `ST3`    | `MailingStreet3__c`      | Idem                                               |
| `ST4`    | `MailingStreet4__c`      | Idem                                               |
| `PC`     | `MailingPostalCode`      | Trim                                               |
| `CITY`   | `MailingCity`            | Majuscules, accents supprimés                      |
| `EMAIL`  | `Email`                  | Trim uniquement                                    |
| `MOBILE` | `MobilePhone`            | Digits only                                        |
| `HOME`   | `HomePhone`              | Digits only                                        |
| `SAL`    | `Salutation`             | Trim                                               |


## Étape 2 — Évaluation des règles

Chaque règle de matching compare les enregistrements sur un sous-ensemble de champs normalisés.

| Règle  | Description                                    | Champs utilisés                                                  |
| ------ | ---------------------------------------------- | ---------------------------------------------------------------- |
| **A0** | Individu × Adresse suffisante                  | `SAL`, `FN`, `LN`, `ST3`, `ST4`, `PC`, `CITY`                    |
| **A1** | Foyer × Adresse suffisante                     | `LN`, `ST3`, `ST4`, `PC`, `CITY`                                 |
| **B0** | Individu × Adresse complète                    | `SAL`, `FN`, `LN`, `ST1`, `ST2`, `ST3`, `ST4`, `PC`, `CITY`      |
| **B1** | Individu × Adresse minimale                    | `SAL`, `FN`, `LN`, `ST3`, `PC`, `CITY`                           |
| **B2** | Individu × Adresse suffisante × Email          | `SAL`, `FN`, `LN`, `ST3`, `ST4`, `PC`, `CITY`, `EMAIL`           |
| **B3** | Individu × Adresse suffisante × Mobile         | `SAL`, `FN`, `LN`, `ST3`, `ST4`, `PC`, `CITY`, `MOBILE`          |
| **B4** | Individu × Adresse suffisante × Email × Mobile | `SAL`, `FN`, `LN`, `ST3`, `ST4`, `PC`, `CITY`, `EMAIL`, `MOBILE` |
| **B5** | Individu × Email seul                          | `SAL`, `FN`, `LN`, `EMAIL`                                       |
| **B6** | Individu × Mobile seul                         | `SAL`, `FN`, `LN`, `MOBILE`                                      |
| **B7** | Individu × Mobile et Home phone                | `SAL`, `FN`, `LN`, `MOBILE`, `HOME`                              |
| **B8** | Individu × Email + Mobile                      | `SAL`, `FN`, `LN`, `EMAIL`, `MOBILE`                             |
| **B9** | Individu × Email + Mobile + Home phone         | `SAL`, `FN`, `LN`, `EMAIL`, `MOBILE`, `HOME`                     |
| **C0** | Foyer × Adresse complète                       | `LN`, `ST1`, `ST2`, `ST3`, `ST4`, `PC`, `CITY`                   |
| **C1** | Foyer × Adresse minimale                       | `LN`, `ST3`, `PC`, `CITY`                                        |
| **C2** | Foyer × Adresse suffisante × Email             | `LN`, `ST3`, `ST4`, `PC`, `CITY`, `EMAIL`                        |
| **C3** | Foyer × Adresse suffisante × Mobile            | `LN`, `ST3`, `ST4`, `PC`, `CITY`, `MOBILE`                       |
| **C4** | Foyer × Adresse suffisante × Email × Mobile    | `LN`, `ST3`, `ST4`, `PC`, `CITY`, `EMAIL`, `MOBILE`              |
| **C5** | Foyer × Email seul                             | `LN`, `EMAIL`                                                    |
| **C6** | Foyer × Mobile seul                            | `LN`, `MOBILE`                                                   |
| **C7** | Foyer × Mobile et Home phone                   | `LN`, `MOBILE`, `HOME`                                           |
| **C8** | Foyer × Email + Mobile                         | `LN`, `EMAIL`, `MOBILE`                                          |
| **C9** | Foyer × Email + Mobile + Home phone            | `LN`, `EMAIL`, `MOBILE`, `HOME`                                  |


Le fichier `doublons.csv` est chargé en mémoire sous forme de dictionnaire `(Principal, Doublon) → Statut`.

* Si une paire existe déjà (dans les deux sens), elle n’est pas recréée.
* Les statuts existants sont comptabilisés dans la colonne `status_distribution` du résumé.

### Sorties générées

Les fichiers sont produits automatiquement dans `./out` selon la convention : `<rule>_<YYYY_MM_DD>_<type>.csv`
