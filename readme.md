# Duplicate Analysis

## Objectif

Ce script permet d’analyser et de détecter les doublons dans un fichier **contacts.csv** à partir d’un fichier de référence **doublons.csv**.
Il repose sur un processus en deux étapes :

1. **Normalisation** : nettoyage et homogénéisation des données de contact.
2. **Évaluation des règles de déduplication** : comparaison des enregistrements selon différents critères.


## Setup

You have to download your contacts from salesforce with this query:

```SQL
SELECT Id, CreatedDate, IdPersonne__c, AccountId, Salutation, FirstName, LastName, FirstNameSearchable__c, LastNameSearchable__c, MailingStreet1__c, MailingStreet2__c, MailingStreet3__c, MailingStreet4__c, MailingPostalCode, MailingCity, MailingCountry, HomePhone, MobilePhone, Email, Est_un_doublon__c, TECH_IsMerged__c, TECH_SFIdPrincipal__c, InformationDonateur__c, Sphere__c, TypeActeurs__c, IdSCCFContact__c, Statut__c
FROM Contact
WHERE RecordType.DeveloperName = 'Acteurs'
```

The file should have this name: `contacts.csv`

Then you have to export Doublons potentiel:
```SQL
SELECT Id, CoherenceDesDoublons__c, CreatedDate, Statut__c, DateDeDernierTraitement__c, ContactPrincipal__c, ContactDoublon__c
FROM Doublon_potentiel__c
WHERE ContactPrincipal__c != null
	AND ContactDoublon__c != null
```

The file should have this name: `doublons.csv`



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
| `ST3`    | `MailingStreet3__c`      | Idem                                               |
| `ST4`    | `MailingStreet4__c`      | Idem                                               |
| `PC`     | `MailingPostalCode`      | Trim                                               |
| `CITY`   | `MailingCity`            | Majuscules, accents supprimés                      |
| `EMAIL`  | `Email`                  | Trim uniquement                                    |
| `MOBILE` | `MobilePhone`            | Digits only                                        |
| `SAL`    | `Salutation`             | Trim                                               |


## Étape 2 — Évaluation des règles

Chaque règle de matching compare les enregistrements sur un sous-ensemble de champs normalisés.

| Règle    | Description                                        | Champs utilisés                                            |
| -------- | -------------------------------------------------- | ---------------------------------------------------------- |
| **I**    | Nom + Prénom + Adresse complète                    | `LN`, `FN`, `ST3`, `ST4`, `PC`, `CITY`                     |
| **II**   | Nom + Prénom + Adresse partielle                   | `LN`, `FN`, `ST3`, `PC`, `CITY`                            |
| **III**  | Nom + Adresse complète                             | `LN`, `ST3`, `ST4`, `PC`, `CITY`                           |
| **IV**   | Nom + Adresse complète + Email                     | `LN`, `ST3`, `ST4`, `PC`, `CITY`, `EMAIL`                  |
| **V**    | Civilité + Nom + Adresse complète + Email          | `SAL`, `LN`, `ST3`, `ST4`, `PC`, `CITY`, `EMAIL`           |
| **VI**   | Civilité + Nom + Adresse complète + Email + Mobile | `SAL`, `LN`, `ST3`, `ST4`, `PC`, `CITY`, `EMAIL`, `MOBILE` |
| **VII**  | Email uniquement                                   | `EMAIL`                                                    |
| **VIII** | Mobile uniquement                                  | `MOBILE`                                                   |


Le fichier `doublons.csv` est chargé en mémoire sous forme de dictionnaire `(Principal, Doublon) → Statut`.

* Si une paire existe déjà (dans les deux sens), elle n’est pas recréée.
* Les statuts existants sont comptabilisés dans la colonne `status_distribution` du résumé.

### Sorties générées

Les fichiers sont produits automatiquement dans `./out` selon la convention :

```
<rule>_<YYYY_MM_DD>_<type>.csv
```
