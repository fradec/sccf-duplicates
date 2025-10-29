# Analyze duplicates contacts according to various criteria

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

## Execute the script

run `python3 duplicate_analysis.py`

### Normalization


### Analysis according to several rules

