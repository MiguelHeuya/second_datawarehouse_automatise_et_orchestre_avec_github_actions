# 🏢 Data Warehouse E-commerce — Olist

> **Centraliser, fiabiliser, transformer et structurer les données opérationnelles d'une entreprise afin de les rendre directement exploitables pour l'analyse, le reporting et la prise de décision.**

---

## 📌 Présentation du projet

Ce projet consiste à concevoir et implémenter un **Data Warehouse orienté Business Intelligence** à partir de plusieurs sources de données issues d'un environnement e-commerce.

L'objectif n'est pas simplement de déplacer des données d'une base vers une autre.

Le véritable objectif est de construire une **chaîne complète de valorisation de la donnée** :

```text
Sources opérationnelles
        ↓
     Bronze
        ↓
Nettoyage / standardisation
        ↓
     Silver
        ↓
Transformation métier
        ↓
      Gold
        ↓
   Modèle en étoile
        ↓
 Analyse / Reporting / BI
```

Le projet met donc en œuvre une architecture permettant de passer de données réparties dans plusieurs sources à une **couche analytique structurée et orientée décision**.

---

# 🎯 1. Le problème Business

Dans une entreprise e-commerce, les informations nécessaires pour comprendre la performance de l'activité ne se trouvent généralement pas dans une seule table.

Les informations relatives :

* aux clients ;
* aux commandes ;
* aux produits ;
* aux vendeurs ;
* aux paiements ;
* aux avis clients ;
* aux informations géographiques ;
* aux catégories de produits ;

peuvent être réparties dans plusieurs systèmes ou plusieurs tables.

Cette organisation est adaptée au fonctionnement opérationnel des applications, mais elle devient beaucoup moins pratique lorsqu'il faut répondre rapidement à des questions analytiques.

### Exemple

Une direction commerciale peut vouloir répondre à des questions telles que :

* Combien de commandes avons-nous réalisées ?
* Quelle est la valeur totale des paiements ?
* Quels produits sont les plus représentés dans les commandes ?
* Quels vendeurs génèrent le plus d'activité ?
* Dans quelles régions se trouvent les clients ?
* Quels modes de paiement sont les plus utilisés ?
* Quelle est la satisfaction moyenne des clients ?
* Comment évoluent les commandes dans le temps ?
* Quels produits présentent des caractéristiques logistiques importantes ?
* Quel est le montant associé au fret ?
* Quel est le délai entre les différentes étapes d'une commande ?

Le problème n'est donc pas uniquement :

> **« Avons-nous les données ? »**

mais plutôt :

> **« Avons-nous les données sous une forme suffisamment fiable, structurée et accessible pour pouvoir les analyser efficacement ? »**

---

# 💼 2. La valeur Business du Data Warehouse

Ce Data Warehouse constitue une couche intermédiaire entre les systèmes opérationnels et les besoins analytiques de l'entreprise.

Il permet de transformer :

```text
Données dispersées
        ↓
Données centralisées
        ↓
Données nettoyées
        ↓
Données harmonisées
        ↓
Données modélisées
        ↓
Données prêtes pour l'analyse
```

## 2.1 Centraliser les données

Les données provenant de deux sources PostgreSQL sont ingérées dans un Data Warehouse central.

Le pipeline identifie automatiquement les tables présentes dans les bases sources et les charge dans la couche Bronze.

Cela permet de créer un point de référence central pour les analyses.

---

## 2.2 Réduire la complexité analytique

Un analyste ne devrait pas avoir à reconstruire toute la logique de transformation avant chaque analyse.

Le Data Warehouse effectue en amont une partie importante du travail :

* nettoyage ;
* typage ;
* normalisation ;
* agrégation ;
* enrichissement ;
* jointures ;
* calcul d'indicateurs ;
* modélisation dimensionnelle.

Ainsi, la couche Gold fournit une structure beaucoup plus directement exploitable.

---

## 2.3 Améliorer la cohérence des indicateurs

Un problème fréquent dans les environnements analytiques est que deux personnes calculent le même indicateur de manière différente.

Ici, certaines règles métier sont centralisées dans le pipeline.

Par exemple, le score moyen d'une commande est calculé à partir des avis associés à cette commande. Lorsqu'aucun avis n'existe, une moyenne globale est utilisée comme valeur de secours.

La logique métier devient donc réutilisable plutôt que reconstruite manuellement dans chaque rapport.

---

## 2.4 Faciliter le reporting

La couche Gold est structurée autour d'un **modèle dimensionnel en étoile** :

```text
                    dim_customers
                          |
                          |
dim_sellers ---- fact_orders ---- dim_products
```

Ce type de modèle permet de séparer :

### Les dimensions

Elles décrivent le contexte de l'activité :

* qui ?
* quoi ?
* où ?
* quel produit ?
* quel vendeur ?

### Les faits

Ils représentent l'activité mesurable :

* commandes ;
* montants ;
* paiements ;
* frais ;
* satisfaction ;
* informations temporelles.

La couche Gold contient actuellement :

```text
dim_customers
dim_sellers
dim_products
fact_orders
```

---

# 📊 3. Ce que l'entreprise peut analyser

La couche Gold permet de construire une base analytique autour de plusieurs axes.

## 👥 Analyse client

La dimension `dim_customers` contient notamment :

* `customer_id`
* `customer_zip_code`
* `customer_city`
* `customer_state`

Cela permet notamment de segmenter l'activité par :

* ville ;
* État ;
* zone géographique ;
* client.

---

## 🏪 Analyse vendeur

La dimension `dim_sellers` contient :

* `seller_id`
* `seller_zip_code`
* `seller_city`
* `seller_state`

Elle permet de construire des analyses autour des vendeurs et de leur localisation.

---

## 📦 Analyse produit

La dimension `dim_products` enrichit les informations produits avec :

* catégorie originale ;
* catégorie anglaise ;
* longueur du nom ;
* longueur de la description ;
* nombre de photos ;
* poids ;
* longueur ;
* hauteur ;
* largeur ;
* volume calculé.

Le volume est calculé à partir de :

```text
Volume = longueur × hauteur × largeur
```

Cela ouvre notamment la possibilité d'analyser les produits selon des dimensions commerciales mais aussi logistiques.

---

## 🛒 Analyse des commandes

La table `fact_orders` centralise les informations analytiques relatives aux commandes.

Elle contient notamment :

### Identifiants

```text
order_id
seller_id
product_id
customer_id
```

### Suivi de la commande

```text
order_status
order_purchase_date
order_approved_date
carrier_delivery_date
customer_delivery_date
estimated_delivery_date
seller_shipping_limit_date
```

### Satisfaction

```text
customer_review_score
```

### Prix et logistique

```text
item_price
item_freight_value
installment_or_tip_fee
```

### Paiement

```text
payment_sequence_number
payment_installments_count
is_paid_by_credit_card
is_paid_by_boleto
is_paid_by_voucher
is_paid_by_debit_card
total_payment_amount
```

---

# 🏗️ 4. Architecture technique

Le projet utilise une architecture en trois couches :

```text
┌─────────────────────────────────────────────┐
│              SOURCES POSTGRESQL             │
│                                             │
│  SOURCE A                SOURCE B           │
│  ├─ Customers            ├─ Orders          │
│  ├─ Payments             ├─ Reviews         │
│  ├─ Order Items          ├─ Products        │
│  ├─ Geolocation          ├─ Sellers         │
│                          └─ Translation     │
└──────────────────────┬──────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────┐
│                    BRONZE                   │
│                                             │
│        Ingestion / Raw Data                 │
│                                             │
│  Conservation des données provenant        │
│  des différentes sources                   │
└──────────────────────┬──────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────┐
│                    SILVER                   │
│                                             │
│  Nettoyage + Typage + Standardisation       │
│  + Agrégations + Préparation                │
└──────────────────────┬──────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────┐
│                     GOLD                    │
│                                             │
│             Modèle en étoile               │
│                                             │
│  dim_customers                              │
│  dim_sellers                                │
│  dim_products                               │
│  fact_orders                                │
└──────────────────────┬──────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────┐
│       ANALYSE / REPORTING / BI              │
│                                             │
│  Dashboards • KPI • Reporting • Analytics   │
└─────────────────────────────────────────────┘
```

---

# 🥉 5. Couche Bronze — Ingestion

La Bronze constitue la première couche du Data Warehouse.

Son rôle principal est de récupérer les données provenant des systèmes sources et de les centraliser.

Le pipeline ne dépend pas d'une liste codée manuellement de tables : il interroge `information_schema.tables` afin d'identifier les tables utilisateur disponibles dans chaque source.

Deux sources sont configurées :

```python
DEFAULT_SOURCES = [
    {"alias": "sourcea", "config": SOURCE_A_DB},
    {"alias": "sourceb", "config": SOURCE_B_DB},
]
```

Les tables sont ensuite chargées dans le schéma `bronze`.

Le nom de destination reprend l'origine de la donnée :

```text
sourcea_<schema>_<table>
sourceb_<schema>_<table>
```

### Pourquoi cette couche ?

Elle permet notamment de :

* centraliser les données ;
* conserver une représentation proche des sources ;
* identifier l'origine des tables ;
* séparer l'ingestion des transformations métier.

---

# 🥈 6. Couche Silver — Nettoyage et préparation

La Silver constitue la couche de préparation des données.

Le principe est :

> **Ne pas demander à la couche Gold de résoudre tous les problèmes de qualité de données.**

Les données sont d'abord préparées dans Silver.

---

## 6.1 Standardisation géographique

Les codes d'États brésiliens sont convertis en noms complets.

Par exemple :

```text
SP → São Paulo
RJ → Rio de Janeiro
MG → Minas Gerais
BA → Bahia
```

Cette logique est appliquée notamment aux clients, vendeurs et données géographiques.

Cela rend les données plus lisibles pour les analyses et les utilisateurs métier.

---

## 6.2 Conversion des types

Plusieurs colonnes numériques sont explicitement converties.

Par exemple :

```text
price
freight_value
payment_value
payment_installments
product_weight_g
product_length_cm
...
```

Les dates sont également converties en types Date.

---

## 6.3 Gestion des valeurs manquantes

Certaines valeurs nulles sont remplacées par des valeurs explicites.

Par exemple, les commentaires d'avis absents sont remplacés par :

```text
n/a
```

Pour les produits, les catégories manquantes sont également traitées.

---

## 6.4 Agrégation des paiements

Les paiements sont regroupés par commande.

La Silver calcule notamment :

```text
total_payment_value
```

et sépare les montants associés aux différents moyens de paiement :

```text
has_credit_card
has_boleto
has_voucher
has_debit_card
```

Cela simplifie considérablement l'utilisation des paiements dans la couche Gold.

---

## 6.5 Traitement particulier des articles

Une logique spécifique est appliquée aux commandes mono-produit et multi-produits.

### Commande avec un seul produit distinct

Les lignes sont agrégées par commande.

### Commande avec plusieurs produits distincts

Les lignes originales sont conservées.

Cette distinction est implémentée explicitement dans la transformation Silver.

Cette transformation est importante car elle prépare les données à leur utilisation dans `fact_orders`.

---

# 🥇 7. Couche Gold — Modèle analytique

La couche Gold constitue l'interface analytique du Data Warehouse.

Elle ne cherche plus à représenter fidèlement les systèmes opérationnels.

Elle cherche plutôt à répondre à une question :

> **Comment organiser les données pour faciliter l'analyse ?**

---

# ⭐ 8. Modèle en étoile

Le modèle final est organisé autour d'une table de faits :

```text
                    ┌─────────────────┐
                    │  dim_customers  │
                    └────────┬────────┘
                             │
                             │
┌───────────────┐      ┌─────▼──────┐      ┌───────────────┐
│  dim_sellers  │──────│ fact_orders│──────│  dim_products │
└───────────────┘      └────────────┘      └───────────────┘
```

---

## 8.1 `dim_customers`

Cette dimension représente le contexte client.

```text
customer_id
customer_zip_code
customer_city
customer_state
```

---

## 8.2 `dim_sellers`

Cette dimension représente le contexte vendeur.

```text
seller_id
seller_zip_code
seller_city
seller_state
```

---

## 8.3 `dim_products`

Cette dimension représente le contexte produit.

Elle enrichit les données originales avec la traduction de la catégorie et un indicateur calculé :

```text
product_volume_cm3
```

La traduction anglaise est obtenue en priorité depuis la table de traduction ; lorsque cette traduction n'est pas disponible, la catégorie originale est utilisée comme solution de repli.

---

## 8.4 `fact_orders`

Cette table constitue le centre du modèle analytique.

Elle rassemble les informations issues de plusieurs tables Silver :

```text
orders
payments
order_items
reviews
customers
```

Elle permet ainsi de rapprocher :

```text
Commande
   +
Client
   +
Produit
   +
Vendeur
   +
Paiement
   +
Satisfaction
   +
Informations temporelles
```

---

# 🧮 9. Logiques métier implémentées

Le Data Warehouse ne se contente pas de déplacer les données.

Il applique également des règles de calcul.

---

## 9.1 Score moyen de satisfaction

Pour chaque commande, le score moyen des avis est calculé :

```text
AVG(review_score)
```

Lorsqu'aucun avis n'est disponible, une moyenne globale est utilisée comme valeur de secours.

Le résultat est arrondi à deux décimales.

---

## 9.2 Calcul du montant complémentaire

Le projet calcule :

```text
installment_or_tip_fee
```

avec :

```text
total_payment_amount
-
(item_price + item_freight_value)
```

Le résultat est arrondi à deux décimales.

Cette transformation permet de créer un indicateur qui n'était pas directement disponible sous cette forme dans les données sources.

---

## 9.3 Calcul du volume produit

Le volume est calculé avec :

```text
product_length_cm
× product_height_cm
× product_width_cm
```

et stocké dans :

```text
product_volume_cm3
```

Cet indicateur peut notamment être utilisé dans des analyses liées aux caractéristiques physiques et logistiques des produits.

---

# ⚡ 10. Indexation de la couche Gold

La couche Gold ne se limite pas à créer les tables.

Des index PostgreSQL sont également créés.

Pour `fact_orders`, les index concernent notamment :

```text
order_id
customer_id
seller_id
product_id
order_purchase_date
order_status
```

Des index sont également créés sur les dimensions :

```text
dim_customers
dim_sellers
dim_products
```

### Objectif

Faciliter les opérations de recherche et de jointure sur les colonnes fréquemment utilisées dans les analyses.

---

# 🔄 11. Orchestration du pipeline

L'ensemble du processus est centralisé dans :

```text
main_etl.py
```

Le pipeline exécute les étapes dans cet ordre :

```text
1. Bronze
      ↓
2. Silver
      ↓
3. Gold
```

Cette séquence est explicitement définie dans l'orchestrateur.

L'utilisateur n'a donc pas besoin d'exécuter manuellement chaque étape.

Une seule commande peut lancer l'ensemble du processus.

---

# 🛡️ 12. Gestion des erreurs et logique "Tout ou Rien"

Un aspect important du projet est la gestion des échecs.

Si une étape du pipeline échoue :

```text
Bronze ❌
     ↓
Rollback
     ↓
Suppression Bronze
Suppression Silver
Suppression Gold
```

L'orchestrateur supprime alors les schémas :

```text
bronze
silver
gold
```

afin d'éviter de conserver un état intermédiaire résultant d'une exécution échouée.

Le pipeline considère donc l'exécution globale comme :

```text
SUCCESS
```

ou

```text
FAILURE
```

plutôt que de considérer une exécution partiellement terminée comme réussie.

---

# 📝 13. Logging et observabilité

Le projet possède également un système de logging.

Les événements sont enregistrés :

* dans le terminal ;
* dans des fichiers de log.

Le niveau de log est configurable via l'environnement.

L'orchestrateur génère également un fichier d'exécution horodaté.

Exemple :

```text
etl_pipeline_20260815_093000.log
```

Le pipeline journalise notamment :

```text
Démarrage
    ↓
Étape en cours
    ↓
Succès / erreur
    ↓
Nombre de lignes
    ↓
Durée
    ↓
Résultat final
```

Cela facilite le diagnostic d'une exécution.

---

# 🔐 14. Gestion de la configuration et des secrets

Les informations de connexion aux bases ne sont pas directement codées dans les scripts.

La configuration est récupérée à partir des variables d'environnement.

Les trois environnements principaux sont :

```text
SOURCE_A_DB
SOURCE_B_DB
DW_DB
```

avec notamment :

```text
host
port
database
user
password
```

Le fichier `.gitignore` exclut notamment :

```text
.env
.env.*
```

afin d'éviter de versionner les fichiers contenant potentiellement des secrets.

---

# 🧰 15. Stack technique

## Base de données

**PostgreSQL**

Utilisé pour :

* les sources ;
* le Data Warehouse ;
* les schémas Bronze, Silver et Gold ;
* les index analytiques.

---

## Langage

**Python**

Utilisé pour :

* l'ingestion ;
* les transformations ;
* la modélisation ;
* l'orchestration ;
* le logging ;
* la gestion des erreurs.

---

## Traitement des données

**Polars**

Utilisé pour manipuler les DataFrames et effectuer les transformations de données.

## Les pipelines Bronze, Silver et Gold utilisent notamment Polars pour lire, transformer et écrire les données.

## Accès aux bases

**ADBC PostgreSQL**

Utilisé pour les connexions PostgreSQL et certaines opérations DDL/DML.

---

## Gestion des variables d'environnement

**python-dotenv**

Utilisé pour charger les variables présentes dans `.env`.

---

# 📁 16. Structure du projet

Une organisation logique du projet est la suivante :

```text
.
├── main_etl.py
│
├── bronze_extract_and_load.py
│   └── Ingestion des sources → Bronze
│
├── silver_data_cleaning.py
│   └── Nettoyage / transformation → Silver
│
├── gold_business_logic.py
│   └── Modélisation / logique métier → Gold
│
├── transformation script.sql
│   └── Transformations SQL de la couche Silver
│
├── gold script.sql
│   └── Modélisation SQL de la couche Gold
│
├── config.py
│   └── Configuration des connexions et environnements
│
├── .env
│   └── Variables d'environnement — non versionné
│
├── .gitignore
│
└── logs/
    └── Journaux d'exécution
```

---

# 🔁 17. Deux approches de transformation

Le projet contient à la fois :

```text
SQL
```

et

```text
Python + Polars
```

pour exprimer les transformations.

Les scripts SQL documentent notamment les transformations de Silver et la construction des tables Gold.

La version Python permet quant à elle d'intégrer ces transformations directement dans un pipeline automatisé.

Cela illustre deux manières courantes d'aborder la transformation dans un environnement Data Warehouse :

```text
Approche SQL
    ↓
Transformation directement dans PostgreSQL

Approche Python
    ↓
Lecture
    ↓
Transformation avec Polars
    ↓
Écriture dans PostgreSQL
```

L'orchestrateur actuel appelle les pipelines Python pour exécuter Bronze → Silver → Gold.

---

# ▶️ 18. Exécution du pipeline

## Étape 1 — Configurer les variables d'environnement

Créer un fichier :

```text
.env
```

avec les paramètres correspondant aux bases utilisées par le projet.

Exemple de structure :

```env
ENV=development
LOG_LEVEL=INFO
LOG_PATH=./logs/

SOURCE_A_DB_HOST=localhost
SOURCE_A_DB_PORT=5432
SOURCE_A_DB_NAME=...
SOURCE_A_DB_USER=...
SOURCE_A_DB_PASSWORD=...

SOURCE_B_DB_HOST=localhost
SOURCE_B_DB_PORT=5432
SOURCE_B_DB_NAME=...
SOURCE_B_DB_USER=...
SOURCE_B_DB_PASSWORD=...

DW_DB_HOST=localhost
DW_DB_PORT=5432
DW_DB_NAME=...
DW_DB_USER=...
DW_DB_PASSWORD=...

DW_SCHEMA_BRONZE=bronze
DW_SCHEMA_SILVER=silver
DW_SCHEMA_GOLD=gold
```

Les variables correspondent aux paramètres effectivement consommés par `config.py`.

---

## Étape 2 — Installer les dépendances

Les imports du projet montrent notamment l'utilisation de :

```text
polars
adbc_driver_postgresql
python-dotenv
```

Une installation de base peut donc être réalisée avec :

```bash
pip install polars adbc-driver-postgresql python-dotenv
```

---

## Étape 3 — Lancer le pipeline

```bash
python main_etl.py
```

Le pipeline exécute alors :

```text
BRONZE
   ↓
SILVER
   ↓
GOLD
```

Si toutes les étapes réussissent :

```text
🎉 PIPELINE ETL EXÉCUTÉ ET VALIDÉ AVEC SUCCÈS
```

En cas d'échec, le mécanisme de rollback est déclenché.

---

# 📈 19. Exemple de chaîne de valorisation

Une donnée provenant initialement d'une source opérationnelle peut suivre ce parcours :

```text
SOURCE
│
│  Donnée opérationnelle
│
▼
BRONZE
│
│  Centralisation
│
▼
SILVER
│
│  Nettoyage
│  Typage
│  Standardisation
│  Agrégation
│
▼
GOLD
│
│  Modélisation dimensionnelle
│  Calculs métier
│  Indexation
│
▼
BI / ANALYTICS
│
│  KPI
│  Dashboards
│  Reporting
│  Analyse
│
▼
DÉCISION
```

C'est précisément cette chaîne qui donne sa valeur au Data Warehouse.

---

# 🧠 20. Ce que ce projet démontre

Ce projet ne démontre pas uniquement la capacité à écrire des requêtes SQL.

Il démontre la capacité à raisonner sur l'ensemble de la chaîne Business Intelligence :

```text
                    DATA
                     │
                     ▼
             ┌───────────────┐
             │   INGESTION   │
             └───────┬───────┘
                     │
                     ▼
             ┌───────────────┐
             │   QUALITÉ     │
             │     DATA      │
             └───────┬───────┘
                     │
                     ▼
             ┌───────────────┐
             │ TRANSFORMATION │
             └───────┬───────┘
                     │
                     ▼
             ┌───────────────┐
             │ MODÉLISATION  │
             └───────┬───────┘
                     │
                     ▼
             ┌───────────────┐
             │    ANALYTICS  │
             └───────┬───────┘
                     │
                     ▼
             ┌───────────────┐
             │   DÉCISION    │
             └───────────────┘
```

Le projet montre notamment des compétences en :

* Data Warehousing ;
* ETL ;
* PostgreSQL ;
* SQL ;
* Python ;
* Polars ;
* modélisation dimensionnelle ;
* modèle en étoile ;
* transformation de données ;
* logique métier ;
* indexation ;
* logging ;
* gestion des erreurs ;
* configuration par environnement ;
* automatisation d'un pipeline.

---

# 🎯 21. Questions Business que cette architecture prépare à résoudre

Une fois la couche Gold connectée à un outil de Business Intelligence, les données sont organisées pour faciliter des analyses telles que :

### Performance commerciale

```text
Quel est le volume de commandes ?
Comment évolue l'activité dans le temps ?
Quels produits sont les plus présents dans les commandes ?
```

### Performance vendeur

```text
Quels vendeurs génèrent le plus d'activité ?
Comment l'activité est-elle répartie géographiquement ?
```

### Performance produit

```text
Quelles catégories sont les plus représentées ?
Quelles sont les caractéristiques physiques des produits ?
Quel est le volume moyen des produits ?
```

### Expérience client

```text
Quel est le score moyen des avis ?
Comment la satisfaction se répartit-elle géographiquement ?
```

### Paiement

```text
Quels moyens de paiement sont utilisés ?
Quel est le montant total associé aux paiements ?
Combien de versements sont associés aux commandes ?
```

### Logistique

```text
Quel est le montant du fret ?
Quelles sont les dates importantes du cycle de commande ?
Quels produits présentent les plus grands volumes physiques ?
```

Ces questions constituent des **cas d'usage analytiques potentiels** du modèle ; le projet fourni ne contient pas de mesure d'impact business réelle ni de dashboard permettant de quantifier ces résultats.

---

# 🔍 22. Points importants sur le grain de `fact_orders`

Un point essentiel de modélisation doit être compris avant d'utiliser la table de faits.

La Silver applique une logique différente selon qu'une commande contient un ou plusieurs produits.

Pour une commande mono-produit, les lignes sont agrégées.

Pour une commande contenant plusieurs produits distincts, les lignes d'articles sont conservées.

Par conséquent, `fact_orders` ne doit pas être interprétée aveuglément comme :

> « exactement une ligne = une commande »

Le grain dépend de la préparation effectuée en Silver.

C'est un point important pour éviter des doubles comptages lors de la création des mesures analytiques.

Avant de calculer un KPI comme :

```text
Nombre de commandes
```

ou :

```text
Montant total
```

il faut donc comprendre le grain réel de la table et adapter les agrégations.

---

# ⚠️ 23. Limites actuelles et pistes d'amélioration

Ce projet constitue une implémentation fonctionnelle d'une architecture Data Warehouse, mais plusieurs axes peuvent encore être développés.

## Tests de qualité des données

Ajouter des contrôles automatisés sur :

* unicité des identifiants ;
* valeurs nulles ;
* intégrité des clés ;
* nombre de lignes ;
* valeurs aberrantes ;
* cohérence des montants.

---

## Data Quality Framework

Mettre en place des règles explicites du type :

```text
order_id IS NOT NULL
customer_id IS NOT NULL
product_id IS NOT NULL
price >= 0
freight_value >= 0
review_score BETWEEN 1 AND 5
```

---

## Gestion historique

Les dimensions actuelles sont essentiellement construites comme des tables analytiques reconstruites.

Une évolution possible serait d'implémenter une gestion historique des dimensions, par exemple avec une logique **Slowly Changing Dimension (SCD)** lorsque le besoin métier le justifie.

---

## Incremental Load

Le pipeline actuel effectue une ingestion complète des sources et remplace les tables cibles.

Une évolution importante serait de passer progressivement à des chargements incrémentaux :

```text
Full Load
    ↓
Incremental Load
    ↓
CDC / Change Data Capture
```

afin d'éviter de retraiter inutilement l'ensemble des données lorsque les volumes augmentent.

---

## Orchestration avancée

L'orchestrateur actuel est implémenté directement en Python.

Une évolution possible serait d'utiliser un orchestrateur spécialisé comme :

```text
Apache Airflow
Dagster
Prefect
```

pour gérer :

* planification ;
* dépendances ;
* retries ;
* monitoring ;
* historique des exécutions ;
* alertes.

---

## Séparation des environnements

Une architecture plus avancée pourrait séparer :

```text
Development
     ↓
Testing
     ↓
Production
```

avec des configurations et contrôles différents.

---

# 🚀 24. Évolutions possibles

La trajectoire possible du projet peut être représentée ainsi :

```text
                   VERSION ACTUELLE
                         │
                         ▼
              Data Warehouse PostgreSQL
                         │
                         ▼
              Bronze → Silver → Gold
                         │
                         ▼
               Modèle dimensionnel
                         │
                         ▼
                    BI / Reporting
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
        Data Quality          Incremental Load
              │                     │
              └──────────┬──────────┘
                         ▼
                  Orchestration
                         │
                         ▼
                 Monitoring / Alerting
                         │
                         ▼
                  Production-ready
```

---

# 🏁 25. Conclusion

Ce projet met en œuvre une chaîne complète de Data Warehousing permettant de transformer plusieurs sources opérationnelles en une couche analytique structurée.

La valeur du projet repose sur une idée simple :

> **La Business Intelligence ne commence pas au moment où l'on ouvre un dashboard. Elle commence bien avant, lorsqu'on construit une donnée fiable, structurée et adaptée aux questions métier.**

Le pipeline réalise cette transformation en plusieurs étapes :

```text
Sources
   ↓
Bronze
   ↓
Centralisation
   ↓
Silver
   ↓
Nettoyage + Standardisation + Agrégation
   ↓
Gold
   ↓
Modèle en étoile + Logique métier + Indexation
   ↓
Analytics
   ↓
Reporting
   ↓
Décision
```

L'architecture permet ainsi de créer une séparation claire entre :

```text
Systèmes opérationnels
        ≠
Couche analytique
```

et de fournir aux utilisateurs BI une donnée préparée spécifiquement pour l'analyse.

---

# 👨‍💻 Technologies

```text
Python
PostgreSQL
SQL
Polars
ADBC
python-dotenv
Data Warehousing
ETL
Modélisation dimensionnelle
Star Schema
Business Intelligence
```

---

# 📚 Concepts mis en pratique

```text
✓ Data Warehouse
✓ ETL
✓ Medallion Architecture
✓ Bronze / Silver / Gold
✓ Data Cleaning
✓ Data Transformation
✓ Data Integration
✓ Data Modeling
✓ Dimensional Modeling
✓ Star Schema
✓ Fact Table
✓ Dimension Tables
✓ Business Logic
✓ PostgreSQL Indexing
✓ Logging
✓ Error Handling
✓ Pipeline Orchestration
✓ Environment Configuration
```

---

## 📌 Résumé en une phrase

> **Un Data Warehouse e-commerce construit avec PostgreSQL, Python et Polars, qui centralise plusieurs sources, nettoie et transforme les données à travers une architecture Bronze/Silver/Gold, puis les organise dans un modèle en étoile conçu pour faciliter l'analyse, le reporting et la prise de décision.**
