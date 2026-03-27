# Guide des sorties normalisees pour PostgreSQL

## Objectif

Ce dossier regroupe les livrables finaux a utiliser pour construire une base de donnees PostgreSQL normalisee a partir des fichiers source `orders.csv`, `people.csv` et `returns.csv`.

La structure a ete renforcee pour aller plus loin dans la normalisation :

- `category`, `sub_category` et `region` ont maintenant leurs propres identifiants
- une table `managers` a ete ajoutee
- la localisation a ete decoupee en plusieurs niveaux
- les tables dependantes utilisent des IDs au lieu des noms quand cela est pertinent

## Contenu du dossier

Le dossier contient les fichiers suivants :

- `customers.csv`
- `categories.csv`
- `sub_categories.csv`
- `products.csv`
- `managers.csv`
- `regions.csv`
- `markets.csv`
- `countries.csv`
- `states.csv`
- `cities.csv`
- `locations.csv`
- `orders_normalized.csv`
- `order_items.csv`
- `order_returns.csv`
- `normalization_anomalies.csv`
- `create_sales_schema.sql`

## Explication des tables

### 1. `customers.csv`

Colonnes :

- `customer_id`
- `customer_name`
- `segment`

Cette table conserve la cle metier client et evite de repeter les informations descriptives du client dans les commandes.

### 2. `categories.csv`

Colonnes :

- `category_id`
- `category_name`

Le nom de categorie reste unique, mais un identifiant technique est maintenant introduit pour fiabiliser les references vers les autres tables.

### 3. `sub_categories.csv`

Colonnes :

- `sub_category_id`
- `sub_category_name`
- `category_id`

La sous-categorie depend de la categorie. La table n'utilise plus le nom de categorie comme reference externe directe.

### 4. `products.csv`

Colonnes :

- `product_id`
- `product_name`
- `sub_category_id`

Le produit garde sa cle metier `product_id`, mais il reference desormais la sous-categorie via son identifiant.

### 5. `managers.csv`

Colonnes :

- `manager_id`
- `manager_name`

Cette table isole les managers. Leur nom n'est plus stocke directement dans `regions`.

### 6. `regions.csv`

Colonnes :

- `region_id`
- `region_name`
- `manager_id`

La region a maintenant son propre identifiant. Quand un manager existe, `manager_id` pointe vers la table `managers`. Quand l'information manque dans la source, `manager_id` reste vide.

### 7. `markets.csv`

Colonnes :

- `market_id`
- `market_name`

Cette table represente le niveau marche dans la hierarchie geographique.

### 8. `countries.csv`

Colonnes :

- `country_id`
- `country_name`
- `market_id`

Chaque pays depend d'un marche.

### 9. `states.csv`

Colonnes :

- `state_id`
- `state_name`
- `country_id`

L'etat depend du pays. Un meme nom d'etat peut exister dans plusieurs pays, d'ou l'utilisation d'un identifiant technique.

### 10. `cities.csv`

Colonnes :

- `city_id`
- `city_name`
- `state_id`
- `region_id`

La ville depend de l'etat, et la region est referencee ici par `region_id` au lieu de conserver le nom de region dans les tables de localisation.

### 11. `locations.csv`

Colonnes :

- `location_id`
- `city_id`
- `postal_code`

La table `locations` a ete normalisee. Elle ne porte plus directement `country`, `state`, `city`, `region` ou `market`. Ces informations se retrouvent en remontant la chaine :

`locations -> cities -> states -> countries -> markets`

Cela reduit fortement la redondance geographique.

### 12. `orders_normalized.csv`

Colonnes :

- `order_id`
- `customer_id`
- `location_id`
- `order_date`
- `ship_date`
- `ship_mode`
- `order_priority`

La commande reste liee au client et a la localisation par identifiant.

### 13. `order_items.csv`

Colonnes :

- `row_id`
- `order_id`
- `product_id`
- `sales`
- `quantity`
- `discount`
- `profit`
- `shipping_cost`

Cette table conserve le grain ligne de commande.

### 14. `order_returns.csv`

Colonne :

- `order_id`

Cette table modele les retours au niveau de la commande.

### 15. `normalization_anomalies.csv`

Colonnes :

- `order_id`
- `conflict_type`
- `distinct_tuple_count`
- `chosen_row_id`
- `chosen_tuple`
- `candidate_tuples`

Ce fichier journalise les cas ou un meme `order_id` avait plusieurs entetes contradictoires dans la source. Le pipeline applique une regle deterministe pour choisir le tuple conserve, puis garde une trace du conflit ici.

## Ordre d'import recommande

Apres execution de `create_sales_schema.sql`, importer les fichiers dans cet ordre :

1. `categories.csv`
2. `sub_categories.csv`
3. `managers.csv`
4. `regions.csv`
5. `markets.csv`
6. `countries.csv`
7. `states.csv`
8. `cities.csv`
9. `locations.csv`
10. `customers.csv`
11. `products.csv`
12. `orders_normalized.csv`
13. `order_items.csv`
14. `order_returns.csv`

## Points importants

- `category_id`, `sub_category_id`, `manager_id`, `region_id`, `market_id`, `country_id`, `state_id`, `city_id` et `location_id` sont des identifiants techniques stables dans cette livraison
- `product_id`, `customer_id`, `order_id` et `row_id` restent des cles metier
- `postal_code` reste en texte nullable
- `order_date` et `ship_date` doivent etre importees comme des dates
- les montants doivent etre importes en numerique

## Conclusion

Ce dossier constitue la version la plus normalisee du projet a ce stade. Il est mieux adapte a une vraie base relationnelle PostgreSQL que la version precedente basee sur une table `locations` plus denormalisee et sur des references textuelles pour les categories, sous-categories, regions et managers.
