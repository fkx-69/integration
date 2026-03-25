# Guide des sorties normalisees pour PostgreSQL

## Objectif

Ce dossier regroupe les livrables finaux a utiliser pour construire une base de donnees PostgreSQL normalisee a partir des fichiers source `orders.csv`, `people.csv` et `returns.csv`.

L'objectif n'est plus d'avoir un seul fichier analytique a plat, mais un ensemble de tables separees qui respectent une logique relationnelle plus propre, proche de la troisieme forme normale (3NF pragmatique).

## Contenu du dossier

Ce dossier doit contenir les fichiers suivants :

- `customers.csv`
- `categories.csv`
- `sub_categories.csv`
- `products.csv`
- `regions.csv`
- `locations.csv`
- `orders_normalized.csv`
- `order_items.csv`
- `order_returns.csv`
- `normalization_anomalies.csv`
- `create_sales_schema.sql`

## Logique du schema

La structure a ete pensee pour separer les grandes entites metier et limiter les redondances.

### 1. `customers.csv`

Cette table contient les clients.

Colonnes :

- `customer_id`
- `customer_name`
- `segment`

Pourquoi cette separation :

Dans le fichier source, les informations du client se repetent sur de nombreuses lignes de commande. Les sortir dans une table dediee permet d'eviter cette repetition et de faire dependre `customer_name` et `segment` uniquement de `customer_id`.

### 2. `categories.csv`

Cette table contient les grandes categories de produits.

Colonne :

- `category_name`

Pourquoi cette separation :

Le nombre de categories est faible et stable. Les isoler permet de poser une hierarchie produit plus propre.

### 3. `sub_categories.csv`

Cette table contient les sous-categories de produits.

Colonnes :

- `sub_category_name`
- `category_name`

Pourquoi cette separation :

Une sous-categorie depend d'une categorie. Cela permet d'exprimer explicitement la relation entre les deux niveaux.

### 4. `products.csv`

Cette table contient les produits.

Colonnes :

- `product_id`
- `product_name`
- `sub_category_name`

Pourquoi cette separation :

Les informations produit sont repetitives dans les lignes de vente. Les isoler dans `products` permet de faire dependre le nom du produit et sa sous-categorie uniquement de `product_id`.

### 5. `regions.csv`

Cette table contient les regions et, quand l'information existe, le responsable associe.

Colonnes :

- `region_name`
- `manager_name`

Pourquoi cette separation :

La region est une dimension stable. Le responsable de region depend de la region, pas d'une commande ou d'une ligne de commande.

Remarque :

Certaines regions presentes dans les ventes ne sont pas renseignees dans `people.csv`. Dans ce cas, `manager_name` reste vide au lieu de supprimer des donnees.

### 6. `locations.csv`

Cette table contient les localisations consolidees.

Colonnes :

- `location_id`
- `country`
- `state`
- `city`
- `postal_code`
- `region_name`
- `market`

Pourquoi cette separation :

La geographie n'avait pas de cle metier simple et stable dans les CSV. Une cle technique `location_id` a donc ete introduite. Cela permet d'eviter de dupliquer toute la localisation sur chaque commande.

### 7. `orders_normalized.csv`

Cette table represente l'entete de commande.

Colonnes :

- `order_id`
- `customer_id`
- `location_id`
- `order_date`
- `ship_date`
- `ship_mode`
- `order_priority`

Pourquoi cette separation :

Les informations de commande doivent dependre de la commande, pas de la ligne de commande. Cette table isole donc les attributs communs a une commande.

### 8. `order_items.csv`

Cette table represente les lignes de commande.

Colonnes :

- `row_id`
- `order_id`
- `product_id`
- `sales`
- `quantity`
- `discount`
- `profit`
- `shipping_cost`

Pourquoi cette separation :

Le fichier source `orders.csv` est en realite une table de lignes de commande. Cette table conserve ce niveau de granularite, tout en reliant chaque ligne a une commande et a un produit.

### 9. `order_returns.csv`

Cette table contient les commandes retournees.

Colonne :

- `order_id`

Pourquoi cette separation :

Les retours sont portes au niveau de la commande et non de la ligne de commande. Cette table permet de modeliser ce fait metier proprement.

### 10. `normalization_anomalies.csv`

Ce fichier journalise les anomalies rencontrees lors de la normalisation.

Colonnes :

- `order_id`
- `conflict_type`
- `distinct_tuple_count`
- `chosen_row_id`
- `chosen_tuple`
- `candidate_tuples`

Pourquoi ce fichier existe :

Certaines commandes source partagent le meme `order_id` mais presentent des valeurs contradictoires sur l'entete de commande, par exemple sur le client, la ville, la region ou le mode d'expedition. Pour normaliser la base sans perdre de donnees, une regle deterministe a ete appliquee :

- on compare les tuples d'entete complets
- on retient le tuple le plus frequent
- en cas d'egalite, on prend celui de la plus petite `row_id`

Toutes les commandes conflictuelles sont journalisees ici pour garder une trace des arbitrages effectues.

## Script SQL

Le fichier `create_sales_schema.sql` permet de creer les tables PostgreSQL, les cles primaires, les cles etrangeres et les index principaux.

Le schema SQL est concu pour une base vide dediee uniquement a ces donnees.

## Ordre d'import recommande dans PostgreSQL

Voici l'ordre dans lequel les CSV doivent etre importes apres execution du script SQL :

1. `categories.csv`
2. `sub_categories.csv`
3. `customers.csv`
4. `regions.csv`
5. `locations.csv`
6. `products.csv`
7. `orders_normalized.csv`
8. `order_items.csv`
9. `order_returns.csv`

Cet ordre respecte les dependances entre tables et evite les violations de cles etrangeres.

## Points importants avant import

- `postal_code` doit rester en texte
- `order_date` et `ship_date` doivent etre interpretees comme des dates
- `row_id` doit etre charge en entier long
- les colonnes monetaires doivent etre chargees en numerique
- les valeurs nulles de `manager_name` sont normales pour certaines regions

## Conclusion

Ce dossier constitue la sortie la plus adaptee si l'objectif est de construire directement une base PostgreSQL normalisee a partir des CSV initiaux.

Le fichier `sales_final_clean.csv` reste utile pour des analyses rapides, mais ce dossier est le bon point d'entree pour une base relationnelle propre.
