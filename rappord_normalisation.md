# Explication du fichier `build_normalized_sales_exports.py`

## Objectif global du script

Le script `build_normalized_sales_exports.py` transforme des fichiers CSV "bruts" (`orders.csv`, `people.csv`, `returns.csv`) en plusieurs tables CSV plus propres et plus normalisées, prêtes à être importées dans PostgreSQL.

Le problème global qu'il essaie de résoudre est le suivant :

- les données d'origine mélangent plusieurs niveaux d'information dans un même fichier ;
- certaines valeurs sont mal formatées ou incohérentes ;
- un même `order_id` peut apparaître sur plusieurs lignes avec des informations d'en-tête contradictoires ;
- la base cible attend des dimensions et des faits séparés, avec des clés cohérentes.

Le script découpe donc le fichier source en dimensions (`customers`, `products`, `markets`, `locations`, etc.) et en tables de faits (`orders_normalized`, `order_items`, `order_returns`), puis vérifie que l'ensemble reste cohérent.

## Fonctions utilitaires

### `to_snake_case(name: str) -> str`

**Rôle**
Convertir un nom de colonne source en nom compatible SQL, au format `snake_case`.

**Problème résolu**
Les colonnes des CSV peuvent contenir des espaces, des tirets, des slashs ou des caractères peu adaptés à un schéma SQL.

**Approche**
La fonction :

- met le texte en minuscules ;
- remplace certains séparateurs (`-`, `/`) par `_` ;
- remplace les caractères non alphanumériques par `_` ;
- supprime les répétitions et les `_` inutiles au début et à la fin.

### `normalize_text_value(value)`

**Rôle**
Nettoyer une valeur texte individuelle.

**Problème résolu**
Les CSV contiennent souvent des espaces parasites, des espaces insécables, ou des cellules vides qui devraient être traitées comme nulles.

**Approche**
La fonction conserve les valeurs nulles, remplace les espaces spéciaux, compacte les espaces multiples et renvoie `pd.NA` si le résultat final est vide.

### `normalize_text_columns(df: pd.DataFrame) -> pd.DataFrame`

**Rôle**
Appliquer le nettoyage texte à toutes les colonnes textuelles d'un DataFrame.

**Problème résolu**
Sans normalisation globale, deux valeurs visuellement proches peuvent être considérées comme différentes à cause d'espaces ou d'un formatage irrégulier.

**Approche**
La fonction parcourt toutes les colonnes de type texte et applique `normalize_text_value` colonne par colonne.

### `clean_postal_code(series: pd.Series) -> pd.Series`

**Rôle**
Nettoyer les codes postaux tout en les gardant comme texte nullable.

**Problème résolu**
Les codes postaux ne doivent pas être traités comme de vrais nombres : sinon on peut perdre des zéros initiaux ou récupérer des formats parasites comme `12345.0`.

**Approche**
La fonction convertit la série en chaîne, remplace les pseudo-nulls par `pd.NA`, puis retire le suffixe `.0` typique d'une mauvaise conversion Excel.

### `stable_sort(df: pd.DataFrame, by: list[str]) -> pd.DataFrame`

**Rôle**
Trier un DataFrame de manière déterministe.

**Problème résolu**
Quand le script génère des identifiants entiers, il faut que l'ordre soit stable d'une exécution à l'autre pour éviter des IDs qui changent inutilement.

**Approche**
La fonction utilise `mergesort`, un tri stable, puis réinitialise l'index.

### `add_integer_id(df: pd.DataFrame, id_column: str) -> pd.DataFrame`

**Rôle**
Ajouter une clé entière séquentielle au début d'une table.

**Problème résolu**
Plusieurs dimensions doivent recevoir une clé technique entière (`category_id`, `market_id`, etc.) au lieu de reposer uniquement sur une valeur métier.

**Approche**
La fonction insère une colonne allant de `1` à `n`, puis la convertit en type nullable `Int64`.

## Chargement et nettoyage des sources

### `load_sources() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]`

**Rôle**
Charger les trois fichiers sources et faire le nettoyage de base partagé.

**Problème résolu**
Les fichiers bruts ne sont pas directement exploitables : noms de colonnes hétérogènes, valeurs texte irrégulières, dates et nombres encore sous forme de chaînes.

**Approche**
La fonction :

- lit `orders.csv`, `people.csv` et `returns.csv` ;
- renomme les colonnes avec `to_snake_case` ;
- normalise les colonnes texte ;
- supprime les doublons exacts ;
- convertit les types importants (`row_id`, dates, métriques numériques) ;
- nettoie `postal_code` ;
- harmonise quelques colonnes métier, par exemple `person -> manager_name` et `region -> region_name`.

## Résolution des en-têtes de commande

### `build_header_resolution(orders: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]`

**Rôle**
Construire une version unique et normalisée de l'en-tête de chaque commande, une ligne par `order_id`.

**Problème résolu**
Dans les données source, un même `order_id` peut apparaître sur plusieurs lignes avec des informations d'en-tête différentes : client, dates, mode d'expédition, pays, ville, région, etc. Or une table `orders` normalisée doit avoir un seul en-tête par commande.

**Approche**
La fonction utilise une stratégie de résolution explicite :

- elle définit les colonnes qui représentent l'en-tête de commande ;
- elle sérialise cet ensemble de colonnes en une clé comparable (`header_key`) ;
- elle compte, pour chaque `order_id`, combien de fois chaque version d'en-tête apparaît ;
- elle choisit la version la plus fréquente ;
- en cas d'égalité, elle prend celle associée au plus petit `row_id`.

Elle produit aussi une table d'anomalies.

Cette table d'anomalies résout un second problème : ne pas perdre la trace des conflits. Au lieu d'écraser silencieusement les divergences, la fonction enregistre :

- le nombre de variantes trouvées ;
- la version retenue ;
- les versions candidates ;
- les `row_id` concernés.

## Construction des dimensions produit et client

### `build_product_dimensions(orders: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]`

**Rôle**
Construire les dimensions `customers`, `categories`, `sub_categories` et `products`.

**Problème résolu**
Le fichier `orders` contient tout à plat. Pour une base relationnelle normalisée, il faut séparer :

- les clients ;
- les catégories ;
- les sous-catégories ;
- les produits.

**Approche**
La fonction :

- extrait les clients sans doublons, en gardant `customer_id` comme clé métier ;
- crée une table `categories` avec un `category_id` entier ;
- crée une table `sub_categories` liée à `categories` par `category_id` ;
- crée une table `products` liée à `sub_categories` par `sub_category_id`.

L'idée centrale est de remplacer progressivement les libellés textuels répétitifs par des clés numériques plus propres pour un modèle relationnel.

## Construction des dimensions géographiques et managériales

### `build_geo_dimensions(people: pd.DataFrame, resolved_headers: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]`

**Rôle**
Construire les dimensions `managers`, `regions`, `markets`, `countries`, `states`, `cities` et `locations`.

**Problème résolu**
Les informations géographiques et organisationnelles sont imbriquées dans les données source. Le script doit les séparer en plusieurs niveaux pour éviter la redondance et permettre des jointures propres.

**Approche**
La fonction reconstruit une hiérarchie de référence :

- `market -> country -> state -> city -> location`

En parallèle, elle garde `region` comme dimension métier distincte, car une région commerciale n'est pas forcément un niveau administratif classique.

Plus précisément :

- `managers` est construit à partir de `people` ;
- `regions` relie les régions aux managers ;
- `markets`, `countries`, `states`, `cities` et `locations` sont déduits des en-têtes de commande déjà résolus ;
- des tables intermédiaires de lookup sont reconstruites pour rattacher correctement chaque niveau au précédent.

Le problème important que cette fonction résout est la reconstitution d'une hiérarchie cohérente à partir d'un fichier source dénormalisé.

## Construction des tables de faits

### `build_fact_tables(...) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]`

**Rôle**
Construire les tables `orders_normalized`, `order_items` et `order_returns`.

**Problème résolu**
Une fois les dimensions créées, il faut réexprimer les données transactionnelles à un grain correct :

- une ligne par commande pour `orders_normalized` ;
- une ligne par article de commande pour `order_items` ;
- une ligne par commande retournée pour `order_returns`.

**Approche**
La fonction :

- reconstitue un `location_id` à partir du triplet pays/état/ville et du code postal ;
- utilise les en-têtes résolus pour produire `orders_normalized` ;
- conserve le grain original des lignes de commande dans `order_items` ;
- filtre `returns` pour ne garder que les commandes marquées `Yes`.

Le point clé est qu'elle raccorde les faits aux dimensions normalisées sans perdre le niveau de détail utile.

## Validation

### `validate_outputs(...) -> None`

**Rôle**
Vérifier que les tables produites sont cohérentes avant export.

**Problème résolu**
Un pipeline de normalisation peut sembler fonctionner tout en générant des clés dupliquées, des références cassées ou des dépendances métier incohérentes.

**Approche**
La fonction utilise une série de `assert` pour contrôler :

- l'unicité des clés primaires ;
- la cohérence des volumes attendus ;
- l'intégrité référentielle entre tables ;
- certaines dépendances métier, par exemple :
  - un `customer_id` ne doit pas pointer vers plusieurs noms ;
  - un `product_id` ne doit pas pointer vers plusieurs sous-catégories ;
  - un pays ne doit pas appartenir à plusieurs marchés.

Elle vérifie aussi que les anomalies enregistrées concernent bien des commandes existantes.

## Export

### `export_csv(df: pd.DataFrame, path: Path, date_columns: list[str] | None = None) -> None`

**Rôle**
Exporter une table en CSV.

**Problème résolu**
Certaines colonnes de date doivent être écrites dans un format stable et lisible par PostgreSQL ou par un processus d'import.

**Approche**
Avant l'export, la fonction reformate les colonnes de date demandées en `YYYY-MM-DD`, puis écrit le fichier sans index.

## Orchestration générale

### `main() -> None`

**Rôle**
Piloter toute la chaîne de traitement de bout en bout.

**Problème résolu**
Sans fonction centrale, les étapes seraient dispersées et difficiles à exécuter dans le bon ordre.

**Approche**
La fonction :

- crée le dossier de sortie si nécessaire ;
- charge les sources ;
- résout les en-têtes de commande ;
- construit les dimensions ;
- construit les tables de faits ;
- valide le résultat ;
- exporte tous les CSV ;
- affiche un résumé compact du nombre de lignes produites par table.

## Résumé de la logique du script

En pratique, le script suit cette logique :

1. nettoyer les sources ;
2. résoudre les ambiguïtés sur les commandes ;
3. extraire des dimensions stables ;
4. rattacher les faits à ces dimensions ;
5. vérifier la cohérence globale ;
6. exporter des fichiers prêts pour une base relationnelle.

Autrement dit, ce fichier essaie surtout de résoudre un problème classique de data engineering : transformer un dataset transactionnel brut et partiellement incohérent en modèle relationnel propre, traçable et importable.
