# Rapport de conception du pipeline d'analyse des ventes

## Introduction

Ce document presente de maniere detaillee le pipeline de preparation des donnees de ventes construit pour ce projet. L'objectif n'etait pas seulement de charger quelques fichiers CSV et de produire un resultat final, mais de mettre en place une demarche claire, progressive et robuste, en restant strictement dans le cadre de Python et Pandas.

Le besoin de depart etait simple en apparence : partir de plusieurs fichiers CSV contenant des informations sur les ventes, les retours et les responsables par region, puis construire un dataset final propre, coherent et exploitable pour l'analyse. En realite, ce type de travail demande plusieurs choix de conception importants : quel niveau de nettoyage appliquer, comment gerer les incoherences, quel type de jointure utiliser, quelles colonnes calculer, et surtout comment preparer le resultat pour un usage futur dans PostgreSQL via PgAdmin.

Le pipeline a donc ete pense comme une suite d'etapes logiques, chacune ayant un role precis : charger, comprendre, verifier, nettoyer, enrichir, valider puis exporter.

## 1. Contexte du projet

Le projet repose sur trois fichiers CSV :

- `orders.csv`
- `people.csv`
- `returns.csv`

Ces fichiers ne jouent pas tous le meme role, et leur structure n'est pas identique.

`orders.csv` est la source principale. C'est elle qui contient les lignes de ventes, les informations clients, les produits, les montants, les remises, les profits et les couts de livraison. C'est la table centrale du pipeline.

`people.csv` joue un role de table de correspondance. Il associe une region a une personne responsable. Cette table sert donc a enrichir les donnees de ventes avec une information supplementaire de type organisationnelle.

`returns.csv` contient les commandes retournees. Cette table ne decrit pas une ligne de produit, mais un evenement de retour au niveau de la commande. Cela introduit une difference importante de granularite par rapport a `orders.csv`.

Le livrable attendu n'etait pas une base relationnelle complete, mais un dataset final a plat, propre et exploitable, exporte en CSV et pense pour un import futur dans PgAdmin.

## 2. Contraintes de depart

Plusieurs contraintes ont directement influence la conception du pipeline.

### 2.1 Python et Pandas uniquement

Le pipeline devait etre construit avec Python et Pandas uniquement. Cela excluait l'usage de bibliotheques ETL specialisees, d'outils SQL integres au pipeline, ou de frameworks plus lourds. Cette contrainte a oriente le travail vers une approche simple, lisible et reproductible.

### 2.2 Code clair et structure comme un pipeline

Le code devait etre facile a lire et a expliquer. Il ne s'agissait pas seulement d'obtenir un resultat fonctionnel, mais de produire une logique pedagogique, avec des etapes bien separees. C'est pour cette raison qu'un notebook Jupyter a ete privilegie : il permet de decouper proprement la demarche en blocs comprehensibles.

### 2.3 Compatibilite avec un import futur dans PostgreSQL via PgAdmin

Cette contrainte a eu un impact tres concret sur plusieurs choix :

- adoption de noms de colonnes en `snake_case`
- attention particuliere aux types de donnees
- conservation de formats simples et stables
- export final en CSV a plat

Le pipeline ne devait pas seulement "marcher dans Pandas", mais produire une sortie facile a importer dans une base PostgreSQL sans friction inutile.

### 2.4 Ne pas detruire l'information metier

Quand on nettoie des donnees, il est tentant de supprimer tout ce qui semble imparfait. Pourtant, un pipeline analytique robuste ne doit pas effacer des informations legitimes sous pretexte qu'elles sont atypiques. Cette contrainte n'etait pas formulee explicitement au depart, mais elle s'est imposee comme principe de travail au fil de l'analyse.

Par exemple, un profit negatif n'est pas forcement une erreur. Une valeur manquante sur un code postal ne justifie pas necessairement la suppression d'une vente. Une region absente de la table `people` ne signifie pas que la transaction doit disparaitre du jeu final.

## 3. Analyse initiale des fichiers

Avant de nettoyer ou fusionner quoi que ce soit, il etait necessaire de comprendre la structure reelle des donnees.

### 3.1 `orders.csv`

Ce fichier contient 51 290 lignes et 24 colonnes. Il s'agit de la table principale du projet.

Un point important est apparu rapidement : `order_id` n'est pas unique. Cela signifie qu'on n'est pas au niveau "commande", mais au niveau "ligne de commande". Une meme commande peut donc apparaitre plusieurs fois, ce qui est normal si elle contient plusieurs produits.

Cette observation a ete determinante, car elle interdit certaines simplifications dangereuses. Par exemple, il aurait ete faux de dedoublonner `order_id` ou de considerer toute repetition comme une anomalie.

### 3.2 `people.csv`

Ce fichier est tres petit : 24 lignes et 2 colonnes. Sa fonction est simple, mais importante. Il associe une personne a une region.

Lors de l'inspection, un detail de qualite a attire l'attention : certaines valeurs de texte contenaient des caracteres d'espacement non standards, notamment des espaces inseparables. Ce type de probleme est discret, mais il peut casser des comparaisons ou produire des sorties peu propres si on ne le traite pas.

### 3.3 `returns.csv`

Ce fichier contient 1 079 lignes. Contrairement a `orders.csv`, sa logique est portee par la commande retournee, pas par la ligne de commande.

Cette difference de granularite etait une difficulte importante. Si une commande est retournee et qu'elle contient plusieurs lignes dans `orders.csv`, alors l'information de retour devra etre propagee a toutes les lignes correspondantes lors de la fusion.

## 4. Pourquoi le pipeline a ete structure en etapes

Le pipeline final a ete organise en huit sections visibles dans le notebook :

1. Configuration
2. Chargement des fichiers CSV
3. Exploration de la qualite des donnees
4. Nettoyage des jeux de donnees
5. Fusion des jeux de donnees
6. Creation des indicateurs metier
7. Validation finale
8. Export du CSV final

Ce decoupage n'est pas uniquement esthetique. Il repond a une logique de travail.

Si l'on melange exploration, nettoyage, fusion et export dans un seul bloc, on perd rapidement la maitrise du pipeline. En revanche, en separant les etapes, on rend le raisonnement visible :

- d'abord comprendre les donnees
- ensuite les corriger
- puis les relier
- ensuite les enrichir
- enfin verifier et exporter

Cette organisation est aussi utile pour la maintenance. Si demain un nouveau probleme apparait dans `people.csv`, il sera naturel d'aller regarder la partie nettoyage ou validation correspondante.

## 5. Etape 1 - Configuration

La premiere etape du notebook sert a poser le cadre du traitement :

- import des modules necessaires
- definition des chemins de fichiers
- reglage de quelques options d'affichage Pandas

Cette etape peut sembler secondaire, mais elle a une vraie utilite. Elle centralise les chemins d'entree et de sortie, ce qui rend le notebook plus lisible et plus facile a reutiliser.

Le choix a aussi ete fait de rester tres sobre sur les dependances. Aucune bibliotheque annexe n'a ete ajoutee au pipeline principal afin de respecter la contrainte initiale.

## 6. Etape 2 - Chargement des fichiers CSV

Le chargement des fichiers est la premiere etape "metier" du pipeline. C'est ici que plusieurs decisions techniques importantes ont ete prises.

### 6.1 Standardisation immediate des noms de colonnes

Des le chargement, toutes les colonnes sont renommees en `snake_case`.

Exemples :

- `Order ID` devient `order_id`
- `Postal Code` devient `postal_code`
- `Shipping Cost` devient `shipping_cost`

Ce choix a ete adopte pour plusieurs raisons :

- les noms avec espaces sont moins pratiques a manipuler dans Pandas
- ils compliquent les requetes SQL dans PostgreSQL
- ils augmentent le risque d'erreur dans les jointures, selections et transformations

Le `snake_case` est un compromis tres solide entre lisibilite, standardisation et compatibilite SQL.

### 6.2 Gestion de `postal_code` comme texte

L'une des decisions les plus importantes concerne la colonne `postal_code`.

Dans le fichier source, cette colonne avait ete interpretee comme numerique, ce qui produisait des valeurs du type `73120.0`. Pourtant, un code postal n'est pas une mesure. C'est un identifiant.

Le choix retenu a donc ete de traiter `postal_code` comme une chaine nullable.

Cette decision repond a plusieurs contraintes :

- eviter les suffixes `.0` parasites
- conserver d'eventuels zeros initiaux dans d'autres cas
- preparer une colonne compatible avec PostgreSQL
- ne pas faire croire qu'il s'agit d'une variable numerique exploitable pour des calculs

### 6.3 Encodage et lecture prudente

Les fichiers ont ete lus en UTF-8, mais l'inspection des donnees a montre que certaines valeurs de `people.csv` restaient visuellement imparfaites a cause d'espaces speciaux ou de caracteres lies a l'encodage initial de la source.

Cela a confirme qu'un simple `read_csv` ne suffisait pas a garantir une qualite textuelle propre. Il fallait une phase de normalisation plus explicite.

## 7. Etape 3 - Exploration de la qualite des donnees

Avant de nettoyer les donnees, le pipeline produit un diagnostic de base :

- types de colonnes
- nombre de valeurs manquantes
- nombre de doublons exacts

Cette etape est essentielle, car elle permet de fonder les decisions de nettoyage sur des constats reels et non sur des suppositions.

### 7.1 Valeurs manquantes

Le principal manque detecte concerne `postal_code`, avec 41 296 valeurs nulles dans `orders.csv`.

Face a ce constat, plusieurs options etaient possibles :

- supprimer les lignes incompletes
- remplir artificiellement les valeurs
- conserver les nulls tels quels

La troisieme option a ete retenue.

Pourquoi ? Parce qu'aucune regle fiable ne permettait d'imputer un code postal manquant, et parce que supprimer ces lignes aurait detruit une grande partie du dataset. Dans une logique analytique, il est preferable de conserver la vente et d'assumer l'absence de code postal.

### 7.2 Doublons

Aucun doublon exact n'a ete detecte dans les trois fichiers.

Ce constat est important car il montre que les repetitions de `order_id` dans `orders.csv` ne sont pas des erreurs de duplication brute, mais des repetitions metier legitimes.

### 7.3 Coherence temporelle

Les colonnes `order_date` et `ship_date` ont ete converties en dates et controlees. Aucun cas d'expedition anterieure a la commande n'a ete detecte.

Ce resultat est rassurant, car il confirme que les dates sont exploitables pour calculer un delai de livraison sans devoir corriger des incoherences fortes.

### 7.4 Profit negatif

12 544 lignes presentent un profit negatif.

La tentation pourrait etre de considerer cela comme une anomalie, mais ce serait une erreur d'analyse. Un profit negatif peut parfaitement correspondre a une vente non rentable, une remise importante ou un cout logistique eleve. Le pipeline conserve donc ces lignes sans correction.

Ce point illustre une idee importante : tout ce qui est atypique n'est pas faux.

## 8. Etape 4 - Nettoyage des donnees

Le nettoyage a ete pense comme une correction de forme et non comme une redefinition arbitraire du contenu.

### 8.1 Normalisation des textes

Une fonction de normalisation a ete appliquee aux colonnes textuelles afin de :

- supprimer les espaces en debut et fin de chaine
- remplacer les espaces inseparables
- reduire les sequences d'espaces multiples a un seul espace

Ce choix est particulierement utile pour les noms de personnes et certaines valeurs descriptives. Sans cette etape, des jointures ou des comparaisons textuelles peuvent echouer de facon invisible.

### 8.2 Conversion des types

Les dates ont ete converties avec `pd.to_datetime`.

Les colonnes numeriques importantes ont ete converties explicitement avec `pd.to_numeric` :

- `sales`
- `quantity`
- `discount`
- `profit`
- `shipping_cost`

Cette conversion explicite permet de mieux controler les erreurs eventuelles et de ne pas laisser Pandas inferer les types de maniere implicite sans verification.

### 8.3 Nettoyage specifique de `postal_code`

Le pipeline supprime le suffixe `.0` quand il apparait dans `postal_code`.

Ce point peut sembler mineur, mais il est important pour la qualite finale. Un code postal comme `73120.0` est peu lisible, trompeur semantiquement et peu satisfaisant pour un import en base. Le ramener a `73120` rend la donnee plus propre sans la denaturer.

### 8.4 Gestion des doublons

Le pipeline supprime uniquement les doublons exacts.

Le choix est prudent et volontaire. Il aurait ete risqué d'appliquer une logique de dedoublonnage plus aggressive sur des cles comme `order_id`, `customer_id` ou `product_id`, car on aurait pu supprimer des lignes valides.

Autrement dit, le pipeline nettoie la structure, mais ne pretend pas "simplifier" le metier a la place des donnees.

## 9. Etape 5 - Fusion des datasets

La fusion est le coeur du pipeline, car c'est elle qui transforme plusieurs sources separees en un seul dataset analytique.

### 9.1 Jointure entre `orders` et `returns`

La premiere fusion relie `orders` et `returns` par `order_id`.

Le choix de la jointure `left` est fondamental. Il garantit que toutes les lignes de `orders` sont conservees, meme si une commande n'apparait pas dans `returns`.

Ensuite, un indicateur booleen `is_returned` est cree :

- `True` si la commande apparait dans `returns`
- `False` sinon

Comme `returns.csv` fonctionne au niveau de la commande, cette information est reportee sur toutes les lignes de commande partageant le meme `order_id`.

### 9.2 Jointure entre le resultat et `people`

La deuxieme fusion relie les ventes enrichies a `people` sur la colonne `region`.

Ici aussi, la jointure `left` a ete retenue afin de ne jamais perdre une ligne de vente si la region n'existe pas dans la table des responsables.

Cette decision s'est revelee necessaire dans la pratique : 384 lignes de `orders.csv` n'ont pas de correspondance dans `people.csv`, toutes liees a la region `Canada`.

Plutot que de supprimer ces lignes ou d'inventer une valeur, le pipeline conserve la vente et laisse `manager_name` a null.

Ce comportement est plus honnete analytiquement. Il signale clairement qu'une information manque dans la table de reference, sans sacrifier la transaction.

## 10. Etape 6 - Creation des indicateurs metier

Une fois les donnees chargees, nettoyees et fusionnees, le pipeline cree plusieurs indicateurs utiles pour l'analyse.

### 10.1 `sales_amount`

Cette colonne reprend la valeur de `sales`.

Sur le plan technique, c'est une duplication volontaire. Sur le plan metier, elle rend le dataset plus explicite, car le terme `sales_amount` parle davantage a un utilisateur qui cherche le chiffre d'affaires.

### 10.2 `unit_price`

Cette colonne est calculee comme :

`sales / quantity`

Elle donne une estimation du prix unitaire moyen sur la ligne de commande. C'est une variable analytique utile pour comparer les produits ou repérer des effets de remise.

### 10.3 `shipping_delay_days`

Cette colonne mesure le nombre de jours entre `order_date` et `ship_date`.

Ce calcul a ete rendu possible par la verification prealable de la coherence des dates. Sans cette precaution, le pipeline aurait pu produire des delais negatifs ou absurdes.

### 10.4 `profit_margin`

Cette marge est calculee comme :

`profit / sales`

Le pipeline integre une protection contre la division par zero afin d'eviter les erreurs techniques ou les resultats infinis.

### 10.5 `manager_name`

Cette colonne provient de `people.csv` et apporte un enrichissement organisationnel utile pour des analyses par zone ou par responsable.

### 10.6 `is_returned`

Cet indicateur binaire permet d'identifier rapidement les lignes associees a une commande retournee. Il est particulierement utile pour croiser les retours avec les categories de produits, les segments clients ou les marges.

## 11. Etape 7 - Validation finale

Une fois le dataset final constitue, plusieurs controles ont ete appliques avant l'export.

L'idee ici etait simple : un pipeline analytique ne doit pas seulement transformer les donnees, il doit aussi verifier qu'il n'a pas introduit d'incoherence.

Les principales validations mises en place sont les suivantes :

- unicite de `people.region`
- unicite de `returns.order_id`
- conservation du nombre de lignes apres les jointures
- validite des dates
- verification que `ship_date >= order_date`
- verification que `quantity > 0`
- verification que `sales >= 0`
- verification que `discount` reste entre 0 et 1

Ces controles jouent un role de garde-fou. Ils permettent de detecter rapidement un probleme de source ou une regression dans le pipeline.

## 12. Etape 8 - Export final

Le dataset final est exporte dans `sales_final_clean.csv`.

Le format de sortie a ete pense pour etre simple a reutiliser dans PostgreSQL :

- colonnes en `snake_case`
- dates au format ISO `YYYY-MM-DD`
- structure a plat
- valeurs nulles conservees quand elles ont du sens

Le choix d'un CSV final unique, plutot que de plusieurs tables separees, a ete fait pour simplifier l'import initial dans PgAdmin. C'est une approche pragmatique : elle permet de commencer rapidement l'exploration SQL, quitte a normaliser davantage plus tard si le projet evolue vers un schema relationnel plus riche.

## 13. Difficultes rencontrees et solutions adoptees

Cette partie est importante, car elle montre que la construction du pipeline n'a pas consisté a appliquer des recettes automatiques, mais a faire des choix.

### 13.1 Difference de granularite entre les tables

`orders.csv` est au niveau ligne de commande, tandis que `returns.csv` est au niveau commande.

Probleme : une commande retournee peut correspondre a plusieurs lignes dans `orders.csv`.

Solution retenue : fusionner sur `order_id` et propager l'information de retour a toutes les lignes correspondantes via `is_returned`.

### 13.2 Valeurs manquantes sur `postal_code`

Probleme : un grand nombre de codes postaux est absent.

Solutions envisagees :

- suppression des lignes
- imputation artificielle
- conservation des nulls

Solution retenue : conserver les nulls. Cette option est la plus honnete et la moins destructive.

### 13.3 Regions absentes de `people.csv`

Probleme : certaines ventes ne trouvent pas de responsable associe.

Solution retenue : garder la ligne de vente et laisser `manager_name` a null.

Cette decision preserve l'integrite du dataset principal et evite de fausser les analyses de ventes.

### 13.4 Texte imparfait dans `people.csv`

Probleme : certains noms contenaient des espaces speciaux ou des formes de texte peu propres.

Solution retenue : normalisation des chaines de caracteres avant la fusion et avant la sortie.

### 13.5 Risque de sur-nettoyage

Probleme : dans un pipeline de donnees, il est facile de nettoyer "trop fort".

Solution retenue : appliquer une logique conservative :

- corriger la forme
- verifier les types
- supprimer uniquement les doublons exacts
- ne pas supprimer des lignes metier valides

## 14. Pourquoi ce pipeline est adapte a PostgreSQL

Le pipeline a ete pense des le debut avec un import futur vers PostgreSQL en tete.

Plusieurs choix vont dans ce sens :

- noms de colonnes standardises
- absence d'espaces dans les noms
- types simples
- dates explicites
- structure tabulaire stable
- export CSV unique

Cela reduit fortement les manipulations supplementaires a faire dans PgAdmin. En pratique, il sera plus simple de definir la table cible, de mapper les colonnes et de charger le fichier final.

## 15. Limites actuelles du pipeline

Le pipeline repond bien au besoin actuel, mais il a aussi des limites normales.

D'abord, il est specifique aux trois fichiers fournis. Ce n'est pas un moteur generique de traitement de tout dossier CSV.

Ensuite, il produit un dataset final denormalise. C'est tres pratique pour l'analyse rapide, mais ce n'est pas la structure la plus elegante si l'on souhaite ensuite construire une base relationnelle complete avec plusieurs tables liees.

Enfin, le nettoyage reste volontairement prudent. Certaines corrections plus avancees pourraient etre ajoutees plus tard, mais seulement si des regles metier claires sont definies.

## Conclusion

Le pipeline construit pour ce projet repond a un objectif concret : transformer plusieurs CSV heterogenes en un dataset final propre, lisible et pret pour une exploitation analytique ou un import PostgreSQL.

Sa force principale ne vient pas d'une complexite technique particuliere, mais de la qualite des choix adoptes :

- respecter la structure reelle des donnees
- nettoyer sans detruire
- fusionner sans perdre d'information
- enrichir avec des indicateurs utiles
- verifier avant d'exporter

Autrement dit, le pipeline ne cherche pas seulement a produire un fichier final. Il cherche a produire un fichier final fiable.

Dans un projet de donnees, cette difference est essentielle.
