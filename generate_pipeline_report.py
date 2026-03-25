from pathlib import Path
from textwrap import wrap

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


BASE_DIR = Path(__file__).resolve().parent
ORDERS_PATH = BASE_DIR / "orders.csv"
PEOPLE_PATH = BASE_DIR / "people.csv"
RETURNS_PATH = BASE_DIR / "returns.csv"
FINAL_DATASET_PATH = BASE_DIR / "sales_final_clean.csv"
PDF_PATH = BASE_DIR / "sales_pipeline_explanation.pdf"


def normalize_whitespace(value):
    if pd.isna(value):
        return pd.NA
    text = str(value).replace("\xa0", " ").strip()
    return " ".join(text.split()) if text else pd.NA


def load_stats():
    orders = pd.read_csv(ORDERS_PATH, dtype={"Postal Code": "string"})
    people = pd.read_csv(PEOPLE_PATH)
    returns = pd.read_csv(RETURNS_PATH)
    final_dataset = pd.read_csv(FINAL_DATASET_PATH, dtype={"postal_code": "string"})

    people_clean = people.copy()
    people_clean["Person"] = people_clean["Person"].map(normalize_whitespace)
    people_clean["Region"] = people_clean["Region"].map(normalize_whitespace)

    order_dates = pd.to_datetime(orders["Order Date"], errors="coerce")
    ship_dates = pd.to_datetime(orders["Ship Date"], errors="coerce")

    stats = {
        "orders_rows": len(orders),
        "orders_cols": len(orders.columns),
        "orders_unique_order_ids": int(orders["Order ID"].nunique()),
        "orders_unique_customers": int(orders["Customer ID"].nunique()),
        "orders_unique_products": int(orders["Product ID"].nunique()),
        "orders_duplicate_rows": int(orders.duplicated().sum()),
        "postal_code_nulls": int(orders["Postal Code"].isna().sum()),
        "negative_profit_rows": int((orders["Profit"] < 0).sum()),
        "people_rows": len(people),
        "people_duplicate_rows": int(people.duplicated().sum()),
        "people_unique_regions": int(people["Region"].nunique()),
        "returns_rows": len(returns),
        "returns_duplicate_rows": int(returns.duplicated().sum()),
        "returns_unique_order_ids": int(returns["Order ID"].nunique()),
        "returned_order_lines": int(orders["Order ID"].isin(returns["Order ID"]).sum()),
        "unmatched_regions": sorted(set(orders["Region"]) - set(people["Region"])),
        "unmatched_region_rows": int((~orders["Region"].isin(people["Region"])).sum()),
        "invalid_order_dates": int(order_dates.isna().sum()),
        "invalid_ship_dates": int(ship_dates.isna().sum()),
        "ship_before_order": int((ship_dates < order_dates).sum()),
        "final_rows": len(final_dataset),
        "final_cols": len(final_dataset.columns),
        "final_null_manager": int(final_dataset["manager_name"].isna().sum()),
    }

    return stats


def paragraph_lines(text, width=100):
    lines = []
    for paragraph in text.strip().split("\n\n"):
        wrapped = wrap(paragraph, width=width, break_long_words=False, break_on_hyphens=False)
        if wrapped:
            lines.extend(wrapped)
        else:
            lines.append("")
        lines.append("")
    if lines and lines[-1] == "":
        lines.pop()
    return lines


def add_page(pdf, title, paragraphs, footer=None):
    fig = plt.figure(figsize=(8.27, 11.69))
    fig.patch.set_facecolor("white")

    fig.text(0.08, 0.95, title, fontsize=18, fontweight="bold", va="top", ha="left", family="DejaVu Sans")

    y = 0.91
    for paragraph in paragraphs:
        for line in paragraph_lines(paragraph):
            if y < 0.07:
                if footer:
                    fig.text(0.08, 0.035, footer, fontsize=9, color="#555555", family="DejaVu Sans")
                pdf.savefig(fig, bbox_inches="tight")
                plt.close(fig)
                fig = plt.figure(figsize=(8.27, 11.69))
                fig.patch.set_facecolor("white")
                fig.text(0.08, 0.95, f"{title} (suite)", fontsize=18, fontweight="bold", va="top", ha="left", family="DejaVu Sans")
                y = 0.91

            if line.startswith("- "):
                fig.text(0.10, y, u"\u2022", fontsize=11, va="top", ha="left", family="DejaVu Sans")
                fig.text(0.125, y, line[2:], fontsize=11, va="top", ha="left", family="DejaVu Sans")
            else:
                fig.text(0.08, y, line, fontsize=11, va="top", ha="left", family="DejaVu Sans")
            y -= 0.024

    if footer:
        fig.text(0.08, 0.035, footer, fontsize=9, color="#555555", family="DejaVu Sans")

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def build_sections(stats):
    unmatched_regions = ", ".join(stats["unmatched_regions"]) if stats["unmatched_regions"] else "aucune"

    return [
        (
            "Rapport detaille du pipeline de preparation des ventes",
            [
                (
                    "Ce document explique en detail le pipeline construit dans le notebook "
                    "`sales_pipeline.ipynb`. Le pipeline repose sur Python et Pandas uniquement "
                    "pour charger, controler, nettoyer, fusionner et exporter des donnees de ventes."
                ),
                (
                    f"Le resultat produit est `sales_final_clean.csv`, un jeu de donnees a plat de "
                    f"{stats['final_rows']} lignes et {stats['final_cols']} colonnes, pense pour un import "
                    "ulterieur dans PostgreSQL via PgAdmin."
                ),
                (
                    "L'objectif du rapport n'est pas seulement de decrire les etapes techniques, "
                    "mais aussi d'expliquer pourquoi certains choix ont ete faits, quelles contraintes "
                    "les ont imposes, et quelles difficultes ont ete rencontrees pendant la construction."
                ),
            ],
        ),
        (
            "1. Contexte, objectifs et contraintes",
            [
                (
                    "Le besoin initial etait de construire un pipeline clair, pedagogique et facilement "
                    "rejouable pour trois fichiers CSV : `orders.csv`, `people.csv` et `returns.csv`. "
                    "Le perimetre retenu est volontairement specifique a ces trois sources et non un "
                    "framework generique, afin de privilegier la lisibilite et l'adaptation au besoin reel."
                ),
                (
                    "Les contraintes majeures ont directement guide la conception :"
                ),
                (
                    "- Utiliser Python et Pandas uniquement pour le pipeline de traitement des donnees.\n\n"
                    "- Produire un code structure comme un pipeline, donc lisible par etapes et simple a maintenir.\n\n"
                    "- Preparer une sortie compatible avec un import futur dans PgAdmin/PostgreSQL.\n\n"
                    "- Eviter les nettoyages destructifs ou arbitraires qui feraient perdre de l'information metier."
                ),
                (
                    "Ces contraintes ont pousse a retenir un notebook Jupyter plutot qu'un script unique, "
                    "car le notebook permet de separer clairement exploration, nettoyage, fusion, calculs "
                    "et validation, tout en gardant une logique executable de bout en bout."
                ),
            ],
        ),
        (
            "2. Comprendre les sources de donnees",
            [
                (
                    f"`orders.csv` contient {stats['orders_rows']} lignes et {stats['orders_cols']} colonnes. "
                    "Il s'agit de la table centrale du pipeline. Son grain n'est pas la commande complete, "
                    "mais la ligne de commande : un meme `order_id` peut apparaitre plusieurs fois lorsqu'une "
                    "commande contient plusieurs produits."
                ),
                (
                    f"On y trouve {stats['orders_unique_order_ids']} identifiants de commande uniques, "
                    f"{stats['orders_unique_customers']} clients distincts et {stats['orders_unique_products']} "
                    "produits distincts."
                ),
                (
                    f"`people.csv` contient {stats['people_rows']} lignes, soit une ligne par region. Cette "
                    "source joue le role d'une table de correspondance entre `region` et le responsable associe."
                ),
                (
                    f"`returns.csv` contient {stats['returns_rows']} lignes et {stats['returns_unique_order_ids']} "
                    "identifiants de commande uniques. Son grain est la commande retournee, pas la ligne de produit. "
                    "Cette difference de granularite a une consequence directe sur la facon de fusionner les tables."
                ),
            ],
        ),
        (
            "3. Etape 1 - Chargement des fichiers CSV",
            [
                (
                    "Le chargement est fait avec `pd.read_csv`, en imposant des choix prudents des le debut. "
                    "Par exemple, `Postal Code` est charge comme texte nullable plutot que comme nombre."
                ),
                (
                    "Ce choix a ete adopte pour trois raisons principales :"
                ),
                (
                    "- Un code postal n'est pas une mesure numerique, mais un identifiant.\n\n"
                    "- Certains codes peuvent commencer par zero dans d'autres jeux de donnees, ce qu'un type numerique peut detruire.\n\n"
                    "- L'import dans PostgreSQL est plus robuste si cette colonne reste textuelle."
                ),
                (
                    "Les noms de colonnes sont aussi renommes immediatement en `snake_case`. Cette normalisation "
                    "evite les espaces, les tirets et les casse mixtes qui compliquent les requetes SQL et les "
                    "jointures futures dans PostgreSQL."
                ),
            ],
        ),
        (
            "4. Etape 2 - Exploration de la qualite des donnees",
            [
                (
                    "Avant tout nettoyage, le pipeline produit un etat des lieux des types, des valeurs "
                    "manquantes et des doublons exacts. Cette etape est importante car elle permet de distinguer "
                    "les vrais problemes des particularites metier legitimes."
                ),
                (
                    f"Constats principaux releves pendant l'exploration :\n\n"
                    f"- `orders.csv` ne contient aucun doublon exact, mais {stats['postal_code_nulls']} valeurs "
                    "manquantes dans `postal_code`.\n\n"
                    f"- `people.csv` et `returns.csv` ne contiennent pas de doublons exacts.\n\n"
                    f"- Les dates de commande et d'expedition sont valides : {stats['invalid_order_dates']} date "
                    f"de commande invalide, {stats['invalid_ship_dates']} date d'expedition invalide, et "
                    f"{stats['ship_before_order']} cas ou l'expedition precede la commande."
                ),
                (
                    f"Un point important est la presence de {stats['negative_profit_rows']} lignes avec un profit "
                    "negatif. Ce n'est pas un probleme de qualite a corriger automatiquement : cela peut simplement "
                    "signifier qu'une vente a ete faite a perte. Le pipeline conserve donc cette information."
                ),
            ],
        ),
        (
            "5. Etape 3 - Nettoyage des donnees",
            [
                (
                    "Le nettoyage applique un principe de prudence : corriger les problemes de forme sans modifier "
                    "inutilement le sens metier des donnees."
                ),
                (
                    "Les actions de nettoyage retenues sont les suivantes :"
                ),
                (
                    "- Normalisation des textes : suppression des espaces parasites en debut et fin de chaine.\n\n"
                    "- Remplacement des espaces inseparables `\\xa0`, observes notamment dans certains noms de `people.csv`.\n\n"
                    "- Homogeneisation simple des chaines pour eviter des faux non-matchs pendant les jointures.\n\n"
                    "- Conversion explicite des dates avec `pd.to_datetime`.\n\n"
                    "- Conversion des mesures numeriques avec `pd.to_numeric`.\n\n"
                    "- Suppression des doublons exacts uniquement."
                ),
                (
                    "La suppression de doublons a ete volontairement limitee aux doublons exacts. Il aurait ete "
                    "dangereux de dedoublonner `order_id` dans `orders.csv`, car les repetitions d'un `order_id` "
                    "sont normales sur une table au grain ligne de commande."
                ),
                (
                    "Le nettoyage de `postal_code` retire aussi le suffixe `.0` apparu a cause d'une lecture de type "
                    "numerique dans la source brute. Cela permet d'obtenir une representation textuelle plus propre "
                    "et plus adaptee a PostgreSQL."
                ),
            ],
        ),
        (
            "6. Etape 4 - Fusion des datasets",
            [
                (
                    "La fusion est realisee en deux temps, toujours a partir de `orders`, avec des jointures `left`."
                ),
                (
                    "- `orders` est fusionne avec `returns` sur `order_id`.\n\n"
                    "- Le resultat est ensuite fusionne avec `people` sur `region`."
                ),
                (
                    "Le choix du `left join` est central. Il permet de conserver toutes les lignes de ventes, "
                    "meme lorsqu'une information complementaire est absente dans une table secondaire."
                ),
                (
                    f"Cette precaution s'est revelee utile : {stats['unmatched_region_rows']} lignes de `orders.csv` "
                    f"ne trouvent pas de correspondance dans `people.csv`, toutes sur la region suivante : "
                    f"{unmatched_regions}. Au lieu de supprimer ces ventes, le pipeline conserve les lignes et laisse "
                    "`manager_name` a null."
                ),
                (
                    f"Autre difficulte : `returns.csv` est au grain commande, tandis que `orders.csv` est au grain "
                    f"ligne de commande. Une commande retournee peut donc marquer plusieurs lignes comme retournees. "
                    f"Au final, {stats['returned_order_lines']} lignes de vente sont associees a une commande retournee."
                ),
            ],
        ),
        (
            "7. Etape 5 - Creation des indicateurs metier",
            [
                (
                    "Le pipeline enrichit le dataset avec des indicateurs simples, mais directement utiles pour une "
                    "analyse dans Pandas, SQL ou un outil BI."
                ),
                (
                    "- `sales_amount` reprend explicitement `sales` afin d'avoir un nom metier plus clair pour le chiffre d'affaires.\n\n"
                    "- `unit_price` est calcule par `sales / quantity`.\n\n"
                    "- `shipping_delay_days` mesure le delai entre la commande et l'expedition.\n\n"
                    "- `profit_margin` est calcule par `profit / sales`, avec protection contre une division par zero.\n\n"
                    "- `is_returned` est derive de la presence de la commande dans `returns.csv`.\n\n"
                    "- `manager_name` provient de la correspondance region -> personne."
                ),
                (
                    "Le choix de conserver a la fois `sales` et `sales_amount` est volontaire. Techniquement, les "
                    "deux colonnes portent la meme valeur, mais `sales_amount` rend le dataset plus explicite pour "
                    "des utilisateurs metier ou pour une lecture en base de donnees."
                ),
            ],
        ),
        (
            "8. Etape 6 - Validation finale",
            [
                (
                    "Avant l'export, le pipeline execute des assertions de coherence. Cette phase est importante car "
                    "elle transforme le notebook en pipeline fiable, et pas seulement en suite de transformations."
                ),
                (
                    "Les controles verifies sont notamment :"
                ),
                (
                    "- unicite de `people.region`\n\n"
                    "- unicite de `returns.order_id`\n\n"
                    "- conservation du nombre de lignes apres les jointures\n\n"
                    "- validite des dates\n\n"
                    "- verification que `ship_date` est posterieure ou egale a `order_date`\n\n"
                    "- verification que `quantity` est strictement positive\n\n"
                    "- verification que `sales` reste non negatif\n\n"
                    "- verification que `discount` reste entre 0 et 1"
                ),
                (
                    "Cette strategie de validation repond a une contrainte implicite forte : si le CSV final est "
                    "importe dans PostgreSQL, il vaut mieux detecter en amont les anomalies structurelles plutot que "
                    "les decouvrir plus tard au moment du chargement ou de l'analyse."
                ),
            ],
        ),
        (
            "9. Etape 7 - Export et preparation pour PgAdmin",
            [
                (
                    "La sortie finale est exportee dans `sales_final_clean.csv`. Le dataset conserve une structure "
                    "a plat, avec une ligne par ligne de commande enrichie."
                ),
                (
                    "Ce choix d'un dataset denormalise a ete retenu parce qu'il simplifie l'import initial dans "
                    "PgAdmin. Une seule table suffit pour commencer les analyses SQL sans devoir recreer tout de suite "
                    "un schema relationnel en plusieurs tables."
                ),
                (
                    "Les dates sont converties au format texte ISO `YYYY-MM-DD`. Ce format est stable, lisible et "
                    "facilement interpretable par PostgreSQL."
                ),
                (
                    f"Le fichier final contient {stats['final_rows']} lignes et {stats['final_cols']} colonnes. "
                    f"Il preserve {stats['final_null_manager']} lignes avec `manager_name` null, ce qui est preferable "
                    "a une suppression silencieuse de donnees."
                ),
            ],
        ),
        (
            "10. Difficultes rencontrees et solutions retenues",
            [
                (
                    "Le projet a mis en evidence plusieurs difficultes typiques des pipelines CSV."
                ),
                (
                    "- Difficulte : les jeux de donnees n'ont pas tous le meme grain.\n\n"
                    "  Solution : prendre `orders` comme table de reference et documenter clairement les jointures `left`.\n\n"
                    "- Difficulte : forte proportion de codes postaux manquants.\n\n"
                    "  Solution : conserver les nulls au lieu d'imputer une valeur arbitraire, car aucune regle metier fiable n'etait disponible.\n\n"
                    "- Difficulte : presence d'espaces inseparables dans certaines valeurs texte.\n\n"
                    "  Solution : normaliser les chaines pour eviter des problemes de comparaison et de jointure.\n\n"
                    "- Difficulte : certaines regions de ventes ne sont pas presentes dans `people.csv`.\n\n"
                    "  Solution : conserver les ventes et laisser `manager_name` a null.\n\n"
                    "- Difficulte : confusion possible entre doublon reel et repetiton metier normale.\n\n"
                    "  Solution : ne supprimer que les doublons exacts."
                ),
                (
                    "Ces choix montrent une orientation generale du pipeline : privilegier l'integrite analytique "
                    "des donnees plutot qu'un nettoyage agressif qui pourrait faire disparaitre des informations utiles."
                ),
            ],
        ),
        (
            "11. Conclusion",
            [
                (
                    "Le pipeline final est volontairement simple, lisible et robuste. Il ne cherche pas a resoudre "
                    "tous les cas possibles d'un systeme de production, mais il couvre proprement le besoin cible : "
                    "transformer trois CSV heterogenes en un dataset analytique exploitable."
                ),
                (
                    "Les decisions de conception les plus importantes sont coherentes avec les contraintes du projet : "
                    "Pandas uniquement, structure par etapes, conservation des ventes, colonnes SQL-friendly, et "
                    "export final adapte a PostgreSQL."
                ),
                (
                    "Le rapport peut aussi servir de base de documentation technique si le pipeline doit ensuite etre "
                    "industrialise sous forme de script, de job planifie ou de chargement automatise vers PostgreSQL."
                ),
            ],
        ),
    ]


def generate_pdf():
    stats = load_stats()
    sections = build_sections(stats)

    footer = (
        "Projet: pipeline Pandas pour la preparation des ventes | "
        "Fichiers: orders.csv, people.csv, returns.csv, sales_final_clean.csv"
    )

    with PdfPages(PDF_PATH) as pdf:
        for title, paragraphs in sections:
            add_page(pdf, title, paragraphs, footer=footer)


if __name__ == "__main__":
    generate_pdf()
    print(f"PDF generated: {PDF_PATH}")
