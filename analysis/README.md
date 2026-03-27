# Analyse Python sur PostgreSQL

Ce dossier contient la couche minimale d'analyse Python qui lit directement les donnees depuis PostgreSQL.

La logique de jointure metier reste dans la vue PostgreSQL, tandis que les chargements et agregations Python utilisent SQLAlchemy Core plutot que du SQL texte ecrit en dur dans le code.

## Fichiers

- `db.py` : creation de la connexion SQLAlchemy vers PostgreSQL
- `queries.py` : requetes SQL centralisees et loaders `pandas`
- `sales_kpis.ipynb` : notebook d'analyse KPI, graphiques et export

## Preparation

1. Installer les dependances :

```bash
pip install -r requirements-analysis.txt
```

2. Renseigner la connexion PostgreSQL dans l'environnement :

```bash
copy .env.example .env
```

Puis completer `DATABASE_URL` ou bien :

- `PGHOST`
- `PGPORT`
- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`

3. Executer le script SQL [create_sales_analysis_view.sql](C:/Users/ibrah/Downloads/integration/normalized_sql_output/create_sales_analysis_view.sql) sur la base apres l'import des tables.

## Utilisation

Depuis la racine du projet :

```bash
jupyter notebook analysis/sales_kpis.ipynb
```

Le notebook :

- verifie la connexion
- controle la coherence de `vw_sales_analysis`
- charge le DataFrame complet depuis PostgreSQL
- calcule les KPI de ventes
- produit plusieurs graphiques
- exporte `analysis_outputs/kpi_summary.csv`
