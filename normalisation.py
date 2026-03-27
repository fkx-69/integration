"""Build normalized PostgreSQL-ready CSV exports from the sales source files.

This script turns the raw CSV sources into a more normalized relational model:
- product hierarchy with integer IDs for categories and sub-categories
- manager and region dimensions with integer IDs
- normalized geography split into markets, countries, states, cities and locations
- order headers separated from order lines
- anomaly logging for conflicting order headers sharing the same order_id

The generated files are written to ``normalized_sql_output`` and are designed to
match the PostgreSQL schema defined in ``create_sales_schema.sql``.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "normalized_sql_output"
ORDERS_PATH = BASE_DIR / "orders.csv"
PEOPLE_PATH = BASE_DIR / "people.csv"
RETURNS_PATH = BASE_DIR / "returns.csv"

CUSTOMERS_PATH = OUTPUT_DIR / "customers.csv"
CATEGORIES_PATH = OUTPUT_DIR / "categories.csv"
SUB_CATEGORIES_PATH = OUTPUT_DIR / "sub_categories.csv"
PRODUCTS_PATH = OUTPUT_DIR / "products.csv"
MANAGERS_PATH = OUTPUT_DIR / "managers.csv"
REGIONS_PATH = OUTPUT_DIR / "regions.csv"
MARKETS_PATH = OUTPUT_DIR / "markets.csv"
COUNTRIES_PATH = OUTPUT_DIR / "countries.csv"
STATES_PATH = OUTPUT_DIR / "states.csv"
CITIES_PATH = OUTPUT_DIR / "cities.csv"
LOCATIONS_PATH = OUTPUT_DIR / "locations.csv"
ORDERS_NORMALIZED_PATH = OUTPUT_DIR / "orders_normalized.csv"
ORDER_ITEMS_PATH = OUTPUT_DIR / "order_items.csv"
ORDER_RETURNS_PATH = OUTPUT_DIR / "order_returns.csv"
ANOMALIES_PATH = OUTPUT_DIR / "normalization_anomalies.csv"


def to_snake_case(name: str) -> str:
    """Convert a source column name into a SQL-friendly snake_case name."""
    name = name.strip().lower()
    name = name.replace("-", "_").replace("/", "_")
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return re.sub(r"_+", "_", name).strip("_")


def normalize_text_value(value):
    """Normalize whitespace in one scalar text value while preserving nulls."""
    if pd.isna(value):
        return pd.NA
    text = str(value).replace("\xa0", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text if text else pd.NA


def normalize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Apply text normalization to every object/string column of a DataFrame."""
    df = df.copy()
    for column in df.select_dtypes(include=["object", "string"]).columns:
        df[column] = df[column].map(normalize_text_value)
    return df


def clean_postal_code(series: pd.Series) -> pd.Series:
    """Keep postal codes as nullable text and remove Excel-like '.0' suffixes."""
    series = series.astype("string").str.strip()
    series = series.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "<NA>": pd.NA})
    return series.str.replace(r"\.0$", "", regex=True)


def stable_sort(df: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    """Sort deterministically with mergesort so generated IDs stay stable."""
    return df.sort_values(by=by, kind="mergesort").reset_index(drop=True)


def add_integer_id(df: pd.DataFrame, id_column: str) -> pd.DataFrame:
    """Add a sequential integer identifier column at the beginning of a table."""
    df = df.reset_index(drop=True).copy()
    df.insert(0, id_column, range(1, len(df) + 1))
    df[id_column] = df[id_column].astype("Int64")
    return df


def load_sources() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load raw CSV files and perform shared low-level cleaning.

    This step standardizes column names, normalizes text values, parses the
    main date/numeric columns and prepares the three source tables for the
    higher-level normalization steps.
    """
    orders = pd.read_csv(ORDERS_PATH, encoding="utf-8", dtype={"Postal Code": "string"})
    people = pd.read_csv(PEOPLE_PATH, encoding="utf-8")
    returns = pd.read_csv(RETURNS_PATH, encoding="utf-8")

    orders = normalize_text_columns(orders.rename(columns=to_snake_case)).drop_duplicates().copy()
    people = normalize_text_columns(people.rename(columns=to_snake_case)).drop_duplicates().copy()
    returns = normalize_text_columns(returns.rename(columns=to_snake_case)).drop_duplicates().copy()

    orders["row_id"] = pd.to_numeric(orders["row_id"], errors="coerce").astype("Int64")
    orders["order_date"] = pd.to_datetime(orders["order_date"], errors="coerce")
    orders["ship_date"] = pd.to_datetime(orders["ship_date"], errors="coerce")
    orders["postal_code"] = clean_postal_code(orders["postal_code"])

    for column in ["sales", "quantity", "discount", "profit", "shipping_cost"]:
        orders[column] = pd.to_numeric(orders[column], errors="coerce")
    orders["quantity"] = orders["quantity"].astype("Int64")

    returns["returned"] = returns["returned"].str.title()
    people = people.rename(columns={"person": "manager_name", "region": "region_name"})

    return orders, people, returns


def build_header_resolution(orders: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Resolve one normalized order header per order_id.

    Some source rows share the same ``order_id`` but disagree on customer,
    shipping or location attributes. To keep a normalized ``orders`` table, the
    script compares full header tuples and keeps the most frequent one. Ties
    are broken by the smallest ``row_id``. Every conflicting order is also
    logged into the anomalies table.
    """
    header_columns = [
        "customer_id",
        "order_date",
        "ship_date",
        "ship_mode",
        "order_priority",
        "country",
        "state",
        "city",
        "postal_code",
        "region",
        "market",
    ]

    work = orders[["row_id", "order_id"] + header_columns].copy()
    serialized = work[header_columns].copy()
    serialized["order_date"] = serialized["order_date"].dt.strftime("%Y-%m-%d")
    serialized["ship_date"] = serialized["ship_date"].dt.strftime("%Y-%m-%d")
    for column in header_columns:
        serialized[column] = serialized[column].astype("string").fillna("<NULL>")
    # Serialize the entire header to compare exact competing versions of the
    # same order across multiple source rows.
    work["header_key"] = serialized.apply(lambda row: "||".join(row.tolist()), axis=1)

    counts = (
        work.groupby(["order_id", "header_key"], as_index=False)
        .agg(tuple_count=("row_id", "size"), chosen_row_id=("row_id", "min"))
    )
    chosen = counts.sort_values(
        by=["order_id", "tuple_count", "chosen_row_id"],
        ascending=[True, False, True],
        kind="mergesort",
    )
    chosen = chosen.groupby("order_id", as_index=False).head(1).copy()
    chosen["chosen_row_id"] = chosen["chosen_row_id"].astype("Int64")

    resolved_headers = chosen[["order_id", "header_key", "chosen_row_id"]].merge(
        work[["order_id", "header_key", "row_id"] + header_columns],
        left_on=["order_id", "header_key", "chosen_row_id"],
        right_on=["order_id", "header_key", "row_id"],
        how="left",
        validate="one_to_one",
    ).drop(columns=["row_id"])

    candidate_rows = work.groupby("order_id", as_index=False).agg(distinct_tuple_count=("header_key", "nunique"))
    candidate_rows = candidate_rows[candidate_rows["distinct_tuple_count"] > 1].copy()

    anomalies = []
    if not candidate_rows.empty:
        conflict_rows = counts.merge(candidate_rows[["order_id"]], on="order_id", how="inner")
        for order_id, group in conflict_rows.groupby("order_id", sort=True):
            selected = resolved_headers.loc[resolved_headers["order_id"] == order_id].iloc[0]
            detail_rows = work.loc[work["order_id"] == order_id, ["row_id", "header_key"] + header_columns].copy()

            candidate_payload = []
            for _, row in group.sort_values(by=["tuple_count", "chosen_row_id"], ascending=[False, True], kind="mergesort").iterrows():
                matching = detail_rows.loc[detail_rows["header_key"] == row["header_key"]].copy()
                sample = matching.iloc[0]
                tuple_dict = {}
                for column in header_columns:
                    value = sample[column]
                    if pd.isna(value):
                        tuple_dict[column] = None
                    elif isinstance(value, pd.Timestamp):
                        tuple_dict[column] = value.strftime("%Y-%m-%d")
                    else:
                        tuple_dict[column] = str(value)
                candidate_payload.append(
                    {
                        "tuple_count": int(row["tuple_count"]),
                        "row_ids": [int(v) for v in matching["row_id"].sort_values().tolist()],
                        "header_tuple": tuple_dict,
                    }
                )

            chosen_tuple = {}
            for column in header_columns:
                value = selected[column]
                if pd.isna(value):
                    chosen_tuple[column] = None
                elif isinstance(value, pd.Timestamp):
                    chosen_tuple[column] = value.strftime("%Y-%m-%d")
                else:
                    chosen_tuple[column] = str(value)

            anomalies.append(
                {
                    "order_id": order_id,
                    "conflict_type": "order_header_conflict",
                    "distinct_tuple_count": int(candidate_rows.loc[candidate_rows["order_id"] == order_id, "distinct_tuple_count"].iloc[0]),
                    "chosen_row_id": int(selected["chosen_row_id"]),
                    "chosen_tuple": json.dumps(chosen_tuple, ensure_ascii=True, sort_keys=True),
                    "candidate_tuples": json.dumps(candidate_payload, ensure_ascii=True, sort_keys=True),
                }
            )

    anomaly_df = pd.DataFrame(
        anomalies,
        columns=[
            "order_id",
            "conflict_type",
            "distinct_tuple_count",
            "chosen_row_id",
            "chosen_tuple",
            "candidate_tuples",
        ],
    )

    return resolved_headers, anomaly_df


def build_product_dimensions(
    orders: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build customer and product-related dimensions.

    Customers keep their business key. Categories and sub-categories receive
    integer IDs, and products are updated to point to ``sub_category_id``
    instead of the sub-category name.
    """
    customers = stable_sort(
        orders[["customer_id", "customer_name", "segment"]].drop_duplicates(),
        ["customer_id"],
    )

    categories = add_integer_id(
        stable_sort(
            orders[["category"]].drop_duplicates().rename(columns={"category": "category_name"}),
            ["category_name"],
        ),
        "category_id",
    )

    sub_categories = stable_sort(
        orders[["sub_category", "category"]]
        .drop_duplicates()
        .rename(columns={"sub_category": "sub_category_name", "category": "category_name"}),
        ["sub_category_name"],
    ).merge(
        categories,
        on="category_name",
        how="left",
        validate="many_to_one",
    )[
        ["sub_category_name", "category_id"]
    ]
    sub_categories = add_integer_id(sub_categories, "sub_category_id")
    sub_categories["category_id"] = sub_categories["category_id"].astype("Int64")

    products = stable_sort(
        orders[["product_id", "product_name", "sub_category"]]
        .drop_duplicates()
        .rename(columns={"sub_category": "sub_category_name"}),
        ["product_id"],
    ).merge(
        sub_categories[["sub_category_id", "sub_category_name"]],
        on="sub_category_name",
        how="left",
        validate="many_to_one",
    )[
        ["product_id", "product_name", "sub_category_id"]
    ]
    products["sub_category_id"] = products["sub_category_id"].astype("Int64")

    return customers, categories, sub_categories, products


def build_geo_dimensions(
    people: pd.DataFrame, resolved_headers: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build normalized geography and management dimensions.

    The geography is decomposed into a hierarchy:
    ``market -> country -> state -> city -> location``.
    Regions are modeled separately because they are business regions linked to
    managers and are not always equivalent to an administrative level.
    """
    managers = add_integer_id(
        stable_sort(
            people[["manager_name"]].drop_duplicates(),
            ["manager_name"],
        ),
        "manager_id",
    )

    order_regions = resolved_headers[["region"]].drop_duplicates().rename(columns={"region": "region_name"})
    people_regions = people[["region_name", "manager_name"]].drop_duplicates()
    regions = stable_sort(
        order_regions.merge(people_regions, on="region_name", how="outer"),
        ["region_name"],
    ).merge(
        managers,
        on="manager_name",
        how="left",
        validate="many_to_one",
    )[
        ["region_name", "manager_id"]
    ]
    regions = add_integer_id(regions, "region_id")
    regions["manager_id"] = regions["manager_id"].astype("Int64")

    markets = add_integer_id(
        stable_sort(
            resolved_headers[["market"]].drop_duplicates().rename(columns={"market": "market_name"}),
            ["market_name"],
        ),
        "market_id",
    )

    countries = stable_sort(
        resolved_headers[["country", "market"]]
        .drop_duplicates()
        .rename(columns={"country": "country_name", "market": "market_name"}),
        ["country_name"],
    ).merge(
        markets,
        on="market_name",
        how="left",
        validate="many_to_one",
    )[
        ["country_name", "market_id"]
    ]
    countries = add_integer_id(countries, "country_id")
    countries["market_id"] = countries["market_id"].astype("Int64")

    states = stable_sort(
        resolved_headers[["country", "state"]]
        .drop_duplicates()
        .rename(columns={"country": "country_name", "state": "state_name"}),
        ["country_name", "state_name"],
    ).merge(
        countries[["country_id", "country_name"]],
        on="country_name",
        how="left",
        validate="many_to_one",
    )[
        ["state_name", "country_id"]
    ]
    states = add_integer_id(states, "state_id")
    states["country_id"] = states["country_id"].astype("Int64")

    cities = stable_sort(
        resolved_headers[["country", "state", "city", "region"]]
        .drop_duplicates()
        .rename(columns={"country": "country_name", "state": "state_name", "city": "city_name", "region": "region_name"}),
        ["country_name", "state_name", "city_name"],
    )
    state_lookup = states[["state_id", "state_name", "country_id"]].merge(
        countries[["country_id", "country_name"]],
        on="country_id",
        how="left",
        validate="many_to_one",
    )
    cities = cities.merge(
        state_lookup,
        on=["country_name", "state_name"],
        how="left",
        validate="many_to_one",
    ).merge(
        regions[["region_id", "region_name"]],
        on="region_name",
        how="left",
        validate="many_to_one",
    )[
        ["city_name", "state_id", "region_id"]
    ]
    cities = add_integer_id(cities, "city_id")
    cities["state_id"] = cities["state_id"].astype("Int64")
    cities["region_id"] = cities["region_id"].astype("Int64")

    locations = stable_sort(
        resolved_headers[["country", "state", "city", "postal_code"]]
        .drop_duplicates()
        .rename(columns={"country": "country_name", "state": "state_name", "city": "city_name"}),
        ["country_name", "state_name", "city_name", "postal_code"],
    )
    # Rebuild a country/state/city lookup from the normalized hierarchy so
    # order headers can be mapped back to a ``location_id``.
    city_lookup = cities[["city_id", "city_name", "state_id"]].merge(
        states[["state_id", "state_name", "country_id"]].merge(
            countries[["country_id", "country_name"]],
            on="country_id",
            how="left",
            validate="many_to_one",
        ),
        on="state_id",
        how="left",
        validate="many_to_one",
    )
    locations = locations.merge(
        city_lookup[["city_id", "country_name", "state_name", "city_name"]],
        on=["country_name", "state_name", "city_name"],
        how="left",
        validate="many_to_one",
    )[
        ["city_id", "postal_code"]
    ]
    locations = add_integer_id(locations, "location_id")
    locations["city_id"] = locations["city_id"].astype("Int64")

    return managers, regions, markets, countries, states, cities, locations


def build_fact_tables(
    orders: pd.DataFrame,
    returns: pd.DataFrame,
    resolved_headers: pd.DataFrame,
    locations: pd.DataFrame,
    countries: pd.DataFrame,
    states: pd.DataFrame,
    cities: pd.DataFrame,
    products: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build order headers, order lines and returned-order tables.

    ``orders_normalized`` uses the resolved order header and the normalized
    ``location_id``. ``order_items`` preserves the original line grain, and
    ``order_returns`` stays at the order level.
    """
    city_lookup = cities[["city_id", "city_name", "state_id"]].merge(
        states[["state_id", "state_name", "country_id"]].merge(
            countries[["country_id", "country_name"]],
            on="country_id",
            how="left",
            validate="many_to_one",
        ),
        on="state_id",
        how="left",
        validate="many_to_one",
    )
    location_lookup = locations.merge(
        city_lookup[["city_id", "country_name", "state_name", "city_name"]],
        on="city_id",
        how="left",
        validate="many_to_one",
    )
    header_locations = resolved_headers.rename(
        columns={"country": "country_name", "state": "state_name", "city": "city_name"}
    ).merge(
        location_lookup[["location_id", "country_name", "state_name", "city_name", "postal_code"]],
        on=["country_name", "state_name", "city_name", "postal_code"],
        how="left",
        validate="many_to_one",
    )

    orders_normalized = stable_sort(
        header_locations[
            [
                "order_id",
                "customer_id",
                "location_id",
                "order_date",
                "ship_date",
                "ship_mode",
                "order_priority",
            ]
        ].drop_duplicates(),
        ["order_id"],
    )
    orders_normalized["location_id"] = orders_normalized["location_id"].astype("Int64")

    product_lookup = products[["product_id", "sub_category_id"]].copy()
    order_items = stable_sort(
        orders[
            [
                "row_id",
                "order_id",
                "product_id",
                "sales",
                "quantity",
                "discount",
                "profit",
                "shipping_cost",
            ]
        ].merge(
            product_lookup,
            on="product_id",
            how="left",
            validate="many_to_one",
        )[
            [
                "row_id",
                "order_id",
                "product_id",
                "sales",
                "quantity",
                "discount",
                "profit",
                "shipping_cost",
            ]
        ],
        ["row_id"],
    )

    order_returns = stable_sort(
        returns.loc[returns["returned"] == "Yes", ["order_id"]].drop_duplicates(),
        ["order_id"],
    )

    return orders_normalized, order_items, order_returns


def validate_outputs(
    orders: pd.DataFrame,
    returns: pd.DataFrame,
    customers: pd.DataFrame,
    categories: pd.DataFrame,
    sub_categories: pd.DataFrame,
    products: pd.DataFrame,
    managers: pd.DataFrame,
    regions: pd.DataFrame,
    markets: pd.DataFrame,
    countries: pd.DataFrame,
    states: pd.DataFrame,
    cities: pd.DataFrame,
    locations: pd.DataFrame,
    orders_normalized: pd.DataFrame,
    order_items: pd.DataFrame,
    order_returns: pd.DataFrame,
    anomalies: pd.DataFrame,
) -> None:
    """Run consistency checks on keys, foreign keys and business dependencies."""
    assert customers["customer_id"].is_unique
    assert categories["category_id"].is_unique
    assert categories["category_name"].is_unique
    assert sub_categories["sub_category_id"].is_unique
    assert sub_categories["sub_category_name"].is_unique
    assert products["product_id"].is_unique
    assert managers["manager_id"].is_unique
    assert managers["manager_name"].is_unique
    assert regions["region_id"].is_unique
    assert regions["region_name"].is_unique
    assert markets["market_id"].is_unique
    assert markets["market_name"].is_unique
    assert countries["country_id"].is_unique
    assert countries["country_name"].is_unique
    assert states["state_id"].is_unique
    assert cities["city_id"].is_unique
    assert locations["location_id"].is_unique
    assert orders_normalized["order_id"].is_unique
    assert order_items["row_id"].is_unique
    assert order_returns["order_id"].is_unique

    assert len(customers) == orders["customer_id"].nunique()
    assert len(products) == orders["product_id"].nunique()
    assert len(orders_normalized) == orders["order_id"].nunique()
    assert len(order_items) == len(orders)
    assert len(order_returns) == returns.loc[returns["returned"] == "Yes", "order_id"].nunique()

    assert set(sub_categories["category_id"]).issubset(set(categories["category_id"]))
    assert set(products["sub_category_id"]).issubset(set(sub_categories["sub_category_id"]))
    assert set(regions["manager_id"].dropna().astype("Int64")).issubset(set(managers["manager_id"]))
    assert set(countries["market_id"]).issubset(set(markets["market_id"]))
    assert set(states["country_id"]).issubset(set(countries["country_id"]))
    assert set(cities["state_id"]).issubset(set(states["state_id"]))
    assert set(cities["region_id"]).issubset(set(regions["region_id"]))
    assert set(locations["city_id"]).issubset(set(cities["city_id"]))
    assert set(orders_normalized["customer_id"]).issubset(set(customers["customer_id"]))
    assert set(orders_normalized["location_id"]).issubset(set(locations["location_id"]))
    assert set(order_items["product_id"]).issubset(set(products["product_id"]))
    assert set(order_items["order_id"]).issubset(set(orders_normalized["order_id"]))
    assert set(order_returns["order_id"]).issubset(set(orders_normalized["order_id"]))

    assert int((orders.groupby("customer_id")["customer_name"].nunique() > 1).sum()) == 0
    assert int((orders.groupby("customer_id")["segment"].nunique() > 1).sum()) == 0
    assert int((orders.groupby("product_id")["product_name"].nunique() > 1).sum()) == 0
    assert int((orders.groupby("product_id")["sub_category"].nunique() > 1).sum()) == 0
    assert int((orders.groupby("sub_category")["category"].nunique() > 1).sum()) == 0
    assert int((orders.groupby("country")["market"].nunique() > 1).sum()) == 0
    assert int((orders.groupby(["country", "state", "city"])["region"].nunique() > 1).sum()) == 0

    if not anomalies.empty:
        assert set(anomalies["order_id"]).issubset(set(orders_normalized["order_id"]))


def export_csv(df: pd.DataFrame, path: Path, date_columns: list[str] | None = None) -> None:
    """Export one table to CSV, formatting selected datetime columns as ISO dates."""
    export_df = df.copy()
    for column in date_columns or []:
        export_df[column] = export_df[column].dt.strftime("%Y-%m-%d")
    export_df.to_csv(path, index=False)


def main() -> None:
    """Generate all normalized outputs and print a compact summary."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    orders, people, returns = load_sources()
    resolved_headers, anomalies = build_header_resolution(orders)
    customers, categories, sub_categories, products = build_product_dimensions(orders)
    managers, regions, markets, countries, states, cities, locations = build_geo_dimensions(
        people, resolved_headers
    )
    orders_normalized, order_items, order_returns = build_fact_tables(
        orders,
        returns,
        resolved_headers,
        locations,
        countries,
        states,
        cities,
        products,
    )

    validate_outputs(
        orders,
        returns,
        customers,
        categories,
        sub_categories,
        products,
        managers,
        regions,
        markets,
        countries,
        states,
        cities,
        locations,
        orders_normalized,
        order_items,
        order_returns,
        anomalies,
    )

    export_csv(customers, CUSTOMERS_PATH)
    export_csv(categories, CATEGORIES_PATH)
    export_csv(sub_categories, SUB_CATEGORIES_PATH)
    export_csv(products, PRODUCTS_PATH)
    export_csv(managers, MANAGERS_PATH)
    export_csv(regions, REGIONS_PATH)
    export_csv(markets, MARKETS_PATH)
    export_csv(countries, COUNTRIES_PATH)
    export_csv(states, STATES_PATH)
    export_csv(cities, CITIES_PATH)
    export_csv(locations, LOCATIONS_PATH)
    export_csv(orders_normalized, ORDERS_NORMALIZED_PATH, date_columns=["order_date", "ship_date"])
    export_csv(order_items, ORDER_ITEMS_PATH)
    export_csv(order_returns, ORDER_RETURNS_PATH)
    export_csv(anomalies, ANOMALIES_PATH)

    summary = {
        "customers": len(customers),
        "categories": len(categories),
        "sub_categories": len(sub_categories),
        "products": len(products),
        "managers": len(managers),
        "regions": len(regions),
        "markets": len(markets),
        "countries": len(countries),
        "states": len(states),
        "cities": len(cities),
        "locations": len(locations),
        "orders_normalized": len(orders_normalized),
        "order_items": len(order_items),
        "order_returns": len(order_returns),
        "normalization_anomalies": len(anomalies),
    }
    print(f"output_directory: {OUTPUT_DIR}")
    for name, count in summary.items():
        print(f"{name}: {count}")


if __name__ == "__main__":
    main()
