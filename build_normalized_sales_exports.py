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
REGIONS_PATH = OUTPUT_DIR / "regions.csv"
LOCATIONS_PATH = OUTPUT_DIR / "locations.csv"
ORDERS_NORMALIZED_PATH = OUTPUT_DIR / "orders_normalized.csv"
ORDER_ITEMS_PATH = OUTPUT_DIR / "order_items.csv"
ORDER_RETURNS_PATH = OUTPUT_DIR / "order_returns.csv"
ANOMALIES_PATH = OUTPUT_DIR / "normalization_anomalies.csv"


def to_snake_case(name: str) -> str:
    name = name.strip().lower()
    name = name.replace("-", "_").replace("/", "_")
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return re.sub(r"_+", "_", name).strip("_")


def normalize_text_value(value):
    if pd.isna(value):
        return pd.NA
    text = str(value).replace("\xa0", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text if text else pd.NA


def normalize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    text_columns = df.select_dtypes(include=["object", "string"]).columns
    for column in text_columns:
        df[column] = df[column].map(normalize_text_value)
    return df


def clean_postal_code(series: pd.Series) -> pd.Series:
    series = series.astype("string").str.strip()
    series = series.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "<NA>": pd.NA})
    return series.str.replace(r"\.0$", "", regex=True)


def stable_sort(df: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    return df.sort_values(by=by, kind="mergesort").reset_index(drop=True)


def load_sources() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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

    numeric_columns = ["sales", "quantity", "discount", "profit", "shipping_cost"]
    for column in numeric_columns:
        orders[column] = pd.to_numeric(orders[column], errors="coerce")
    orders["quantity"] = orders["quantity"].astype("Int64")

    returns["returned"] = returns["returned"].str.title()
    people = people.rename(columns={"person": "manager_name", "region": "region_name"})

    return orders, people, returns


def build_header_resolution(orders: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
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

    chosen_rows = chosen[["order_id", "header_key", "chosen_row_id"]].merge(
        work[["order_id", "header_key", "row_id"] + header_columns],
        left_on=["order_id", "header_key", "chosen_row_id"],
        right_on=["order_id", "header_key", "row_id"],
        how="left",
        validate="one_to_one",
    )

    candidate_rows = work.groupby("order_id", as_index=False).agg(distinct_tuple_count=("header_key", "nunique"))
    candidate_rows = candidate_rows[candidate_rows["distinct_tuple_count"] > 1].copy()

    anomalies = []
    if not candidate_rows.empty:
        conflict_rows = counts.merge(candidate_rows[["order_id"]], on="order_id", how="inner")
        for order_id, group in conflict_rows.groupby("order_id", sort=True):
            selected = chosen_rows.loc[chosen_rows["order_id"] == order_id].iloc[0]
            candidate_payload = []
            detail_rows = work.loc[work["order_id"] == order_id, ["row_id", "header_key"] + header_columns].copy()
            for _, row in stable_sort(group, ["tuple_count", "chosen_row_id"]).iterrows():
                matching = detail_rows.loc[detail_rows["header_key"] == row["header_key"]].copy()
                tuple_dict = {}
                sample = matching.iloc[0]
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

    return chosen_rows.drop(columns=["row_id"]), anomaly_df


def build_dimensions(
    orders: pd.DataFrame, people: pd.DataFrame, resolved_headers: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    customers = stable_sort(
        orders[["customer_id", "customer_name", "segment"]].drop_duplicates(),
        ["customer_id"],
    )

    categories = stable_sort(
        orders[["category"]].drop_duplicates().rename(columns={"category": "category_name"}),
        ["category_name"],
    )

    sub_categories = stable_sort(
        orders[["sub_category", "category"]]
        .drop_duplicates()
        .rename(columns={"sub_category": "sub_category_name", "category": "category_name"}),
        ["sub_category_name"],
    )

    products = stable_sort(
        orders[["product_id", "product_name", "sub_category"]]
        .drop_duplicates()
        .rename(columns={"sub_category": "sub_category_name"}),
        ["product_id"],
    )

    order_regions = pd.DataFrame({"region_name": stable_sort(orders[["region"]].drop_duplicates().rename(columns={"region": "region_name"}), ["region_name"])["region_name"]})
    people_regions = people[["region_name", "manager_name"]].drop_duplicates().copy()
    regions = stable_sort(
        order_regions.merge(people_regions, on="region_name", how="outer"),
        ["region_name"],
    )

    locations = stable_sort(
        resolved_headers[
            ["country", "state", "city", "postal_code", "region", "market"]
        ]
        .drop_duplicates()
        .rename(columns={"region": "region_name"}),
        ["country", "state", "city", "postal_code", "region_name", "market"],
    ).reset_index(drop=True)
    locations.insert(0, "location_id", locations.index + 1)
    locations["location_id"] = locations["location_id"].astype("Int64")

    return customers, categories, sub_categories, products, regions, locations


def build_fact_tables(
    orders: pd.DataFrame,
    returns: pd.DataFrame,
    resolved_headers: pd.DataFrame,
    locations: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    header_locations = resolved_headers.rename(columns={"region": "region_name"}).merge(
        locations,
        on=["country", "state", "city", "postal_code", "region_name", "market"],
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
        ].copy(),
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
    regions: pd.DataFrame,
    locations: pd.DataFrame,
    orders_normalized: pd.DataFrame,
    order_items: pd.DataFrame,
    order_returns: pd.DataFrame,
    anomalies: pd.DataFrame,
) -> None:
    assert customers["customer_id"].is_unique
    assert categories["category_name"].is_unique
    assert sub_categories["sub_category_name"].is_unique
    assert products["product_id"].is_unique
    assert regions["region_name"].is_unique
    assert locations["location_id"].is_unique
    assert orders_normalized["order_id"].is_unique
    assert order_items["row_id"].is_unique
    assert order_returns["order_id"].is_unique

    assert len(customers) == orders["customer_id"].nunique()
    assert len(products) == orders["product_id"].nunique()
    assert len(orders_normalized) == orders["order_id"].nunique()
    assert len(order_items) == len(orders)
    assert len(order_returns) == returns.loc[returns["returned"] == "Yes", "order_id"].nunique()

    assert set(orders_normalized["customer_id"]).issubset(set(customers["customer_id"]))
    assert set(orders_normalized["location_id"]).issubset(set(locations["location_id"]))
    assert set(order_items["product_id"]).issubset(set(products["product_id"]))
    assert set(order_items["order_id"]).issubset(set(orders_normalized["order_id"]))
    assert set(order_returns["order_id"]).issubset(set(orders_normalized["order_id"]))

    customer_name_violations = int((orders.groupby("customer_id")["customer_name"].nunique() > 1).sum())
    customer_segment_violations = int((orders.groupby("customer_id")["segment"].nunique() > 1).sum())
    product_name_violations = int((orders.groupby("product_id")["product_name"].nunique() > 1).sum())
    product_sub_violations = int((orders.groupby("product_id")["sub_category"].nunique() > 1).sum())
    sub_category_violations = int((orders.groupby("sub_category")["category"].nunique() > 1).sum())
    assert customer_name_violations == 0
    assert customer_segment_violations == 0
    assert product_name_violations == 0
    assert product_sub_violations == 0
    assert sub_category_violations == 0

    if not anomalies.empty:
        assert set(anomalies["order_id"]).issubset(set(orders_normalized["order_id"]))


def export_csv(df: pd.DataFrame, path: Path, date_columns: list[str] | None = None) -> None:
    export_df = df.copy()
    for column in date_columns or []:
        export_df[column] = export_df[column].dt.strftime("%Y-%m-%d")
    export_df.to_csv(path, index=False)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    orders, people, returns = load_sources()
    resolved_headers, anomalies = build_header_resolution(orders)
    customers, categories, sub_categories, products, regions, locations = build_dimensions(
        orders, people, resolved_headers
    )
    orders_normalized, order_items, order_returns = build_fact_tables(
        orders, returns, resolved_headers, locations
    )

    validate_outputs(
        orders,
        returns,
        customers,
        categories,
        sub_categories,
        products,
        regions,
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
    export_csv(regions, REGIONS_PATH)
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
        "regions": len(regions),
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
