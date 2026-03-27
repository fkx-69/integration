"""SQLAlchemy Core loaders for the PostgreSQL sales analysis notebook."""

from __future__ import annotations

from collections.abc import Iterable
from functools import lru_cache

import pandas as pd
from sqlalchemy import MetaData, Numeric, Table, case, cast, distinct, false, func, literal, select, union_all

from .db import get_engine


VIEW_NAME = "vw_sales_analysis"

VIEW_COLUMNS = [
    "order_id",
    "row_id",
    "customer_id",
    "customer_name",
    "product_id",
    "product_name",
    "order_date",
    "ship_date",
    "sales",
    "quantity",
    "discount",
    "profit",
    "shipping_cost",
    "is_returned",
    "segment",
    "category_name",
    "sub_category_name",
    "market_name",
    "country_name",
    "state_name",
    "city_name",
    "region_name",
    "manager_name",
    "location_id",
    "postal_code",
    "ship_mode",
    "order_priority",
]

FILTERABLE_COLUMNS = {
    "order_id",
    "customer_id",
    "customer_name",
    "product_id",
    "product_name",
    "order_date",
    "ship_date",
    "segment",
    "category_name",
    "sub_category_name",
    "market_name",
    "country_name",
    "state_name",
    "city_name",
    "region_name",
    "manager_name",
    "ship_mode",
    "order_priority",
    "is_returned",
}


def _is_iterable_filter(value: object) -> bool:
    return isinstance(value, Iterable) and not isinstance(value, (str, bytes, dict))


def _normalize_column_name(raw_key: str) -> str:
    if raw_key.endswith("__from"):
        return raw_key[:-6]
    if raw_key.endswith("__to"):
        return raw_key[:-4]
    return raw_key


@lru_cache(maxsize=None)
def _get_table(table_name: str, schema: str | None = None) -> Table:
    metadata = MetaData()
    return Table(table_name, metadata, schema=schema, autoload_with=get_engine())


def _get_view() -> Table:
    return _get_table(VIEW_NAME, schema="public")


def _coalesce_numeric(expression, scale: int = 2):
    return func.round(cast(func.coalesce(expression, 0), Numeric), scale)


def _build_conditions(table: Table, where_clauses: dict | None) -> list:
    if not where_clauses:
        return []

    conditions: list = []
    for raw_key, value in where_clauses.items():
        if value is None:
            continue

        column_name = _normalize_column_name(raw_key)
        if column_name not in FILTERABLE_COLUMNS:
            raise ValueError(f"Unsupported filter column: {column_name}")

        column = table.c[column_name]
        if raw_key.endswith("__from"):
            conditions.append(column >= value)
            continue

        if raw_key.endswith("__to"):
            conditions.append(column <= value)
            continue

        if _is_iterable_filter(value):
            expanded_values = list(value)
            conditions.append(false() if not expanded_values else column.in_(expanded_values))
            continue

        conditions.append(column == value)

    return conditions


def load_sales_analysis(where_clauses: dict | None = None) -> pd.DataFrame:
    """Load the analysis-ready dataset from the PostgreSQL view."""
    view = _get_view()
    selected_columns = [view.c[column_name] for column_name in VIEW_COLUMNS]

    query = select(*selected_columns).order_by(view.c.order_date, view.c.order_id, view.c.row_id)
    for condition in _build_conditions(view, where_clauses):
        query = query.where(condition)

    return pd.read_sql(query, get_engine(), parse_dates=["order_date", "ship_date"])


def load_kpi_summary() -> pd.DataFrame:
    """Return a one-row DataFrame containing the core sales KPIs."""
    view = _get_view()

    total_orders = func.count(distinct(view.c.order_id))
    total_sales = func.sum(view.c.sales)
    total_profit = func.sum(view.c.profit)

    returned_orders = func.count(
        distinct(
            case(
                (view.c.is_returned.is_(True), view.c.order_id),
                else_=None,
            )
        )
    )
    return_rate_pct = 100.0 * returned_orders / func.nullif(total_orders, 0)
    average_margin_pct = 100.0 * total_profit / func.nullif(total_sales, 0)
    average_shipping_delay = func.avg(view.c.ship_date - view.c.order_date)

    query = select(
        total_orders.label("total_orders"),
        func.count(distinct(view.c.customer_id)).label("total_customers"),
        func.count(distinct(view.c.product_id)).label("total_products"),
        _coalesce_numeric(total_sales).label("total_sales"),
        _coalesce_numeric(total_profit).label("total_profit"),
        func.coalesce(func.sum(view.c.quantity), 0).label("total_quantity"),
        _coalesce_numeric(func.avg(view.c.discount), scale=4).label("average_discount"),
        _coalesce_numeric(return_rate_pct).label("return_rate_pct"),
        _coalesce_numeric(average_margin_pct).label("average_margin_pct"),
        _coalesce_numeric(average_shipping_delay).label("average_shipping_delay_days"),
    )

    return pd.read_sql(query, get_engine())


def _check_row(check_name: str, expected_value, actual_value):
    return select(
        literal(check_name).label("check_name"),
        cast(expected_value, Numeric).label("expected_value"),
        cast(actual_value, Numeric).label("actual_value"),
        (expected_value == actual_value).label("passed"),
    )


def load_validation_checks() -> pd.DataFrame:
    """Return consistency checks between the view and the source tables."""
    view = _get_view()
    order_items = _get_table("order_items")
    orders_normalized = _get_table("orders_normalized")
    order_returns = _get_table("order_returns")

    view_row_count = select(func.count()).select_from(view).scalar_subquery()
    order_items_row_count = select(func.count()).select_from(order_items).scalar_subquery()

    view_order_count = select(func.count(distinct(view.c.order_id))).select_from(view).scalar_subquery()
    orders_normalized_count = select(func.count()).select_from(orders_normalized).scalar_subquery()

    order_items_sales_sum = select(_coalesce_numeric(func.sum(order_items.c.sales))).scalar_subquery()
    view_sales_sum = select(_coalesce_numeric(func.sum(view.c.sales))).scalar_subquery()

    order_items_profit_sum = select(_coalesce_numeric(func.sum(order_items.c.profit))).scalar_subquery()
    view_profit_sum = select(_coalesce_numeric(func.sum(view.c.profit))).scalar_subquery()

    returned_line_count = (
        select(func.count())
        .select_from(order_items.join(order_returns, order_returns.c.order_id == order_items.c.order_id))
        .scalar_subquery()
    )
    returned_view_line_count = (
        select(func.count())
        .select_from(view)
        .where(view.c.is_returned.is_(True))
        .scalar_subquery()
    )

    query = union_all(
        _check_row("row_count_matches_order_items", order_items_row_count, view_row_count),
        _check_row("distinct_orders_match_orders_normalized", orders_normalized_count, view_order_count),
        _check_row("sales_sum_matches_order_items", order_items_sales_sum, view_sales_sum),
        _check_row("profit_sum_matches_order_items", order_items_profit_sum, view_profit_sum),
        _check_row("returned_line_count_matches_join", returned_line_count, returned_view_line_count),
    )

    return pd.read_sql(query, get_engine())
