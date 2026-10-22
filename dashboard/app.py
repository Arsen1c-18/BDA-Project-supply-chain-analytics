"""Streamlit dashboard for the supply-chain delivery analysis.

The dashboard reads the raw DataCo CSV (or a path supplied through
BDA_DATA_PATH). It does not train the model; it only displays artefacts saved by
the notebook in outputs/model/.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    import plotly.express as px

    HAS_PLOTLY = True
except ImportError:  # pragma: no cover - only used on minimal installs
    HAS_PLOTLY = False


st.set_page_config(
    page_title="Supply Chain Delivery Dashboard",
    page_icon="🚚",
    layout="wide",
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = Path(
    os.environ.get(
        "BDA_DATA_PATH", PROJECT_ROOT / "data" / "DataCoSupplyChainDataset.csv"
    )
).expanduser().resolve()
OUTPUT_DIR = Path(
    os.environ.get("BDA_OUTPUT_DIR", PROJECT_ROOT / "outputs")
).expanduser().resolve()
CPP_RESULTS_DIR = Path(
    os.environ.get("BDA_CPP_RESULTS", PROJECT_ROOT / "results")
).expanduser().resolve()
HADOOP_RESULTS_DIR = Path(
    os.environ.get("BDA_HADOOP_RESULTS", PROJECT_ROOT / "results" / "hadoop")
).expanduser().resolve()
HADOOP_RUN_ID = os.environ.get("BDA_HADOOP_RUN_ID", "").strip()

DELIVERY_HADOOP_COLUMNS = ["shipping_mode", "status", "distinct_orders"]
SHIPPING_HADOOP_COLUMNS = [
    "shipping_mode",
    "records",
    "distinct_orders",
    "late_records",
    "on_time_records",
    "canceled_records",
    "other_records",
    "avg_actual_days",
    "avg_scheduled_days",
    "avg_delay_days",
    "total_sales",
    "avg_sales_per_order",
]
CATEGORY_HADOOP_COLUMNS = [
    "category",
    "records",
    "distinct_orders",
    "total_quantity",
    "units_per_order",
    "total_sales",
    "avg_sales_per_item",
    "late_records",
    "late_rate_pct",
]

REQUIRED_COLUMNS = [
    "Order Id",
    "Shipping Mode",
    "Delivery Status",
    "Category Name",
    "Sales",
    "Order Item Total",
    "Order Profit Per Order",
    "Days for shipping (real)",
    "Days for shipment (scheduled)",
    "Order Item Quantity",
    "Market",
    "Order Region",
    "Customer Segment",
    "Order Status",
    "Order Item Discount Rate",
    "Product Price",
]

RENAME = {
    "Order Id": "order_id",
    "Shipping Mode": "shipping_mode",
    "Delivery Status": "delivery_status",
    "Category Name": "category",
    "Sales": "sales",
    "Order Item Total": "order_item_total",
    "Order Profit Per Order": "profit",
    "Days for shipping (real)": "actual_days",
    "Days for shipment (scheduled)": "scheduled_days",
    "Order Item Quantity": "quantity",
    "Market": "market",
    "Order Region": "order_region",
    "Customer Segment": "customer_segment",
    "Order Status": "order_status",
    "Order Item Discount Rate": "discount_rate",
    "Product Price": "product_price",
}

NUMERIC_COLUMNS = [
    "sales",
    "order_item_total",
    "profit",
    "actual_days",
    "scheduled_days",
    "quantity",
    "discount_rate",
    "product_price",
]

DELIVERY_GROUP = {
    "Late delivery": "late",
    "Shipping on time": "on_time",
    "Advance shipping": "on_time",
    "Shipping canceled": "canceled",
}


def validate_columns(path: Path) -> None:
    header = pd.read_csv(path, nrows=0, encoding="latin-1").columns.tolist()
    missing = [name for name in REQUIRED_COLUMNS if name not in header]
    if missing:
        raise ValueError(
            "The CSV is missing required columns: " + ", ".join(missing)
        )


@st.cache_data(show_spinner="Loading DataCo dataset ...")
def load_data(path_text: str) -> pd.DataFrame:
    path = Path(path_text)
    validate_columns(path)
    frame = pd.read_csv(
        path,
        usecols=REQUIRED_COLUMNS,
        encoding="latin-1",
        low_memory=False,
    )
    frame = frame.rename(columns=RENAME)

    for name in NUMERIC_COLUMNS:
        frame[name] = pd.to_numeric(frame[name], errors="coerce")

    for name in ["shipping_mode", "delivery_status", "category", "market", "order_region", "customer_segment", "order_status"]:
        frame[name] = frame[name].astype("string").str.strip()

    frame = frame.dropna(
        subset=["order_id", "shipping_mode", "delivery_status", "category"]
    )
    frame["order_id"] = frame["order_id"].astype("int64")
    frame["delivery_group"] = (
        frame["delivery_status"].map(DELIVERY_GROUP).fillna("other")
    )
    frame["is_late"] = (frame["delivery_group"] == "late").astype(int)
    frame["delivery_delta_days"] = frame["actual_days"] - frame["scheduled_days"]
    return frame


def pct(numerator: float, denominator: float) -> float:
    return 100.0 * numerator / denominator if denominator else 0.0


def latest_hadoop_run_dir() -> Path | None:
    if not HADOOP_RESULTS_DIR.exists():
        return None
    if HADOOP_RUN_ID:
        candidate = HADOOP_RESULTS_DIR / HADOOP_RUN_ID
        return candidate if candidate.is_dir() else None
    candidates = [
        path
        for path in HADOOP_RESULTS_DIR.iterdir()
        if path.is_dir() and any(path.glob("*.tsv"))
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def read_result_tsv(path: Path, columns: list[str]) -> pd.DataFrame:
    frame = pd.read_csv(
        path,
        sep="\t",
        header=None,
        names=columns,
        encoding="latin-1",
    )
    if frame.shape[1] != len(columns):
        raise ValueError(
            f"{path} has {frame.shape[1]} columns; expected {len(columns)}"
        )
    return frame


def load_hadoop_bundle():
    run_dir = latest_hadoop_run_dir()
    if run_dir is None:
        return None, None, None, None

    delivery = shipping = category = None
    delivery_path = run_dir / "delivery-mode.tsv"
    shipping_path = run_dir / "shipping-performance.tsv"
    category_path = run_dir / "category-demand.tsv"
    if delivery_path.exists():
        delivery = read_result_tsv(delivery_path, DELIVERY_HADOOP_COLUMNS)
    if shipping_path.exists():
        shipping = read_result_tsv(shipping_path, SHIPPING_HADOOP_COLUMNS)
    if category_path.exists():
        category = read_result_tsv(category_path, CATEGORY_HADOOP_COLUMNS)
    return run_dir, delivery, shipping, category


def latest_flat_delivery_result() -> tuple[pd.DataFrame | None, Path | None]:
    if not CPP_RESULTS_DIR.exists():
        return None, None
    for path in sorted(
        CPP_RESULTS_DIR.glob("*.tsv"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    ):
        try:
            frame = read_result_tsv(path, DELIVERY_HADOOP_COLUMNS)
        except Exception:
            continue
        return frame, path
    return None, None


def render_bar(
    data: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    color: str | None = None,
    text: str | None = None,
    horizontal: bool = False,
):
    if HAS_PLOTLY:
        if horizontal:
            fig = px.bar(
                data,
                x=y,
                y=x,
                orientation="h",
                title=title,
                color=color,
                text=text,
                labels={y: y.replace("_", " ").title(), x: x.replace("_", " ").title()},
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
        else:
            fig = px.bar(
                data,
                x=x,
                y=y,
                title=title,
                color=color,
                text=text,
                labels={y: y.replace("_", " ").title(), x: x.replace("_", " ").title()},
            )
        fig.update_layout(margin=dict(l=10, r=10, t=50, b=10), height=390)
        st.plotly_chart(fig, width="stretch")
    else:
        st.subheader(title)
        st.bar_chart(data.set_index(x)[y])


st.title("🚚 Supply Chain Delivery Disruption & Shipping Performance")
st.caption(
    "DataCo supply-chain dataset · 180,519 order-item rows · 65,752 unique orders · "
    "PySpark analysis plus three C++ Hadoop Streaming jobs"
)

if not DATA_PATH.exists():
    st.error(
        "Dataset not found.\n\n"
        f"Expected: `{DATA_PATH}`\n\n"
        "Set `BDA_DATA_PATH` to the CSV path or copy the dataset into `data/`."
    )
    st.stop()

try:
    df = load_data(str(DATA_PATH))
except Exception as exc:
    st.error("Could not load the dataset.")
    st.exception(exc)
    st.stop()

# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------
st.sidebar.header("Filters")
st.sidebar.caption(
    "Filters apply to order-item rows. Order metrics count distinct Order Id values."
)

modes = sorted(df["shipping_mode"].dropna().unique().tolist())
statuses = sorted(df["delivery_status"].dropna().unique().tolist())
categories = sorted(df["category"].dropna().unique().tolist())

selected_modes = st.sidebar.multiselect("Shipping mode", modes, default=modes)
selected_statuses = st.sidebar.multiselect(
    "Delivery status", statuses, default=statuses
)
selected_categories = st.sidebar.multiselect(
    "Product category", categories, default=categories
)

if not selected_modes or not selected_statuses or not selected_categories:
    st.warning("Select at least one value in each filter.")
    st.stop()

mask = (
    df["shipping_mode"].isin(selected_modes)
    & df["delivery_status"].isin(selected_statuses)
    & df["category"].isin(selected_categories)
)
filtered = df.loc[mask].copy()

if filtered.empty:
    st.warning("No rows match the selected filters.")
    st.stop()

orders = filtered.drop_duplicates("order_id")
total_orders = orders["order_id"].nunique()
total_items = len(filtered)
late_orders = int(orders["is_late"].sum())
on_time_orders = int((orders["delivery_group"] == "on_time").sum())
canceled_orders = int((orders["delivery_group"] == "canceled").sum())
active_orders = late_orders + on_time_orders
late_rate = pct(late_orders, total_orders)
active_late_rate = pct(late_orders, active_orders)

order_values = filtered.groupby("order_id")["order_item_total"].sum()
total_order_value = order_values.sum()
avg_order_value = order_values.mean()
avg_actual_days = orders["actual_days"].mean()

# ---------------------------------------------------------------------------
# KPI cards
# ---------------------------------------------------------------------------
kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
kpi1.metric("Orders", f"{total_orders:,}", help=f"{total_items:,} filtered order-item rows")
kpi2.metric("Late delivery rate", f"{late_rate:.1f}%", help="Late orders / all selected orders")
kpi3.metric(
    "Late rate (active)",
    f"{active_late_rate:.1f}%",
    help="Late orders / (late + on-time); canceled orders excluded",
)
kpi4.metric(
    "Order value (proxy)",
    f"${total_order_value:,.0f}",
    help="Sum of Order Item Total. The dataset has no shipping-cost column.",
)
kpi5.metric("Avg actual days", f"{avg_actual_days:.2f}", help="Mean Days for shipping (real)")

st.caption(
    f"On-time orders: {on_time_orders:,} · Canceled orders: {canceled_orders:,} · "
    f"Average order value: ${avg_order_value:,.0f}"
)
st.warning(
    "**No shipping-cost field exists in the dataset.** The monetary metric above is "
    "order value (`Order Item Total`), used only as a proxy. It is not a shipping charge."
)

# ---------------------------------------------------------------------------
# Shared summaries
# ---------------------------------------------------------------------------
status_counts = (
    orders["delivery_status"]
    .value_counts()
    .rename_axis("delivery_status")
    .reset_index(name="orders")
)

mode_stats = (
    orders.groupby("shipping_mode")
    .agg(
        orders=("order_id", "size"),
        late_orders=("is_late", "sum"),
        avg_actual_days=("actual_days", "mean"),
        avg_scheduled_days=("scheduled_days", "mean"),
    )
    .reset_index()
)
status_by_mode = pd.crosstab(orders["shipping_mode"], orders["delivery_group"])
for group in ["late", "on_time", "canceled", "other"]:
    if group not in status_by_mode:
        status_by_mode[group] = 0
status_by_mode = status_by_mode.reset_index()
mode_stats = mode_stats.merge(status_by_mode, on="shipping_mode", how="left")
mode_stats = mode_stats.rename(
    columns={
        "late": "late_orders_crosstab",
        "on_time": "on_time_orders",
        "canceled": "canceled_orders",
        "other": "other_orders",
    }
)
mode_stats = mode_stats.drop(columns=["late_orders_crosstab"])
mode_stats["late_rate_pct"] = (
    100 * mode_stats["late_orders"] / mode_stats["orders"]
).where(mode_stats["orders"] > 0, 0)
mode_stats["active_orders"] = (
    mode_stats["late_orders"] + mode_stats["on_time_orders"]
)
mode_stats["late_rate_active_pct"] = (
    100 * mode_stats["late_orders"] / mode_stats["active_orders"]
).where(mode_stats["active_orders"] > 0, 0)

mode_value = (
    filtered.groupby("shipping_mode")["order_item_total"]
    .sum()
    .reset_index(name="total_order_value_proxy")
)
mode_stats = mode_stats.merge(mode_value, on="shipping_mode", how="left")
mode_stats["avg_order_value_proxy"] = (
    mode_stats["total_order_value_proxy"] / mode_stats["orders"]
)

category_stats = (
    filtered.groupby("category")
    .agg(
        order_items=("order_id", "size"),
        orders=("order_id", "nunique"),
        late_items=("is_late", "sum"),
        total_sales=("sales", "sum"),
        avg_item_value=("order_item_total", "mean"),
    )
    .reset_index()
)
category_stats["late_rate_pct"] = (
    100 * category_stats["late_items"] / category_stats["order_items"]
).where(category_stats["order_items"] > 0, 0)

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_overview, tab_modes, tab_categories, tab_model, tab_hadoop = st.tabs(
    [
        "Overview",
        "Shipping performance",
        "Product categories",
        "Model",
        "Hadoop jobs",
    ]
)

with tab_overview:
    left, right = st.columns(2)
    with left:
        render_bar(
            status_counts,
            "delivery_status",
            "orders",
            "Delivery status (distinct orders)",
            text="orders",
        )
    with right:
        render_bar(
            mode_stats.sort_values("late_rate_pct", ascending=False),
            "shipping_mode",
            "late_rate_pct",
            "Late-delivery rate by shipping mode",
            text="late_rate_pct",
        )

    st.subheader("Summary by shipping mode")
    st.dataframe(
        mode_stats[
            [
                "shipping_mode",
                "orders",
                "late_orders",
                "on_time_orders",
                "canceled_orders",
                "late_rate_pct",
                "late_rate_active_pct",
                "avg_actual_days",
                "avg_scheduled_days",
            ]
        ]
        .sort_values("late_rate_pct", ascending=False)
        .style.format(
            {
                "late_rate_pct": "{:.2f}%",
                "late_rate_active_pct": "{:.2f}%",
                "avg_actual_days": "{:.2f}",
                "avg_scheduled_days": "{:.2f}",
            }
        ),
        width="stretch",
        hide_index=True,
    )

with tab_modes:
    st.subheader("Delivery outcome counts by shipping mode")
    st.dataframe(
        mode_stats[
            [
                "shipping_mode",
                "orders",
                "late_orders",
                "on_time_orders",
                "canceled_orders",
                "late_rate_active_pct",
                "avg_actual_days",
                "avg_scheduled_days",
            ]
        ].sort_values("orders", ascending=False),
        width="stretch",
        hide_index=True,
    )

    left, right = st.columns(2)
    with left:
        render_bar(
            mode_stats.sort_values("avg_actual_days", ascending=False),
            "shipping_mode",
            "avg_actual_days",
            "Average actual shipping days",
            text="avg_actual_days",
        )
    with right:
        render_bar(
            mode_stats.sort_values("avg_scheduled_days", ascending=False),
            "shipping_mode",
            "avg_scheduled_days",
            "Average scheduled shipping days",
            text="avg_scheduled_days",
        )

    st.subheader("Monetary proxy by shipping mode")
    st.caption(
        "Order value is the sum of `Order Item Total`. The dataset contains no "
        "shipping-cost column, so this is not a shipping charge."
    )
    render_bar(
        mode_stats.sort_values("total_order_value_proxy", ascending=False),
        "shipping_mode",
        "total_order_value_proxy",
        "Total order value (proxy)",
        text="total_order_value_proxy",
    )
    st.dataframe(
        mode_stats[
            [
                "shipping_mode",
                "avg_order_value_proxy",
                "total_order_value_proxy",
            ]
        ]
        .sort_values("total_order_value_proxy", ascending=False)
        .style.format(
            {
                "avg_order_value_proxy": "${:,.2f}",
                "total_order_value_proxy": "${:,.0f}",
            }
        ),
        width="stretch",
        hide_index=True,
    )

with tab_categories:
    st.subheader("Product category performance")
    st.caption(
        "Category analysis is item-level because one order can contain several "
        "product categories. 'Orders' counts distinct Order Id values."
    )
    top_categories = category_stats.sort_values("orders", ascending=False).head(15)
    left, right = st.columns(2)
    with left:
        render_bar(
            top_categories,
            "category",
            "orders",
            "Top categories by distinct orders",
            text="orders",
            horizontal=True,
        )
    with right:
        top_late = category_stats.sort_values("orders", ascending=False).head(12)
        render_bar(
            top_late.sort_values("late_rate_pct", ascending=False),
            "category",
            "late_rate_pct",
            "Item late rate — top categories",
            text="late_rate_pct",
            horizontal=True,
        )

    st.subheader("Category summary")
    st.dataframe(
        category_stats.sort_values("orders", ascending=False)
        .head(30)
        .style.format(
            {
                "late_rate_pct": "{:.2f}%",
                "total_sales": "${:,.0f}",
                "avg_item_value": "${:,.2f}",
            }
        ),
        width="stretch",
        hide_index=True,
    )

with tab_model:
    st.subheader("Late-delivery model")
    model_dir = OUTPUT_DIR / "model"
    metrics_path = model_dir / "metrics.json"
    predictions_path = model_dir / "predictions_sample.csv"

    if metrics_path.exists():
        try:
            metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
        except Exception as exc:
            st.error("Could not read model metrics.")
            st.exception(exc)
        else:
            if metrics.get("trained"):
                m1, m2, m3 = st.columns(3)
                m1.metric("Accuracy", f"{metrics.get('accuracy', 0):.4f}")
                m2.metric("F1 score", f"{metrics.get('f1', 0):.4f}")
                m3.metric("AUC", f"{metrics.get('auc', 0):.4f}")
                st.caption(
                    f"Decision tree trained in the notebook on {metrics.get('train_rows', 0):,} "
                    f"orders and evaluated on {metrics.get('test_rows', 0):,} orders. "
                    "The dashboard does not retrain the model."
                )
                if metrics.get("model_path"):
                    st.caption(f"Spark model artifact: `{metrics['model_path']}`")
                with st.expander("Model metadata and leakage controls"):
                    st.json(metrics)
            else:
                st.warning("The saved model metadata says training did not succeed.")

            if predictions_path.exists():
                st.subheader("Sample predictions")
                st.dataframe(
                    pd.read_csv(predictions_path),
                    width="stretch",
                    hide_index=True,
                )
            else:
                st.info("No sample-prediction file was saved by the notebook.")
    else:
        st.info(
            "No model metrics found. Run the machine-learning section of "
            "`notebooks/supply_chain_analysis.ipynb`; it writes "
            f"`{metrics_path}`. The dashboard never retrains the model."
        )

    st.caption(
        "The model excludes Delivery Status, Late_delivery_risk, actual shipping "
        "days, shipping date, Order Status and profit fields to reduce target leakage."
    )

with tab_hadoop:
    st.subheader("Three C++ Hadoop Streaming job results")
    st.caption(
        "These tables come from downloaded mapper/reducer output. Dashboard filters "
        "do not apply to them. Job 1 counts distinct orders; Jobs 2 and 3 also count "
        "order-item rows, so their record totals must not be compared with order totals."
    )

    try:
        run_dir, delivery_hadoop, shipping_hadoop, category_hadoop = load_hadoop_bundle()
    except Exception as exc:
        st.error("Could not read the downloaded Hadoop result bundle.")
        st.exception(exc)
        run_dir = delivery_hadoop = shipping_hadoop = category_hadoop = None
    flat_result = None
    flat_path = None
    if delivery_hadoop is None:
        flat_result, flat_path = latest_flat_delivery_result()
        delivery_hadoop = flat_result

    if run_dir is None and delivery_hadoop is None:
        st.info(
            "No downloaded Hadoop result bundle was found. Run all three jobs with:\n\n"
            "```bash\n"
            "bash scripts/run_all_hadoop_jobs.sh data/DataCoSupplyChainDataset.csv\n"
            "```\n\n"
            "The submit scripts write `results/hadoop/<run-id>/`. A local "
            "`local-verification` bundle is useful for UI testing but is not evidence "
            "of a YARN run."
        )
    else:
        if run_dir is not None:
            st.write(f"Result bundle: `{run_dir}`")
            if "local" in run_dir.name.lower():
                st.warning(
                    "This is a local verification bundle, not a YARN-distributed run. "
                    "Run `scripts/run_all_hadoop_jobs.sh` on the running cluster to "
                    "produce a job-backed bundle."
                )
        elif flat_path is not None:
            st.write(f"Legacy delivery result: `{flat_path}`")

        if delivery_hadoop is not None:
            st.markdown("#### Job 1 — delivery status (distinct orders)")
            st.dataframe(
                delivery_hadoop.sort_values(["shipping_mode", "status"]),
                width="stretch",
                hide_index=True,
            )

            full_orders = df.drop_duplicates("order_id")
            pandas_counts = (
                full_orders.assign(status=full_orders["delivery_group"])
                .groupby(["shipping_mode", "status"])
                .size()
                .reset_index(name="pandas_orders")
            )
            comparison = delivery_hadoop.rename(
                columns={"distinct_orders": "hadoop_orders"}
            ).merge(
                pandas_counts,
                on=["shipping_mode", "status"],
                how="outer",
            ).fillna(0)
            comparison["hadoop_orders"] = comparison["hadoop_orders"].astype(int)
            comparison["pandas_orders"] = comparison["pandas_orders"].astype(int)
            comparison["difference"] = (
                comparison["hadoop_orders"] - comparison["pandas_orders"]
            )
            st.caption("Job 1 vs pandas distinct-order counts")
            st.dataframe(
                comparison.sort_values(["shipping_mode", "status"]),
                width="stretch",
                hide_index=True,
            )
        else:
            st.info(
                "Job 1 result is missing from this bundle. Rerun "
                "`bash scripts/run_all_hadoop_jobs.sh`."
            )

        if shipping_hadoop is not None:
            st.markdown("#### Job 2 — shipping-mode performance")
            st.caption(
                "The sales columns are an order-value proxy (`Sales`). DataCo has no "
                "shipping-cost column, so no shipping charge is reported."
            )
            st.dataframe(
                shipping_hadoop.sort_values("shipping_mode").style.format(
                    {
                        "avg_actual_days": "{:.2f}",
                        "avg_scheduled_days": "{:.2f}",
                        "avg_delay_days": "{:.2f}",
                        "total_sales": "${:,.2f}",
                        "avg_sales_per_order": "${:,.2f}",
                    }
                ),
                width="stretch",
                hide_index=True,
            )

            left, right = st.columns(2)
            with left:
                render_bar(
                    shipping_hadoop.sort_values("avg_actual_days", ascending=False),
                    "shipping_mode",
                    "avg_actual_days",
                    "Job 2: average actual shipping days",
                    text="avg_actual_days",
                )
            with right:
                render_bar(
                    shipping_hadoop.sort_values("distinct_orders", ascending=False),
                    "shipping_mode",
                    "distinct_orders",
                    "Job 2: distinct orders",
                    text="distinct_orders",
                )

            pandas_shipping = (
                df.groupby("shipping_mode")
                .agg(
                    pandas_records=("order_id", "size"),
                    pandas_orders=("order_id", "nunique"),
                    pandas_total_sales=("sales", "sum"),
                )
                .reset_index()
            )
            shipping_compare = shipping_hadoop[
                ["shipping_mode", "records", "distinct_orders", "total_sales"]
            ].merge(pandas_shipping, on="shipping_mode", how="outer")
            shipping_compare["records_difference"] = (
                shipping_compare["records"] - shipping_compare["pandas_records"]
            )
            shipping_compare["orders_difference"] = (
                shipping_compare["distinct_orders"] - shipping_compare["pandas_orders"]
            )
            shipping_compare["sales_difference"] = (
                shipping_compare["total_sales"] - shipping_compare["pandas_total_sales"]
            ).round(2)
            st.caption("Job 2 item-row/order/sales comparison with pandas")
            st.dataframe(
                shipping_compare.sort_values("shipping_mode"),
                width="stretch",
                hide_index=True,
            )
        else:
            st.info("Job 2 result is missing from this bundle.")

        if category_hadoop is not None:
            st.markdown("#### Job 3 — category demand proxies")
            st.caption(
                "Quantity and sales are demand/revenue proxies. The dataset has no "
                "inventory-stock level, so this job does not measure shortages or "
                "inventory risk."
            )
            top_categories_hadoop = category_hadoop.sort_values(
                "total_quantity", ascending=False
            ).head(15)
            render_bar(
                top_categories_hadoop,
                "category",
                "total_quantity",
                "Job 3: units by category (top 15)",
                text="total_quantity",
                horizontal=True,
            )
            st.dataframe(
                category_hadoop.sort_values(
                    "records", ascending=False
                ).style.format(
                    {
                        "units_per_order": "{:.2f}",
                        "total_sales": "${:,.2f}",
                        "avg_sales_per_item": "${:,.2f}",
                        "late_rate_pct": "{:.2f}%",
                    }
                ),
                width="stretch",
                hide_index=True,
            )

            pandas_category = (
                df.groupby("category")
                .agg(
                    pandas_records=("order_id", "size"),
                    pandas_orders=("order_id", "nunique"),
                    pandas_quantity=("quantity", "sum"),
                    pandas_late_records=("is_late", "sum"),
                    pandas_total_sales=("sales", "sum"),
                )
                .reset_index()
            )
            category_compare = category_hadoop[
                [
                    "category",
                    "records",
                    "distinct_orders",
                    "total_quantity",
                    "late_records",
                    "total_sales",
                ]
            ].merge(pandas_category, on="category", how="outer")
            category_compare["records_difference"] = (
                category_compare["records"] - category_compare["pandas_records"]
            )
            category_compare["orders_difference"] = (
                category_compare["distinct_orders"] - category_compare["pandas_orders"]
            )
            category_compare["quantity_difference"] = (
                category_compare["total_quantity"] - category_compare["pandas_quantity"]
            )
            category_compare["late_difference"] = (
                category_compare["late_records"] - category_compare["pandas_late_records"]
            )
            category_compare["sales_difference"] = (
                category_compare["total_sales"] - category_compare["pandas_total_sales"]
            ).round(2)
            st.caption("Job 3 item/order/quantity/late/sales comparison with pandas")
            st.dataframe(
                category_compare.sort_values("pandas_quantity", ascending=False).head(
                    20
                ),
                width="stretch",
                hide_index=True,
            )
        else:
            st.info("Job 3 result is missing from this bundle.")

st.sidebar.divider()
st.sidebar.caption(f"Data: `{DATA_PATH}`")
st.sidebar.caption(f"Outputs: `{OUTPUT_DIR}`")
st.sidebar.caption(f"Hadoop results: `{HADOOP_RESULTS_DIR}`")
st.sidebar.caption(f"Rows loaded: {len(df):,}")
