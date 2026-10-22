"""Presentation dashboard for verified supply-chain analytics."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    import plotly.express as px
    HAS_PLOTLY = True
except ImportError:  # pragma: no cover
    HAS_PLOTLY = False


st.set_page_config(page_title="Supply Chain Risk | Dashboard", layout="wide")
ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = Path(os.environ.get("BDA_DATA_PATH", ROOT / "data" / "DataCoSupplyChainDataset.csv")).expanduser()
OUTPUT_DIR = Path(os.environ.get("BDA_OUTPUT_DIR", ROOT / "outputs")).expanduser()
# The demonstration bundle is pinned to the verified YARN run. Override explicitly when needed.
HADOOP_RUN_ID = os.environ.get("BDA_HADOOP_RUN_ID", "retry-20261022T175034Z").strip()
HADOOP_DIR = Path(os.environ.get("BDA_HADOOP_RESULTS", ROOT / "results" / "hadoop")).expanduser() / HADOOP_RUN_ID

st.markdown("""
<style>
:root { --ink:#183247; --blue:#236b8e; --teal:#2b8a83; --muted:#687985; --line:#dce5e9; --paper:#f5f8fa; }
.stApp { background:var(--paper); color:var(--ink); }
[data-testid="stHeader"] { background:transparent; }
[data-testid="stMetric"] { background:white; border:1px solid var(--line); border-radius:10px; padding:14px 16px; }
[data-testid="stMetricLabel"] p { color:var(--muted); font-size:.82rem; }
[data-testid="stMetricValue"] { color:var(--ink); font-size:1.55rem; }
h1 { color:var(--ink); font-size:1.9rem !important; padding-bottom:.1rem; }
h2,h3 { color:var(--ink); }
div[data-testid="stTabs"] button { font-weight:600; }
div[data-testid="stDataFrame"] { border:1px solid var(--line); border-radius:8px; }
.note { border-left:3px solid var(--teal); background:#edf5f4; padding:10px 14px; border-radius:4px; color:#31535a; font-size:.9rem; }
</style>
""", unsafe_allow_html=True)

REQUIRED = ["Order Id", "Shipping Mode", "Delivery Status", "Category Name", "Sales", "Order Item Total", "Days for shipping (real)", "Days for shipment (scheduled)", "Order Item Quantity", "Market", "Order Region", "Customer Segment", "Order Item Discount Rate"]
OPTIONAL = ["Type", "Order Item Id", "Order Customer Id", "Payment Type"]
RENAME = {"Order Id":"order_id", "Shipping Mode":"shipping_mode", "Delivery Status":"delivery_status", "Category Name":"category", "Sales":"sales", "Order Item Total":"item_total", "Days for shipping (real)":"actual_days", "Days for shipment (scheduled)":"scheduled_days", "Order Item Quantity":"quantity", "Market":"market", "Order Region":"order_region", "Customer Segment":"customer_segment", "Order Item Discount Rate":"discount_rate", "Type":"order_type", "Order Item Id":"item_id", "Order Customer Id":"customer_id", "Payment Type":"payment_type"}
GROUP = {"Late delivery":"late", "Shipping on time":"on_time", "Advance shipping":"on_time", "Shipping canceled":"canceled"}
NUMERIC = ["sales", "item_total", "actual_days", "scheduled_days", "quantity", "discount_rate"]

@st.cache_data(show_spinner="Loading dataset…")
def load_data(path_text: str) -> pd.DataFrame:
    path = Path(path_text)
    header = pd.read_csv(path, nrows=0, encoding="latin-1").columns.tolist()
    missing = [col for col in REQUIRED if col not in header]
    if missing:
        raise ValueError("Missing required columns: " + ", ".join(missing))
    usecols = [c for c in REQUIRED + OPTIONAL if c in header]
    d = pd.read_csv(path, usecols=usecols, encoding="latin-1", low_memory=False).rename(columns=RENAME)
    for c in NUMERIC:
        d[c] = pd.to_numeric(d[c], errors="coerce")
    for c in ["shipping_mode", "delivery_status", "category", "market", "order_region", "customer_segment", "payment_type", "order_type"]:
        if c in d:
            d[c] = d[c].astype("string").str.strip()
    d = d.dropna(subset=["order_id", "shipping_mode", "delivery_status", "category"])
    d["order_id"] = d["order_id"].astype("int64")
    d["delivery_group"] = d.delivery_status.map(GROUP).fillna("other")
    d["is_late"] = (d.delivery_group == "late").astype(int)
    return d

def pct(n, den):
    return 100 * n / den if den else 0.0

def chart(data, x, y, title, *, horizontal=False, color=None, percent=False):
    if data.empty:
        st.info("No data available for the current selection.")
        return
    if HAS_PLOTLY:
        if horizontal:
            fig = px.bar(data, x=y, y=x, orientation="h", color=color, text_auto=".1f" if percent else True)
        else:
            fig = px.bar(data, x=x, y=y, color=color, text_auto=".1f" if percent else True)
        fig.update_layout(title=title, height=330, margin=dict(l=8,r=8,t=45,b=8), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#183247", showlegend=False)
        fig.update_traces(marker_color="#236b8e", textposition="outside", cliponaxis=False)
        if percent:
            (fig.update_xaxes(ticksuffix="%") if horizontal else fig.update_yaxes(ticksuffix="%"))
        st.plotly_chart(fig, width="stretch", key="chart_" + title)
    else:
        st.subheader(title)
        st.bar_chart(data.set_index(x)[y])

def download(frame, label, filename):
    st.download_button(label, frame.to_csv(index=False).encode("utf-8"), filename, "text/csv", key="download_"+filename)

st.title("Supply Chain Disruption and Inventory Risk Analysis")
st.caption("DataCo order and item analytics · Local analysis and distributed Hadoop verification")
if not DATA_PATH.exists():
    st.error("Dataset not found. Set BDA_DATA_PATH to the supported DataCo CSV.")
    st.stop()
try:
    df = load_data(str(DATA_PATH.resolve()))
except Exception as exc:
    st.error("Could not load the dataset.")
    st.exception(exc)
    st.stop()

# Reset widget state before widgets are instantiated, avoiding stale selections.
modes = sorted(df.shipping_mode.dropna().unique().tolist())
statuses = sorted(df.delivery_status.dropna().unique().tolist())
categories = sorted(df.category.dropna().unique().tolist())
for key, default in [("filter_modes", modes), ("filter_status", statuses), ("filter_categories", categories)]:
    if key not in st.session_state:
        st.session_state[key] = default

with st.sidebar:
    st.header("Dashboard controls")
    st.caption(f"Loaded rows: {len(df):,}")
    st.caption(f"Active bundle: {HADOOP_RUN_ID}")
    if st.button("Reset Filters", use_container_width=True):
        st.session_state.filter_modes = modes
        st.session_state.filter_status = statuses
        st.session_state.filter_categories = categories
        st.rerun()
    if st.button("Refresh Data", use_container_width=True):
        load_data.clear()
        st.rerun()
    st.multiselect("Shipping mode", modes, key="filter_modes")
    st.multiselect("Delivery status", statuses, key="filter_status")
    st.multiselect("Product category", categories, key="filter_categories")
    with st.expander("Data Sources and Diagnostics"):
        st.write(f"Dataset: `{DATA_PATH.resolve()}`")
        st.write(f"Hadoop bundle: `{HADOOP_DIR.resolve()}`")
        st.write(f"Model artifacts: `{(OUTPUT_DIR / 'model').resolve()}`")
        st.write(f"Rows loaded: {len(df):,}; categories: {len(categories)}; shipping modes: {len(modes)}")

if not st.session_state.filter_modes or not st.session_state.filter_status or not st.session_state.filter_categories:
    st.info("Select at least one value in each sidebar filter to view local analytics.")
    st.stop()
filtered = df[df.shipping_mode.isin(st.session_state.filter_modes) & df.delivery_status.isin(st.session_state.filter_status) & df.category.isin(st.session_state.filter_categories)].copy()
orders = filtered.drop_duplicates("order_id")
order_count = orders.order_id.nunique()
late_orders = int(orders.is_late.sum())
active = int(orders.delivery_group.isin(["late", "on_time"]).sum())
late_rate = pct(late_orders, order_count)
active_late_rate = pct(late_orders, active)
order_values = filtered.groupby("order_id").item_total.sum()
total_value = float(order_values.sum())
avg_days = float(orders.actual_days.mean()) if len(orders) else 0

tabs = st.tabs(["Overview", "Analysis", "Model", "Verification"])

with tabs[0]:
    if filtered.empty:
        st.info("No rows match the current filters.")
    else:
        cols = st.columns(5)
        for col, label, value in zip(cols, ["Orders", "Late delivery rate", "Late rate excluding cancellations", "Order value proxy", "Average actual shipping days"], [f"{order_count:,}", f"{late_rate:.1f}%", f"{active_late_rate:.1f}%", f"${total_value:,.0f}", f"{avg_days:.2f} days"]):
            col.metric(label, value)
        status_counts = orders.delivery_status.value_counts().rename_axis("delivery_status").reset_index(name="orders")
        mode = orders.groupby("shipping_mode").agg(orders=("order_id","size"), late_orders=("is_late","sum")).reset_index()
        mode["late_rate_pct"] = mode.apply(lambda r: pct(r.late_orders, r.orders), axis=1)
        left,right=st.columns(2)
        with left: chart(status_counts,"delivery_status","orders","Delivery status distribution")
        with right: chart(mode.sort_values("late_rate_pct",ascending=False),"shipping_mode","late_rate_pct","Late delivery rate by shipping mode",percent=True)
        if not mode.empty:
            worst=mode.sort_values("late_rate_pct",ascending=False).iloc[0]
            top_status=status_counts.sort_values("orders",ascending=False).iloc[0]
            st.subheader("Key findings")
            st.markdown(f"- **{worst.shipping_mode}** has the highest observed late rate at **{worst.late_rate_pct:.1f}%** in this selection.\n- **{top_status.delivery_status}** is the most common delivery status ({int(top_status.orders):,} distinct orders).\n- {order_count:,} distinct orders across {len(filtered):,} selected order-item rows.")
        st.markdown('<div class="note">No shipping-cost field is available. Order value is the sum of Order Item Total and is shown only as a monetary proxy.</div>',unsafe_allow_html=True)
        safe = [c for c in ["shipping_mode","delivery_status","category","sales","item_total","actual_days","scheduled_days","quantity","market","order_region","customer_segment"] if c in filtered]
        download(filtered[safe],"Download Filtered CSV","filtered_order_items.csv")

with tabs[1]:
    analysis_view = st.radio("Analysis workspace", ["Shipping Analysis", "Category Analysis"], horizontal=True, label_visibility="collapsed")
    if filtered.empty:
        st.info("No rows match the current filters.")
    elif analysis_view == "Shipping Analysis":
        mode = filtered.groupby("shipping_mode").agg(order_items=("order_id","size"), orders=("order_id","nunique"), actual_days=("actual_days","mean"), scheduled_days=("scheduled_days","mean"), order_value_proxy=("item_total","sum")).reset_index()
        ords = filtered.drop_duplicates("order_id").groupby("shipping_mode").agg(late_orders=("is_late","sum"), active_orders=("delivery_group",lambda x:int(x.isin(["late","on_time"]).sum()))).reset_index()
        mode=mode.merge(ords,on="shipping_mode")
        mode["late_rate_pct"] = mode.apply(lambda r:pct(r.late_orders,r.orders),axis=1)
        mode["avg_order_value_proxy"] = mode.order_value_proxy/mode.orders
        a,b=st.columns(2)
        with a: chart(mode,"shipping_mode","actual_days","Actual shipping days",color=None)
        with b: chart(mode,"shipping_mode","scheduled_days","Scheduled shipping days",color=None)
        a,b=st.columns(2)
        with a: chart(mode.sort_values("late_rate_pct",ascending=False),"shipping_mode","late_rate_pct","Late delivery rate",percent=True)
        with b: chart(mode.sort_values("order_value_proxy",ascending=False),"shipping_mode","order_value_proxy","Order value proxy by shipping mode")
        st.dataframe(mode[["shipping_mode","orders","order_items","late_rate_pct","actual_days","scheduled_days","order_value_proxy","avg_order_value_proxy"]].rename(columns={"shipping_mode":"Shipping mode","orders":"Orders","order_items":"Order items","late_rate_pct":"Late rate (%)","actual_days":"Actual days","scheduled_days":"Scheduled days","order_value_proxy":"Order value proxy","avg_order_value_proxy":"Proxy per order"}).style.format({"Late rate (%)":"{:.1f}%","Actual days":"{:.2f}","Scheduled days":"{:.2f}","Order value proxy":"${:,.0f}","Proxy per order":"${:,.2f}"}),width="stretch",hide_index=True)
        st.markdown('<div class="note">The dataset has no shipping-cost column; monetary values use Order Item Total as a proxy.</div>',unsafe_allow_html=True)
    else:
        cat=filtered.groupby("category").agg(order_items=("order_id","size"),orders=("order_id","nunique"),late_items=("is_late","sum"),total_sales=("sales","sum"),avg_item_value=("item_total","mean")).reset_index()
        cat["late_rate_pct"]=cat.apply(lambda r:pct(r.late_items,r.order_items),axis=1)
        limit=st.select_slider("Categories shown",options=[10,15,25,50],value=15)
        ranked=cat.sort_values(["orders","category"],ascending=[False,True]).head(limit)
        a,b=st.columns(2)
        with a: chart(ranked.sort_values("orders"),"category","orders","Top categories by distinct orders",horizontal=True)
        with b: chart(ranked.sort_values("late_rate_pct"),"category","late_rate_pct","Item late rate by category",horizontal=True,percent=True)
        st.dataframe(ranked.rename(columns={"category":"Category","orders":"Orders","order_items":"Order items","late_items":"Late items","late_rate_pct":"Item late rate (%)","total_sales":"Sales","avg_item_value":"Mean item value"}).style.format({"Item late rate (%)":"{:.1f}%","Sales":"${:,.0f}","Mean item value":"${:,.2f}"}),width="stretch",hide_index=True)
        st.caption("Late rate is item-level because a single order may contain several categories.")

with tabs[2]:
    metrics_path=OUTPUT_DIR/"model"/"metrics.json"
    pred_path=OUTPUT_DIR/"model"/"predictions_sample.csv"
    if not metrics_path.exists():
        st.info("Saved model metrics are unavailable. This dashboard does not retrain the model.")
    else:
        try: metrics=json.loads(metrics_path.read_text(encoding="utf-8"))
        except Exception as exc: st.error(f"Could not read model metadata: {exc}"); metrics={}
        if metrics.get("trained"):
            a,b,c=st.columns(3)
            a.metric("Accuracy",f"{metrics['accuracy']:.4f}"); b.metric("F1",f"{metrics['f1']:.4f}"); c.metric("AUC",f"{metrics['auc']:.4f}")
            st.write("Decision tree trained in the notebook on order-level features; these saved evaluation metrics are unchanged.")
            st.info("Interactive prediction is unavailable: the saved artifact requires fitted Spark StringIndexer mappings and order-level aggregated features. Using raw sidebar fields would not reproduce the trained feature schema safely.")
            if pred_path.exists():
                with st.expander("Saved sample predictions"):
                    st.dataframe(pd.read_csv(pred_path),width="stretch",hide_index=True)
            with st.expander("Model metadata and feature design"):
                st.write("Features: " + ", ".join(metrics.get("features",[])))
                st.write(f"Training rows: {metrics.get('train_rows',0):,}; test rows: {metrics.get('test_rows',0):,}; trained at {metrics.get('trained_at_utc','unknown')}.")
                st.write("Excluded to prevent leakage: " + ", ".join(metrics.get("excluded_for_leakage",[])))
                st.write(f"Model artifact: `{metrics.get('model_path','not recorded')}`")

with tabs[3]:
    st.markdown('<div class="note"><b>Hadoop outputs are distributed batch results and are not affected by the interactive sidebar filters.</b></div>',unsafe_allow_html=True)
    expected={"delivery-mode.tsv":11,"shipping-performance.tsv":4,"category-demand.tsv":50}
    job_info={"Delivery Status Job 1":("delivery-mode.tsv",["shipping_mode","status","distinct_orders"],"job_1792688515026_0001",2,1),"Shipping Performance Job 2":("shipping-performance.tsv",["shipping_mode","records","distinct_orders","late_records","on_time_records","canceled_records","other_records","avg_actual_days","avg_scheduled_days","avg_delay_days","total_sales","avg_sales_per_order"],"job_1792688515026_0004",2,1),"Category Demand Job 3":("category-demand.tsv",["category","records","distinct_orders","total_quantity","units_per_order","total_sales","avg_sales_per_item","late_records","late_rate_pct"],"job_1792688515026_0005",2,1)}
    with st.expander("Verification method and source"):
        st.write(f"Pinned verified bundle: `{HADOOP_DIR.resolve()}`. Expected-versus-actual comparisons use the unfiltered local dataset and the existing job definitions.")
        st.write("Job IDs and task counts are from the project verification record. Output row counts and comparisons are read from the pinned bundle at runtime.")
    if not HADOOP_DIR.is_dir():
        st.error("Pinned verified Hadoop bundle is missing; no job status is reported.")
    else:
        job_name=st.radio("Hadoop job",list(job_info),horizontal=True)
        filename,cols,job_id,mappers,reducers=job_info[job_name]
        path=HADOOP_DIR/filename
        if not path.exists():
            st.error(f"{job_name}: output file is missing; verification unavailable.")
        else:
            try:
                result=pd.read_csv(path,sep="\t",header=None,names=cols,encoding="latin-1")
                row_ok=len(result)==expected[filename]
                st.subheader(job_name)
                a,b,c,d=st.columns(4)
                a.metric("Output rows",f"{len(result)} / {expected[filename]}")
                b.metric("YARN job ID",job_id)
                c.metric("Map tasks",str(mappers))
                d.metric("Reduce tasks",str(reducers))
                if filename=="delivery-mode.tsv":
                    base=df.drop_duplicates("order_id")
                    local=base.assign(status=base.delivery_group).groupby(["shipping_mode","status"]).order_id.nunique().reset_index(name="actual_orders")
                    check=result.merge(local,left_on=["shipping_mode","status"],right_on=["shipping_mode","status"],how="outer").fillna(0)
                    check["match"] = check.distinct_orders.astype(int)==check.actual_orders.astype(int)
                    chart(result,"status","distinct_orders","Distinct orders by delivery status",color="shipping_mode")
                    shown=result
                elif filename=="shipping-performance.tsv":
                    local=df.groupby("shipping_mode").agg(records=("order_id","size"),distinct_orders=("order_id","nunique"),total_sales=("sales","sum")).reset_index()
                    check=result[["shipping_mode","records","distinct_orders","total_sales"]].merge(local,on="shipping_mode",suffixes=("_hadoop","_local"),how="outer")
                    check["match"]=(check.records_hadoop==check.records_local)&(check.distinct_orders_hadoop==check.distinct_orders_local)&((check.total_sales_hadoop-check.total_sales_local).abs()<0.02)
                    chart(result,"shipping_mode","avg_actual_days","Average actual shipping days")
                    shown=result
                else:
                    local=df.groupby("category").agg(records=("order_id","size"),distinct_orders=("order_id","nunique"),total_quantity=("quantity","sum"),late_records=("is_late","sum"),total_sales=("sales","sum")).reset_index()
                    fields=["records","distinct_orders","total_quantity","late_records","total_sales"]
                    check=result[["category"]+fields].merge(local,on="category",suffixes=("_hadoop","_local"),how="outer")
                    check["match"]=True
                    for field in fields: check["match"] &= (check[field+"_hadoop"]-check[field+"_local"]).abs()<0.02
                    chart(result.sort_values("total_quantity",ascending=False).head(15),"category","total_quantity","Units by category",horizontal=True)
                    shown=result.sort_values("records",ascending=False)
                verified=row_ok and len(check)>0 and bool(check.match.fillna(False).all())
                st.success("PASS · output count and local comparison match") if verified else st.error("CHECK REQUIRED · output count or local comparison differs")
                st.dataframe(shown,width="stretch",hide_index=True)
                with st.expander("Local-versus-distributed comparison"):
                    st.dataframe(check,width="stretch",hide_index=True)
                download(result,"Download verified job result CSV",f"{Path(filename).stem}_verified.csv")
                st.download_button("Download verified Hadoop TSV",path.read_bytes(),filename,"text/tab-separated-values",key="tsv_"+filename)
            except Exception as exc:
                st.error("Could not parse or verify this Hadoop output.")
                st.exception(exc)
