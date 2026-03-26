"""
RavenStack Retention Analysis Dashboard
========================================
Run with: streamlit run ravenstack_dashboard.py

Prerequisites:
    pip install streamlit plotly pandas duckdb openpyxl

"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
import warnings
warnings.filterwarnings('ignore')

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
DATA_DIR = "/Users/promojz/Documents/FutureInterns/FUTURE_DS_02/Dataset"   # ← change this if your CSVs are in a different folder

st.set_page_config(
    page_title="RavenStack · Retention Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
# STEP 1 — DATA LOADING & CLEANING
# ─────────────────────────────────────────────
@st.cache_data
def load_data():
    """
    STEP 1 LEARNING: We load and clean 5 related tables.
    We use @st.cache_data so Streamlit only reads files once.
    """
    accounts = pd.read_csv(f"{DATA_DIR}/ravenstack_accounts.csv", parse_dates=["signup_date"])
    churn_events = pd.read_csv(f"{DATA_DIR}/ravenstack_churn_events.csv", parse_dates=["churn_date"])
    feature_usage = pd.read_csv(f"{DATA_DIR}/ravenstack_feature_usage.csv", parse_dates=["usage_date"])
    subscriptions = pd.read_csv(
        f"{DATA_DIR}/ravenstack_subscriptions.csv", parse_dates=["start_date", "end_date"]
    )
    support_tickets = pd.read_csv(
        f"{DATA_DIR}/ravenstack_support_tickets.csv", parse_dates=["submitted_at", "closed_at"]
    )

    # Clean: fill missing end dates with a reference date for active subs
    REFERENCE_DATE = pd.Timestamp("2025-01-01")
    subscriptions["end_date_filled"] = subscriptions["end_date"].fillna(REFERENCE_DATE)
    subscriptions["duration_days"] = (
        subscriptions["end_date_filled"] - subscriptions["start_date"]
    ).dt.days.clip(lower=0)

    # Derive cohort month from signup (keep as timestamp for DuckDB compatibility)
    accounts["cohort_ts"] = accounts["signup_date"].dt.to_period("M").dt.to_timestamp()
    accounts["cohort"] = accounts["signup_date"].dt.to_period("M")  # used only in cohort_analysis()

    return accounts, churn_events, feature_usage, subscriptions, support_tickets


# ─────────────────────────────────────────────
# STEP 2 — SQL-POWERED AGGREGATIONS (DuckDB)
# ─────────────────────────────────────────────
@st.cache_data
def run_sql_queries(accounts, churn_events, feature_usage, subscriptions, support_tickets):
    """
    STEP 2 LEARNING: DuckDB lets you write SQL directly on pandas DataFrames.
    This is extremely useful for ad-hoc analysis without a database server.
    """
    con = duckdb.connect()
    # DuckDB cannot handle pandas Period dtype.
    # Detect Period columns by their .freq attribute and drop them before registering.
    period_cols = [c for c in accounts.columns if hasattr(accounts[c].dtype, "freq")]
    accounts_db = accounts.drop(columns=period_cols)
    con.register("accounts", accounts_db)
    con.register("churn_events", churn_events)
    con.register("feature_usage", feature_usage)
    con.register("subscriptions", subscriptions)
    con.register("support_tickets", support_tickets)

    # Churn reason distribution
    churn_reasons = con.execute("""
        SELECT reason_code, COUNT(*) AS count,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
        FROM churn_events
        GROUP BY reason_code
        ORDER BY count DESC
    """).df()

    # Churn by plan tier
    plan_churn = con.execute("""
        SELECT plan_tier,
               SUM(churn_flag::INT) AS churned,
               COUNT(*) AS total,
               ROUND(AVG(churn_flag::FLOAT) * 100, 1) AS churn_rate
        FROM accounts
        GROUP BY plan_tier
        ORDER BY churn_rate DESC
    """).df()

    # Monthly churn trend
    monthly_churn = con.execute("""
        SELECT DATE_TRUNC('month', churn_date) AS month,
               COUNT(*) AS churn_count
        FROM churn_events
        GROUP BY month
        ORDER BY month
    """).df()

    # Country churn
    country_churn = con.execute("""
        SELECT country,
               SUM(churn_flag::INT) AS churned,
               COUNT(*) AS total,
               ROUND(AVG(churn_flag::FLOAT) * 100, 1) AS churn_rate
        FROM accounts
        GROUP BY country
        ORDER BY churn_rate DESC
    """).df()

    # Referral source churn
    referral_churn = con.execute("""
        SELECT referral_source,
               SUM(churn_flag::INT) AS churned,
               COUNT(*) AS total,
               ROUND(AVG(churn_flag::FLOAT) * 100, 1) AS churn_rate
        FROM accounts
        GROUP BY referral_source
        ORDER BY churn_rate DESC
    """).df()

    # Revenue by churn status
    revenue_churn = con.execute("""
        SELECT a.churn_flag,
               ROUND(AVG(s.mrr_amount), 0) AS avg_mrr,
               ROUND(SUM(s.mrr_amount), 0) AS total_mrr,
               ROUND(AVG(s.duration_days), 0) AS avg_lifetime_days
        FROM accounts a
        JOIN subscriptions s ON a.account_id = s.account_id
        GROUP BY a.churn_flag
    """).df()

    # Feature usage vs churn
    feature_vs_churn = con.execute("""
        SELECT s.churn_flag,
               ROUND(AVG(fu_agg.total_usage), 1) AS avg_usage,
               ROUND(AVG(fu_agg.unique_features), 1) AS avg_features,
               ROUND(AVG(fu_agg.errors), 1) AS avg_errors
        FROM subscriptions s
        LEFT JOIN (
            SELECT subscription_id,
                   SUM(usage_count) AS total_usage,
                   COUNT(DISTINCT feature_name) AS unique_features,
                   SUM(error_count) AS errors
            FROM feature_usage
            GROUP BY subscription_id
        ) fu_agg ON s.subscription_id = fu_agg.subscription_id
        GROUP BY s.churn_flag
    """).df()

    # Support ticket impact
    support_vs_churn = con.execute("""
        SELECT a.churn_flag,
               ROUND(AVG(t_agg.ticket_count), 2) AS avg_tickets,
               ROUND(AVG(t_agg.avg_sat), 2) AS avg_satisfaction,
               ROUND(AVG(t_agg.escalations), 3) AS avg_escalations
        FROM accounts a
        LEFT JOIN (
            SELECT account_id,
                   COUNT(*) AS ticket_count,
                   AVG(satisfaction_score) AS avg_sat,
                   SUM(escalation_flag::INT) AS escalations
            FROM support_tickets
            GROUP BY account_id
        ) t_agg ON a.account_id = t_agg.account_id
        GROUP BY a.churn_flag
    """).df()

    # Days to churn distribution bins
    days_to_churn = con.execute("""
        SELECT DATEDIFF('day', a.signup_date, ce.churn_date) AS days_to_churn
        FROM churn_events ce
        JOIN accounts a ON ce.account_id = a.account_id
        WHERE DATEDIFF('day', a.signup_date, ce.churn_date) >= 0
    """).df()

    # Churn reasons by plan tier
    reasons_by_plan = con.execute("""
        SELECT a.plan_tier, ce.reason_code, COUNT(*) AS count
        FROM churn_events ce
        JOIN accounts a ON ce.account_id = a.account_id
        GROUP BY a.plan_tier, ce.reason_code
        ORDER BY a.plan_tier, count DESC
    """).df()

    con.close()
    return (churn_reasons, plan_churn, monthly_churn, country_churn,
            referral_churn, revenue_churn, feature_vs_churn, support_vs_churn,
            days_to_churn, reasons_by_plan)



# ─────────────────────────────────────────────
# STEP 3 — COHORT ANALYSIS
# ─────────────────────────────────────────────
@st.cache_data
def cohort_analysis(accounts, churn_events):
    """
    STEP 3 LEARNING: Cohort analysis groups customers by signup month
    and tracks what % remained (or churned) over time.
    This reveals whether retention is improving or worsening.
    """
    cohort_size = accounts.groupby("cohort").size().rename("cohort_size")
    churn_events["cohort"] = churn_events["churn_date"].dt.to_period("M")
    churned_by_cohort = (
        churn_events.merge(accounts[["account_id", "cohort"]], on="account_id", how="left")
        .groupby("cohort_x")  # cohort of signup, not churn
        .size()
    )
    # Use signup cohort properly
    merged = accounts.merge(
        churn_events[["account_id", "churn_date"]],
        on="account_id", how="left"
    )
    churned_by_cohort = (
        merged[merged["churn_flag"] == True]
        .groupby("cohort")
        .size()
        .rename("churned")
    )
    cohort_df = pd.DataFrame({
        "cohort_size": cohort_size,
        "churned": churned_by_cohort
    }).fillna(0).reset_index()
    cohort_df["retention_rate"] = (
        (1 - cohort_df["churned"] / cohort_df["cohort_size"]) * 100
    ).round(1)
    cohort_df["churn_rate"] = (
        cohort_df["churned"] / cohort_df["cohort_size"] * 100
    ).round(1)
    cohort_df["cohort_str"] = cohort_df["cohort"].astype(str)
    return cohort_df


# ─────────────────────────────────────────────
# CALL ALL FUNCTIONS (after definitions)
# ─────────────────────────────────────────────
with st.spinner("Loading data..."):
    accounts, churn_events, feature_usage, subscriptions, support_tickets = load_data()

with st.spinner("Running analysis..."):
    (churn_reasons, plan_churn, monthly_churn, country_churn,
     referral_churn, revenue_churn, feature_vs_churn, support_vs_churn,
     days_to_churn, reasons_by_plan) = run_sql_queries(
        accounts, churn_events, feature_usage, subscriptions, support_tickets
    )
    cohort_df = cohort_analysis(accounts, churn_events)


# ─────────────────────────────────────────────
# SIDEBAR FILTERS
# ─────────────────────────────────────────────
st.sidebar.markdown("## 🐦 RavenStack")
st.sidebar.title("🔍 Filters")

plan_filter = st.sidebar.multiselect(
    "Plan Tier",
    options=accounts["plan_tier"].unique().tolist(),
    default=accounts["plan_tier"].unique().tolist(),
)
country_filter = st.sidebar.multiselect(
    "Country",
    options=accounts["country"].unique().tolist(),
    default=accounts["country"].unique().tolist(),
)
referral_filter = st.sidebar.multiselect(
    "Referral Source",
    options=accounts["referral_source"].unique().tolist(),
    default=accounts["referral_source"].unique().tolist(),
)

# Apply filters
mask = (
    accounts["plan_tier"].isin(plan_filter) &
    accounts["country"].isin(country_filter) &
    accounts["referral_source"].isin(referral_filter)
)
filtered_accounts = accounts[mask]
filtered_ids = set(filtered_accounts["account_id"])
filtered_churn = churn_events[churn_events["account_id"].isin(filtered_ids)]

# ─────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────
st.title("📊 RavenStack — Customer Retention Dashboard")
st.caption("Churn patterns · Retention trends · Lifetime value · Actionable recommendations")
st.divider()

# ─────────────────────────────────────────────
# STEP 4 — KPI CARDS
# ─────────────────────────────────────────────
total = len(filtered_accounts)
churned = int(filtered_accounts["churn_flag"].sum())
churn_rate = churned / total * 100 if total else 0
active = total - churned

rev_df = revenue_churn.copy()
churned_rev = float(rev_df[rev_df["churn_flag"] == True]["total_mrr"].values[0]) if True in rev_df["churn_flag"].values else 0
active_rev = float(rev_df[rev_df["churn_flag"] == False]["total_mrr"].values[0]) if False in rev_df["churn_flag"].values else 0
avg_lifetime_active = float(rev_df[rev_df["churn_flag"] == False]["avg_lifetime_days"].values[0]) if False in rev_df["churn_flag"].values else 0

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total Accounts", f"{total:,}")
col2.metric("Active Accounts", f"{active:,}", delta=f"+{active}")
col3.metric("Churned Accounts", f"{churned:,}", delta=f"-{churned}", delta_color="inverse")
col4.metric("Churn Rate", f"{churn_rate:.1f}%", delta="Target: <15%", delta_color="inverse")
col5.metric("Avg Lifetime (Active)", f"{avg_lifetime_active:.0f} days")

st.divider()

# ─────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🔥 Churn Analysis",
    "📅 Cohort Retention",
    "💡 Feature & Support",
    "💰 Revenue Impact",
    "✅ Recommendations",
])

# ════════════════════════════════════════════
# TAB 1 — CHURN ANALYSIS
# ════════════════════════════════════════════
with tab1:
    st.subheader("Churn Reasons & Patterns")

    c1, c2 = st.columns(2)

    # Churn reasons donut
    with c1:
        fig = px.pie(
            churn_reasons, values="count", names="reason_code",
            hole=0.45,
            title="Churn Reasons — Distribution",
            color_discrete_sequence=px.colors.qualitative.Bold,
        )
        fig.update_traces(textposition="outside", textinfo="percent+label")
        fig.update_layout(showlegend=False, height=380)
        st.plotly_chart(fig, use_container_width=True)

    # Churn by plan
    with c2:
        fig2 = px.bar(
            plan_churn, x="plan_tier", y="churn_rate",
            color="churn_rate",
            color_continuous_scale="Reds",
            title="Churn Rate by Plan Tier (%)",
            text="churn_rate",
            labels={"churn_rate": "Churn Rate (%)"},
        )
        fig2.update_traces(texttemplate="%{text}%", textposition="outside")
        fig2.update_layout(height=380, coloraxis_showscale=False)
        st.plotly_chart(fig2, use_container_width=True)

    # Monthly churn trend
    st.subheader("Monthly Churn Trend")
    fig3 = px.area(
        monthly_churn, x="month", y="churn_count",
        title="Number of Churned Accounts per Month",
        color_discrete_sequence=["#e63946"],
        labels={"churn_count": "Churned Accounts", "month": "Month"},
    )
    fig3.update_layout(height=320)
    st.plotly_chart(fig3, use_container_width=True)

    c3, c4 = st.columns(2)

    # Days to churn histogram
    with c3:
        fig4 = px.histogram(
            days_to_churn, x="days_to_churn",
            nbins=30,
            title="Time to Churn Distribution (Days from Signup)",
            color_discrete_sequence=["#457b9d"],
            labels={"days_to_churn": "Days from Signup to Churn"},
        )
        fig4.add_vline(x=days_to_churn["days_to_churn"].median(), line_dash="dash",
                       line_color="red", annotation_text="Median")
        fig4.update_layout(height=350)
        st.plotly_chart(fig4, use_container_width=True)

    # Churn reasons by plan heatmap
    with c4:
        pivot = reasons_by_plan.pivot(index="plan_tier", columns="reason_code", values="count").fillna(0)
        fig5 = px.imshow(
            pivot,
            title="Churn Reasons by Plan Tier (Heatmap)",
            color_continuous_scale="YlOrRd",
            text_auto=True,
            labels={"color": "Count"},
        )
        fig5.update_layout(height=350)
        st.plotly_chart(fig5, use_container_width=True)

    # Country & referral
    c5, c6 = st.columns(2)
    with c5:
        fig6 = px.bar(
            country_churn, x="churn_rate", y="country",
            orientation="h", title="Churn Rate by Country (%)",
            color="churn_rate", color_continuous_scale="Oranges",
            text="churn_rate",
            labels={"churn_rate": "Churn Rate (%)"},
        )
        fig6.update_traces(texttemplate="%{text}%", textposition="outside")
        fig6.update_layout(height=350, coloraxis_showscale=False, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig6, use_container_width=True)

    with c6:
        fig7 = px.bar(
            referral_churn, x="referral_source", y="churn_rate",
            title="Churn Rate by Referral Source (%)",
            color="churn_rate", color_continuous_scale="Blues",
            text="churn_rate",
            labels={"churn_rate": "Churn Rate (%)"},
        )
        fig7.update_traces(texttemplate="%{text}%", textposition="outside")
        fig7.update_layout(height=350, coloraxis_showscale=False)
        st.plotly_chart(fig7, use_container_width=True)

# ════════════════════════════════════════════
# TAB 2 — COHORT RETENTION
# ════════════════════════════════════════════
with tab2:
    st.subheader("Cohort Retention Analysis")
    st.info(
        "**How to read this:** Each bar represents a monthly signup cohort. "
        "The height shows what % of customers from that month are still active. "
        "A declining trend signals worsening acquisition quality or product issues."
    )

    fig8 = px.bar(
        cohort_df, x="cohort_str", y="retention_rate",
        color="retention_rate",
        color_continuous_scale="RdYlGn",
        title="Monthly Cohort Retention Rate (%)",
        labels={"cohort_str": "Signup Cohort", "retention_rate": "Retention Rate (%)"},
        text="retention_rate",
    )
    fig8.update_traces(texttemplate="%{text}%", textposition="outside")
    fig8.add_hline(y=80, line_dash="dash", line_color="green", annotation_text="Target 80%")
    fig8.update_layout(height=420, xaxis_tickangle=-45, coloraxis_showscale=False)
    st.plotly_chart(fig8, use_container_width=True)

    c7, c8 = st.columns(2)

    with c7:
        fig9 = px.line(
            cohort_df.tail(18), x="cohort_str", y=["retention_rate", "churn_rate"],
            title="Retention vs Churn Rate Trend (Last 18 Months)",
            markers=True,
            labels={"value": "Rate (%)", "cohort_str": "Cohort", "variable": "Metric"},
            color_discrete_map={"retention_rate": "#2a9d8f", "churn_rate": "#e63946"},
        )
        fig9.update_layout(height=350, xaxis_tickangle=-45)
        st.plotly_chart(fig9, use_container_width=True)

    with c8:
        fig10 = px.bar(
            cohort_df, x="cohort_str", y=["cohort_size", "churned"],
            barmode="overlay",
            title="Cohort Size vs Churned Accounts",
            labels={"value": "Accounts", "cohort_str": "Cohort"},
            color_discrete_map={"cohort_size": "#a8dadc", "churned": "#e63946"},
            opacity=0.85,
        )
        fig10.update_layout(height=350, xaxis_tickangle=-45)
        st.plotly_chart(fig10, use_container_width=True)

    st.subheader("📋 Cohort Detail Table")
    display_cohort = cohort_df.copy()
    display_cohort["cohort"] = display_cohort["cohort"].astype(str)
    display_cohort = display_cohort.rename(columns={
        "cohort": "Cohort Month", "cohort_size": "New Accounts",
        "churned": "Churned", "retention_rate": "Retention %", "churn_rate": "Churn %"
    })
    st.dataframe(
        display_cohort[["Cohort Month", "New Accounts", "Churned", "Retention %", "Churn %"]],
        use_container_width=True,
        hide_index=True,
    )

# ════════════════════════════════════════════
# TAB 3 — FEATURE & SUPPORT
# ════════════════════════════════════════════
with tab3:
    st.subheader("Feature Usage & Support Signals")

    # Feature usage comparison
    st.markdown("#### Feature Engagement: Churned vs Active Customers")
    feat_melt = feature_vs_churn.copy()
    feat_melt["Status"] = feat_melt["churn_flag"].map({True: "Churned", False: "Active"})
    metrics_cols = ["avg_usage", "avg_features", "avg_errors"]
    fig11 = go.Figure()
    colors = {"Active": "#2a9d8f", "Churned": "#e63946"}
    for _, row in feat_melt.iterrows():
        status = row["Status"]
        fig11.add_trace(go.Bar(
            name=status,
            x=["Avg Total Usage", "Avg Unique Features", "Avg Errors"],
            y=[row["avg_usage"], row["avg_features"], row["avg_errors"]],
            marker_color=colors[status],
        ))
    fig11.update_layout(barmode="group", title="Feature Metrics: Active vs Churned",
                        height=380, legend_title="Status")
    st.plotly_chart(fig11, use_container_width=True)

    c9, c10 = st.columns(2)

    # Support ticket metrics
    with c9:
        supp_melt = support_vs_churn.copy()
        supp_melt["Status"] = supp_melt["churn_flag"].map({True: "Churned", False: "Active"})
        fig12 = go.Figure()
        for _, row in supp_melt.iterrows():
            status = row["Status"]
            fig12.add_trace(go.Bar(
                name=status,
                x=["Avg Tickets", "Avg Satisfaction", "Avg Escalations"],
                y=[row["avg_tickets"], row["avg_satisfaction"], row["avg_escalations"]],
                marker_color=colors[status],
            ))
        fig12.update_layout(barmode="group", title="Support Metrics: Active vs Churned",
                            height=360, legend_title="Status")
        st.plotly_chart(fig12, use_container_width=True)

    # Top features used by churned customers
    with c10:
        feat_churn_agg = (
            feature_usage
            .merge(subscriptions[["subscription_id", "account_id", "churn_flag"]], on="subscription_id")
            [lambda df: df["churn_flag"] == True]
            .groupby("feature_name")["usage_count"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
            .reset_index()
        )
        fig13 = px.bar(
            feat_churn_agg, x="usage_count", y="feature_name",
            orientation="h",
            title="Top 10 Features Used Before Churning",
            color="usage_count", color_continuous_scale="Purples",
            labels={"usage_count": "Total Usage", "feature_name": "Feature"},
        )
        fig13.update_layout(height=360, coloraxis_showscale=False,
                            yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig13, use_container_width=True)

    # Support satisfaction distribution
    st.markdown("#### Support Satisfaction Score Distribution")
    sat_scores = support_tickets.merge(
        accounts[["account_id", "churn_flag"]], on="account_id"
    ).dropna(subset=["satisfaction_score"])
    sat_scores["Status"] = sat_scores["churn_flag"].map({True: "Churned", False: "Active"})
    fig14 = px.histogram(
        sat_scores, x="satisfaction_score", color="Status",
        barmode="overlay", opacity=0.7,
        title="Satisfaction Score Distribution: Active vs Churned",
        color_discrete_map={"Active": "#2a9d8f", "Churned": "#e63946"},
        labels={"satisfaction_score": "Satisfaction Score (1–5)"},
        nbins=10,
    )
    fig14.update_layout(height=320)
    st.plotly_chart(fig14, use_container_width=True)

# ════════════════════════════════════════════
# TAB 4 — REVENUE IMPACT
# ════════════════════════════════════════════
with tab4:
    st.subheader("Revenue & Lifetime Value Impact")

    # Revenue KPIs
    rev_active = revenue_churn[revenue_churn["churn_flag"] == False].iloc[0]
    rev_churned = revenue_churn[revenue_churn["churn_flag"] == True].iloc[0]

    rc1, rc2, rc3, rc4 = st.columns(4)
    rc1.metric("Active MRR (Total)", f"${rev_active['total_mrr']:,.0f}")
    rc2.metric("Lost MRR (Churned)", f"${rev_churned['total_mrr']:,.0f}", delta_color="inverse", delta="Lost")
    rc3.metric("Avg Lifetime (Active)", f"{rev_active['avg_lifetime_days']:.0f} days")
    rc4.metric("Avg Lifetime (Churned)", f"{rev_churned['avg_lifetime_days']:.0f} days", delta_color="inverse")

    st.divider()

    c11, c12 = st.columns(2)

    with c11:
        # Revenue split pie
        rev_labels = ["Active MRR", "Lost MRR (Churned)"]
        rev_values = [rev_active["total_mrr"], rev_churned["total_mrr"]]
        fig15 = go.Figure(go.Pie(
            labels=rev_labels, values=rev_values,
            hole=0.45,
            marker_colors=["#2a9d8f", "#e63946"],
        ))
        fig15.update_layout(title="MRR: Active vs Churned Accounts", height=360)
        st.plotly_chart(fig15, use_container_width=True)

    with c12:
        # Lifetime comparison bar
        fig16 = go.Figure()
        fig16.add_trace(go.Bar(
            x=["Active", "Churned"],
            y=[rev_active["avg_lifetime_days"], rev_churned["avg_lifetime_days"]],
            marker_color=["#2a9d8f", "#e63946"],
            text=[f"{rev_active['avg_lifetime_days']:.0f}d", f"{rev_churned['avg_lifetime_days']:.0f}d"],
            textposition="outside",
        ))
        fig16.update_layout(
            title="Avg Customer Lifetime: Active vs Churned (Days)",
            yaxis_title="Days", height=360,
        )
        st.plotly_chart(fig16, use_container_width=True)

    # MRR distribution by plan
    # subscriptions already has plan_tier & churn_flag — merge only account-level churn_flag
    # Use suffixes to avoid silent column collisions
    sub_merged = subscriptions.merge(
        accounts[["account_id", "churn_flag"]].rename(columns={"churn_flag": "acct_churn"}),
        on="account_id",
        how="left",
    )
    plan_mrr = sub_merged.groupby(["plan_tier", "acct_churn"])["mrr_amount"].mean().reset_index()
    plan_mrr["Status"] = plan_mrr["acct_churn"].map({True: "Churned", False: "Active"})
    fig17 = px.bar(
        plan_mrr, x="plan_tier", y="mrr_amount", color="Status",
        barmode="group",
        title="Average MRR by Plan Tier: Active vs Churned",
        color_discrete_map={"Active": "#2a9d8f", "Churned": "#e63946"},
        labels={"mrr_amount": "Avg MRR ($)", "plan_tier": "Plan Tier"},
        text_auto=".0f",
    )
    fig17.update_layout(height=360)
    st.plotly_chart(fig17, use_container_width=True)

# ════════════════════════════════════════════
# TAB 5 — RECOMMENDATIONS
# ════════════════════════════════════════════
with tab5:
    st.subheader("✅ Actionable Retention Recommendations")
    st.caption("Based on churn pattern analysis across all 5 data sources")

    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("""
### 🔴 Critical Issues
---
**1. Feature Gaps are the #1 Churn Driver (19%)**
> Missing or poor product features cause more churn than pricing.
- **Action:** Run user research sessions with accounts citing "features" before cancellation.
- Prioritize feature requests from high-MRR plan tiers.
- Build an in-app feedback loop for feature requests.

---
**2. Churn is Accelerating (Dec 2024: 117 events)**
> Monthly churn nearly doubled from Jan to Dec 2024.
- **Action:** Implement a "churn risk score" model using signals like feature drop-off, support escalations, and low satisfaction.
- Trigger proactive outreach when a customer's engagement score drops.

---
**3. September 2024 Cohort: Only 40% Retention**
> One cohort had particularly poor retention — investigate what changed.
- **Action:** Audit onboarding or product changes made in Aug–Sep 2024.
- Interview customers from that cohort who churned.
""")

    with col_b:
        st.markdown("""
### 🟡 Growth Opportunities
---
**4. Partner Channel = Lowest Churn (14.6%)**
> Partner-referred customers churn 50% less than event-sourced customers.
- **Action:** Increase investment in partner referral programs.
- Create a formal partner certification or co-selling program.

---
**5. Germany Has the Highest Churn Rate (32%)**
> DE customers churn at 1.4× the global average.
- **Action:** Investigate localization gaps — language, billing, compliance (GDPR).
- Assign dedicated CSM coverage for DE enterprise accounts.

---
**6. Support Escalations Correlate with Churn (0.22 vs 0.18)**
> Churned customers have 22% more support escalations.
- **Action:** Create early-warning alerts when tickets are escalated.
- Offer success calls within 48hrs of any escalation.
""")

    st.divider()
    st.markdown("""
### 🟢 Retention Levers Summary

| Priority | Action | Expected Impact |
|----------|--------|-----------------|
| 🔴 High | Fix top churned feature gaps | Reduce features-churn by ~20% |
| 🔴 High | Deploy churn risk scoring | Catch at-risk accounts 30 days early |
| 🟡 Med | Double down on partner channel | Shift CAC to lower-churn source |
| 🟡 Med | DACH-specific success program | Reduce DE churn rate to <20% |
| 🟡 Med | Post-escalation success calls | Reduce support-driven churn by 15% |
| 🟢 Low | Improve trial-to-paid conversion | Reduce early-stage churn |
| 🟢 Low | Annual billing incentives | Improve retention via longer commitment |
""")

    
