# 📊 RavenStack — Customer Retention Analysis Dashboard

An interactive data analytics dashboard built with **Streamlit**, **Plotly**, and **DuckDB** to analyze customer churn patterns, retention trends, and lifetime value for a subscription-based SaaS business.

---

## 🔗 Live Demo

👉 [View the Live Dashboard](https://mojalefa-04-fut-ravenstack-dashboardravenstack-dashboard-q4thtm.streamlit.app/)


---

## 📌 Project Overview

Customer churn is one of the biggest challenges for subscription businesses. This project performs a full end-to-end retention analysis across 5 datasets — identifying **why customers leave**, **when they leave**, and **what can be done to keep them**.

### Key Questions Answered
- What are the top reasons customers churn?
- Which plan tiers, countries, and referral sources have the highest churn rates?
- How does retention vary across monthly signup cohorts?
- What is the revenue impact of churn?
- How do feature usage and support tickets correlate with churn?

---

## 📂 Project Structure

```
ravenstack-dashboard/
├── ravenstack_dashboard.py       # Main Streamlit app
├── requirements.txt              # Python dependencies
├── .gitignore                    # Files excluded from Git
├── README.md                     # Project documentation
└── data/
    ├── ravenstack_accounts.csv
    ├── ravenstack_churn_events.csv
    ├── ravenstack_feature_usage.csv
    ├── ravenstack_subscriptions.csv
    └── ravenstack_support_tickets.csv
```

---

## 📁 Dataset Description

| File | Records | Description |
|------|---------|-------------|
| `ravenstack_accounts.csv` | 500 | Customer accounts with plan, country, signup date, churn flag |
| `ravenstack_churn_events.csv` | 600 | Churn events with reason codes, refund amounts, feedback |
| `ravenstack_feature_usage.csv` | 25,000 | Feature-level usage logs per subscription |
| `ravenstack_subscriptions.csv` | 5,000 | Subscription records with MRR, ARR, billing frequency |
| `ravenstack_support_tickets.csv` | 2,000 | Support tickets with resolution time, satisfaction scores |

---

## 🧠 Analysis Workflow

The project is broken into **5 learning steps**:

| Step | Focus | Technique |
|------|-------|-----------|
| **Step 1** | Data Loading & Cleaning | Pandas, date parsing, null handling |
| **Step 2** | SQL Aggregations | DuckDB on DataFrames |
| **Step 3** | Cohort Analysis | Monthly retention curves |
| **Step 4** | Feature & Support Signals | Behavioral churn correlation |
| **Step 5** | Streamlit Dashboard | Interactive filters and Plotly charts |

---

## 📊 Dashboard Features

- **5 interactive tabs** — Churn Analysis, Cohort Retention, Feature & Support, Revenue Impact, Recommendations
- **Sidebar filters** — Filter by Plan Tier, Country, and Referral Source
- **17 Plotly charts** — Bar charts, area charts, heatmaps, histograms, pie charts
- **KPI cards** — Total accounts, churn rate, active MRR, average lifetime
- **SQL-powered analytics** — DuckDB queries running directly on pandas DataFrames
- **Actionable recommendations** — Priority-ranked retention strategies

---

## 🔑 Key Findings

| Insight | Value |
|---------|-------|
| Overall churn rate | **22%** (target: <15%) |
| #1 churn reason | **Feature gaps (19%)** |
| Best retention channel | **Partner referrals (85.4% retention)** |
| Highest churn country | **Germany (32%)** |
| Avg lifetime — active customers | **303 days** |
| Avg lifetime — churned customers | **105 days** |
| Lost MRR from churn | **$2.35M** |

---

## ✅ Top Recommendations

1. **Fix feature gaps** — the #1 driver of churn; run user research with at-risk accounts
2. **Deploy churn risk scoring** — catch at-risk customers 30 days before they cancel
3. **Invest in partner channel** — lowest churn source at 14.6%, vs 30.2% for events
4. **DACH success program** — Germany churns at 32%, nearly 1.5× the global average
5. **Post-escalation outreach** — churned customers had 22% more support escalations

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| **Python 3.13** | Core language |
| **Streamlit** | Interactive web dashboard |
| **Plotly** | Data visualizations |
| **DuckDB** | In-memory SQL on DataFrames |
| **Pandas** | Data wrangling and analysis |
| **NumPy** | Numerical operations |

---

## ⚙️ Installation & Running Locally

### 1. Clone the repository
```bash
git clone https://github.com/your-username/ravenstack-dashboard.git
cd ravenstack-dashboard
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the dashboard
```bash
streamlit run ravenstack_dashboard.py
```

Then open your browser at `http://localhost:8501`

---

## 🚀 Deployment

This app is deployed on **Streamlit Community Cloud**.

To deploy your own copy:
1. Fork this repository
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect your GitHub account
4. Select this repo and set `ravenstack_dashboard.py` as the main file
5. Click **Deploy**

---

## 📚 Skills Demonstrated

- Exploratory data analysis (EDA) across multiple related tables
- Retention and cohort analysis
- Customer lifetime value (CLV) metrics
- SQL querying with DuckDB on in-memory DataFrames
- Interactive dashboard development with Streamlit
- Data visualization with Plotly
- Insight-driven storytelling and business recommendations

---

## 👤 Author

**Your Name**
- GitHub: [@Mojalefa-04](https://github.com/Mojalefa-04)
- LinkedIn: [Mojalefa Mokhathi](https://www.linkedin.com/in/mojalefa-mokhathi-81540224a)

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
