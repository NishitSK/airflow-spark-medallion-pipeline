import plotly.express as px

def plot_age_distribution(df):
    fig = px.histogram(df, x="age", nbins=20, title="Age Distribution")
    fig.update_layout(
        template="plotly_dark",
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#e0e0e0")
    )
    return fig

def plot_gold_trends(df):
    fig = px.line(df, x="processed_date", y="average_age", 
                   title="Average Age Trend", markers=True)
    fig.update_layout(
        template="plotly_dark",
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#e0e0e0")
    )
    return fig

def plot_dq_violations(latest):
    error_counts = {
        "Null IDs": latest['null_ids'],
        "Neg. Ages": latest['negative_ages'],
        "Inv. Ages": latest['invalid_ages'],
        "Duplicates": latest['duplicate_ids']
    }
    fig = px.pie(names=list(error_counts.keys()), values=list(error_counts.values()), 
                  hole=.4, title="Violation Distribution")
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#e0e0e0")
    )
    return fig

def plot_data_funnel(bronze_count, silver_count, gold_count):
    data = dict(
        number=[bronze_count, silver_count, gold_count],
        stage=["Bronze (Raw)", "Silver (Cleaned)", "Gold (Aggregated)"]
    )
    fig = px.funnel(data, x='number', y='stage', title="Data Medallion Funnel")
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#e0e0e0")
    )
    return fig

def plot_pipeline_history(history_df):
    history_df = history_df[history_df["Cleaned Rows Written"] > 0].copy()
    if history_df.empty:
        return None
    fig = px.area(history_df, x="Timestamp", y="Cleaned Rows Written", title="Data Volume Over Time", markers=True)
    fig.update_layout(
        template="plotly_dark",
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#e0e0e0")
    )
    return fig
