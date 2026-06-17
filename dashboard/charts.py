import plotly.express as px
def plot_age_distribution(df):
    if df is None or df.empty or "age" not in df.columns:
        return None
    fig = px.histogram(df, x="age", nbins=20, title="Age Distribution")
    fig.update_layout(
        template="plotly_dark",
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#e0e0e0")
    )
    return fig

def plot_gold_trends(df):
    if df is None or df.empty or "average_age" not in df.columns:
        return None
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

def plot_dq_trends(df):
    """
    Plots Data Quality metrics over time (Quality Score, Null Rate, Duplicate Rate, Failure Rate).
    """
    import plotly.graph_objects as go
    
    if df is None or df.empty:
        return None
        
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['Timestamp'], y=df['Quality Score (%)'], mode='lines+markers', name='Quality Score (%)', line=dict(color='#00CC96', width=2.5)))
    fig.add_trace(go.Scatter(x=df['Timestamp'], y=df['Null Rate (%)'], mode='lines+markers', name='Null Rate (%)', line=dict(color='#636EFA', width=2)))
    fig.add_trace(go.Scatter(x=df['Timestamp'], y=df['Duplicate Rate (%)'], mode='lines+markers', name='Duplicate Rate (%)', line=dict(color='#EF553B', width=2)))
    fig.add_trace(go.Scatter(x=df['Timestamp'], y=df['Failure Rate (%)'], mode='lines+markers', name='Failure Rate (%)', line=dict(color='#AB63FA', width=2, dash='dash')))
    
    fig.update_layout(
        title="Data Quality Trends (Quality Score, Null Rate, Duplicate Rate, Failure Rate)",
        xaxis_title="Run Time",
        yaxis_title="Percentage (%)",
        yaxis=dict(range=[-2, 105]),
        template="plotly_dark",
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#e0e0e0"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig
