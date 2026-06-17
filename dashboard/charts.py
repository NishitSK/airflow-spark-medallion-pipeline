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
    dataset_type = latest.get("dataset_type", "CUSTOMER")
    error_counts = {}
    if dataset_type == "CUSTOMER":
        error_counts = {
            "Null IDs": latest.get('null_ids', 0),
            "Malformed/Invalid Ages": latest.get('invalid_ages', 0) + latest.get('null_ages', 0),
            "Duplicates": latest.get('duplicate_ids', 0)
        }
    elif dataset_type == "ORDERS":
        error_counts = {
            "Null Order IDs": latest.get('null_order_ids', 0),
            "Invalid Quantities": latest.get('invalid_qty', 0),
            "Invalid Prices": latest.get('invalid_price', 0),
            "Duplicate Orders": latest.get('duplicate_ids', 0)
        }
    else:
        dup_rate = latest.get('duplicate_rate', 0.0)
        error_counts = {
            "Unique Rows (%)": 100.0 - dup_rate,
            "Duplicate Rows (%)": dup_rate
        }
        
    title = "Violation Distribution" if dataset_type != "GENERIC" else "Row Uniqueness Distribution"
    fig = px.pie(names=list(error_counts.keys()), values=list(error_counts.values()), 
                  hole=.4, title=title, color_discrete_sequence=px.colors.qualitative.Pastel)
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

def plot_orders_trends(df):
    if df is None or df.empty or "total_orders" not in df.columns:
        return None
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(x=df["processed_date"], y=df["total_orders"], name="Total Orders", marker_color="#636EFA"),
        secondary_y=False,
    )
    
    if "total_revenue" in df.columns:
        fig.add_trace(
            go.Scatter(x=df["processed_date"], y=df["total_revenue"], name="Total Revenue ($)", mode="lines+markers", line=dict(color="#00CC96", width=2.5)),
            secondary_y=True,
        )
        
    fig.update_layout(
        title="Orders & Revenue Trends Over Time",
        xaxis_title="Processed Date",
        template="plotly_dark",
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#e0e0e0")
    )
    fig.update_yaxes(title_text="Total Orders", secondary_y=False)
    fig.update_yaxes(title_text="Total Revenue ($)", secondary_y=True)
    return fig

def plot_product_volume(df):
    if df is None or df.empty or "product_name" not in df.columns or "quantity" not in df.columns:
        return None
    prod_df = df.groupby("product_name")["quantity"].sum().reset_index()
    prod_df = prod_df.sort_values("quantity", ascending=False).head(15)
    fig = px.bar(prod_df, x="product_name", y="quantity", title="Top Products by Quantity Sold",
                 color="quantity", color_continuous_scale="Viridis")
    fig.update_layout(
        template="plotly_dark",
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#e0e0e0")
    )
    return fig

def plot_generic_nulls(column_metrics):
    if not column_metrics:
        return None
    import pandas as pd
    data = []
    for col_name, metrics in column_metrics.items():
        data.append({
            "Column": col_name,
            "Null Percentage (%)": metrics.get("null_percentage", 0.0),
            "Null Count": metrics.get("null_count", 0),
            "Distinct Count": metrics.get("distinct_count", 0),
            "Datatype": metrics.get("datatype", "string")
        })
    df_metrics = pd.DataFrame(data)
    df_metrics = df_metrics.sort_values("Null Percentage (%)", ascending=False)
    
    fig = px.bar(df_metrics, x="Column", y="Null Percentage (%)", 
                 title="Missing Values (Null %) Per Column",
                 hover_data=["Null Count", "Distinct Count", "Datatype"],
                 color="Null Percentage (%)", color_continuous_scale="Reds")
    fig.update_layout(
        template="plotly_dark",
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#e0e0e0")
    )
    return fig

def plot_generic_duplicates(duplicate_rate):
    if duplicate_rate is None:
        return None
    fig = px.pie(names=["Unique Rows", "Duplicate Rows"], values=[100.0 - duplicate_rate, duplicate_rate], 
                  hole=.4, title="Row Uniqueness Profile", color_discrete_sequence=["#00CC96", "#EF553B"])
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#e0e0e0")
    )
    return fig
