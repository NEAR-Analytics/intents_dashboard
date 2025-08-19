import os
import pandas as pd
import streamlit as st
from flipside_handler import get_fs_data
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta

SQL_TOP_ASSETS_PATH = os.path.join(os.path.dirname(__file__), "queries_top_assets.sql")
SQL_DAILY_CUMULATIVE_PATH = os.path.join(os.path.dirname(__file__), "queries_daily_cumulative.sql")
SQL_SUMMARY_STATS_PATH = os.path.join(os.path.dirname(__file__), "queries_summary_stats.sql")

def _read_sql(path: str) -> str:
    try:
        with open(path, "r") as f:
            return f.read().strip()
    except Exception:
        return ""

@st.cache_data(show_spinner=False, ttl=300)
def run_query_text(sql_text: str) -> pd.DataFrame:
    return get_fs_data(query_path=None, query_text=sql_text)

@st.cache_data(show_spinner=False, ttl=300)
def run_query_file(sql_path: str) -> pd.DataFrame:
    return get_fs_data(query_path=sql_path, query_text=None)

def format_currency(value):
    """Format currency values"""
    if pd.isna(value):
        return "$0"
    return f"${value:,.2f}"

def format_number(value):
    """Format large numbers with commas"""
    if pd.isna(value):
        return "0"
    return f"{int(value):,}"

def create_kpi_metrics(summary_df):
    """Create KPI metric cards using native Streamlit metrics"""
    if summary_df.empty:
        st.warning("No summary data available")
        return
    
    row = summary_df.iloc[0]
    
    # Create 6 columns for metrics
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.metric(
            label="Total Fees Collected (USD)",
            value=format_currency(row.get('total_usd', 0)),
            delta="All-time total"
        )
    
    with col2:
        st.metric(
            label="Total Transactions",
            value=format_number(row.get('total_transactions', 0)),
            delta="Fee collections"
        )
    
    with col3:
        st.metric(
            label="Unique Assets",
            value=f"{int(row.get('unique_assets', 0))}+",
            delta="Different tokens"
        )
    
    with col4:
        st.metric(
            label="Source Blockchains",
            value=str(int(row.get('unique_chains', 0))),
            delta="Chains integrated"
        )
    
    with col5:
        today_usd = row.get('today_usd', 0)
        latest_date = row.get('latest_date', datetime.now())
        if isinstance(latest_date, str):
            date_str = latest_date.split()[0]
        else:
            date_str = latest_date.strftime('%b %d, %Y')
        st.metric(
            label="Today's Fees",
            value=format_currency(today_usd),
            delta=date_str
        )
    
    with col6:
        top_asset = row.get('top_asset', 'N/A')
        top_asset_usd = row.get('top_asset_usd', 0)
        st.metric(
            label="Top Asset",
            value=top_asset if top_asset else "N/A",
            delta=format_currency(top_asset_usd)
        )

def prepare_daily_data(df: pd.DataFrame, view_type: str = 'asset'):
    """Prepare data for daily charts based on view type"""
    if df.empty or "date" not in df.columns:
        return None
    
    if view_type == 'asset':
        # Group by date and asset
        pivot_col = 'asset'
        top_items = df.groupby('asset')['daily_usd_amount'].sum().nlargest(4).index.tolist()
    else:
        # Group by date and blockchain - show more blockchains
        pivot_col = 'source_chain'
        top_items = df.groupby('source_chain')['daily_usd_amount'].sum().nlargest(8).index.tolist()
    
    # Create pivot table
    df_copy = df.copy()
    df_copy['category'] = df_copy[pivot_col].apply(lambda x: x if x in top_items else 'Others')
    
    pivot_df = df_copy.pivot_table(
        index='date',
        columns='category',
        values='daily_usd_amount',
        aggfunc='sum',
        fill_value=0
    ).reset_index()
    
    return pivot_df

def create_daily_stacked_column_chart(df: pd.DataFrame, view_type: str = 'asset'):
    """Create stacked column chart for daily fee collection"""
    if df.empty:
        return None
    
    pivot_df = prepare_daily_data(df, view_type)
    if pivot_df is None:
        return None
    
    # Define more colors for additional categories
    colors = ['#1D4E89', '#F79256', '#00B2CA', '#7DCFB6', '#FBD1A2', '#B08EA2', '#C5D86D', '#A23E48', '#6C464E', '#9E7682']
    
    # Create stacked bar chart
    fig = go.Figure()
    
    columns = [col for col in pivot_df.columns if col != 'date']
    
    for i, col in enumerate(columns):
        fig.add_trace(go.Bar(
            name=col,
            x=pivot_df['date'],
            y=pivot_df[col],
            marker_color=colors[i % len(colors)],
            hovertemplate='<b>%{x|%b %d}</b><br>' + col + ': $%{y:,.2f}<extra></extra>'
        ))
    
    title = f"Daily Fee Collection by {'Asset' if view_type == 'asset' else 'Source Blockchain'} (USD)"
    subtitle = f"Daily fees broken down by top {'assets' if view_type == 'asset' else 'source blockchains'}"
    
    fig.update_layout(
        title={
            'text': f"{title}<br><sub>{subtitle}</sub>",
            'x': 0.5,
            'xanchor': 'center'
        },
        barmode='stack',
        xaxis_title="Date",
        yaxis_title="Fees (USD)",
        yaxis=dict(tickformat="$,.0f"),
        hovermode='x unified',
        height=500,
        showlegend=True,
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )
    
    return fig

def create_cumulative_area_chart(df: pd.DataFrame, view_type: str = 'asset'):
    """Create stacked area chart for cumulative fee collection"""
    if df.empty:
        return None
    
    if view_type == 'asset':
        pivot_col = 'asset'
        top_items = df.groupby('asset')['cumulative_usd_amount'].max().nlargest(4).index.tolist()
    else:
        pivot_col = 'source_chain'
        # Increase to 8 blockchains for blockchain view to reduce "Others" category
        top_items = df.groupby('source_chain')['cumulative_usd_amount'].max().nlargest(8).index.tolist()
    
    # Prepare cumulative data
    df_copy = df.copy()
    df_copy['category'] = df_copy[pivot_col].apply(lambda x: x if x in top_items else 'Others')
    
    # Calculate cumulative for each category
    cumulative_data = []
    for date in df_copy['date'].unique():
        date_data = {'date': date}
        for category in df_copy['category'].unique():
            mask = (df_copy['date'] <= date) & (df_copy['category'] == category)
            date_data[category] = df_copy[mask]['daily_usd_amount'].sum()
        cumulative_data.append(date_data)
    
    cumulative_df = pd.DataFrame(cumulative_data).sort_values('date')
    
    # Define more colors for blockchain view
    colors = ['#1D4E89', '#F79256', '#00B2CA', '#7DCFB6', '#FBD1A2', '#B08EA2', '#C5D86D', '#A23E48', '#6C464E', '#9E7682']
    
    # Create stacked area chart
    fig = go.Figure()
    
    columns = [col for col in cumulative_df.columns if col != 'date']
    
    for i, col in enumerate(columns):
        fig.add_trace(go.Scatter(
            name=col,
            x=cumulative_df['date'],
            y=cumulative_df[col],
            mode='lines',
            line=dict(width=0.5, color=colors[i % len(colors)]),
            stackgroup='one',
            fillcolor=colors[i % len(colors)],
            hovertemplate='<b>%{x|%b %d}</b><br>' + col + ': $%{y:,.2f}<extra></extra>'
        ))
    
    title = f"Cumulative Fee Collection by {'Asset' if view_type == 'asset' else 'Source Blockchain'} Over Time"
    subtitle = f"Total fees accumulated since inception"
    
    fig.update_layout(
        title={
            'text': f"{title}<br><sub>{subtitle}</sub>",
            'x': 0.5,
            'xanchor': 'center'
        },
        xaxis_title="Date",
        yaxis_title="Cumulative Fees (USD)",
        yaxis=dict(tickformat="$,.0f"),
        hovermode='x unified',
        height=500,
        showlegend=True,
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )
    
    return fig

def create_horizontal_bar_chart(df: pd.DataFrame, view_type: str = 'asset'):
    """Create horizontal bar chart for fee distribution"""
    if df.empty:
        return None
    
    if view_type == 'asset':
        # Group by asset
        grouped = df.groupby('asset').agg({
            'total_usd': 'sum',
            'total_txs': 'sum'
        }).reset_index()
        label_col = 'asset'
        # Keep 9 top items for assets
        top_n = 9
    else:
        # Group by blockchain
        grouped = df.groupby('source_chain').agg({
            'total_usd': 'sum',
            'total_txs': 'sum'
        }).reset_index()
        label_col = 'source_chain'
        # Show 12 items for blockchains to reduce "Others"
        top_n = 12
    
    # Sort and take top items
    grouped = grouped.sort_values('total_usd', ascending=False).head(top_n)
    
    # Calculate percentages
    total_sum = grouped['total_usd'].sum()
    grouped['percentage'] = (grouped['total_usd'] / total_sum * 100).round(1)
    
    # Add "Others" if there are more items
    if len(df[label_col].unique()) > top_n:
        others_usd = df[~df[label_col].isin(grouped[label_col])]['total_usd'].sum()
        others_row = pd.DataFrame({
            label_col: ['Others'],
            'total_usd': [others_usd],
            'percentage': [others_usd / (total_sum + others_usd) * 100]
        })
        grouped = pd.concat([grouped, others_row], ignore_index=True)
    
    # Sort for horizontal bar chart
    grouped = grouped.sort_values('total_usd', ascending=True)
    
    # Define more colors for additional items
    colors = ['#1D4E89', '#F79256', '#00B2CA', '#7DCFB6', '#FBD1A2', '#B08EA2', '#C5D86D', '#A23E48', '#6C464E', '#9E7682', '#FF6B6B', '#4ECDC4', '#45B7D1']
    
    # Create bar chart
    fig = go.Figure(go.Bar(
        x=grouped['total_usd'],
        y=grouped[label_col],
        orientation='h',
        marker_color=[colors[i % len(colors)] for i in range(len(grouped))],
        text=grouped.apply(lambda x: f"${x['total_usd']:,.0f} ({x['percentage']}%)", axis=1),
        textposition='outside',
        hovertemplate='<b>%{y}</b><br>Total: $%{x:,.2f}<br>Percentage: %{customdata}%<extra></extra>',
        customdata=grouped['percentage']
    ))
    
    title = f"Total Fee Distribution by {'Asset' if view_type == 'asset' else 'Source Blockchain'}"
    subtitle = f"All-time fees collected per {'asset' if view_type == 'asset' else 'source blockchain'}"
    
    fig.update_layout(
        title={
            'text': f"{title}<br><sub>{subtitle}</sub>",
            'x': 0.5,
            'xanchor': 'center'
        },
        xaxis_title="Total Fees (USD)",
        yaxis_title="",
        xaxis=dict(tickformat="$,.0f"),
        height=500,
        showlegend=False
    )
    
    return fig

def create_top_performers_area_chart(df: pd.DataFrame, view_type: str = 'asset'):
    """Create area chart showing top performers over time"""
    if df.empty:
        return None
    
    if view_type == 'asset':
        pivot_col = 'asset'
        top_items = df.groupby('asset')['daily_usd_amount'].sum().nlargest(5).index.tolist()
    else:
        pivot_col = 'source_chain'
        # Show top 8 blockchains for better visibility
        top_items = df.groupby('source_chain')['daily_usd_amount'].sum().nlargest(8).index.tolist()
    
    # Filter for top items
    df_filtered = df[df[pivot_col].isin(top_items)].copy()
    
    # Pivot data
    pivot_df = df_filtered.pivot_table(
        index='date',
        columns=pivot_col,
        values='daily_usd_amount',
        aggfunc='sum',
        fill_value=0
    ).reset_index()
    
    # Define more colors for additional series
    colors = ['#1D4E89', '#F79256', '#00B2CA', '#7DCFB6', '#FBD1A2', '#B08EA2', '#C5D86D', '#A23E48']
    
    # Create area chart
    fig = go.Figure()
    
    columns = [col for col in pivot_df.columns if col != 'date']
    
    for i, col in enumerate(columns):
        fig.add_trace(go.Scatter(
            name=col,
            x=pivot_df['date'],
            y=pivot_df[col],
            mode='lines',
            line=dict(width=0.5, color=colors[i % len(colors)]),
            stackgroup='one',
            fillcolor=colors[i % len(colors)],
            hovertemplate='<b>%{x|%b %d}</b><br>' + col + ': $%{y:,.2f}<extra></extra>'
        ))
    
    title = f"Top 5 {'Assets' if view_type == 'asset' else 'Source Blockchains'} Fee Collection Over Time"
    subtitle = f"Daily breakdown of fees by top performing {'assets' if view_type == 'asset' else 'source blockchains'}"
    
    fig.update_layout(
        title={
            'text': f"{title}<br><sub>{subtitle}</sub>",
            'x': 0.5,
            'xanchor': 'center'
        },
        xaxis_title="Date",
        yaxis_title="Daily Fees (USD)",
        yaxis=dict(tickformat="$,.0f"),
        hovermode='x unified',
        height=500,
        showlegend=True,
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )
    
    return fig

def main():
    st.set_page_config(
        page_title="NEAR Intents Fee Dashboard", 
        layout="wide",
        page_icon="💰"
    )
    
    # Header
    st.markdown("""
    <div style='text-align: center; padding: 2rem 0; background: linear-gradient(135deg, #1D4E89 0%, #00B2CA 100%); 
                color: white; border-radius: 10px; margin-bottom: 2rem;'>
        <h1 style='margin: 0; font-size: 2.5rem;'>💰 NEAR Intents Fee Collection Dashboard</h1>
        <p style='margin: 0.5rem 0 0 0; font-size: 1.2rem;'>Account: app-fee.near | Real-time Multichain Fee Analytics</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Load data
    with st.spinner("Loading dashboard data..."):
        try:
            summary_df = run_query_file(SQL_SUMMARY_STATS_PATH)
            df_daily = run_query_file(SQL_DAILY_CUMULATIVE_PATH)
            df_top = run_query_file(SQL_TOP_ASSETS_PATH)
        except Exception as e:
            st.error(f"Error loading data: {e}")
            summary_df = pd.DataFrame()
            df_daily = pd.DataFrame()
            df_top = pd.DataFrame()
    
    # KPI Metrics
    st.header("📊 Summary Statistics")
    create_kpi_metrics(summary_df)
    
    # Charts Section
    st.header("📈 Analytics & Trends")
    
    # Daily Fee Collection Chart
    st.subheader("Daily Fee Collection")
    col1, col2 = st.columns([3, 1])
    with col2:
        daily_view = st.radio(
            "View by:",
            ["Asset Breakdown", "Source Blockchain"],
            key="daily_view",
            horizontal=True
        )
    view_type = 'asset' if daily_view == "Asset Breakdown" else 'blockchain'
    fig_daily = create_daily_stacked_column_chart(df_daily, view_type)
    if fig_daily:
        st.plotly_chart(fig_daily, use_container_width=True)
    
    # Cumulative Fee Collection Chart
    st.subheader("Cumulative Fee Collection")
    col1, col2 = st.columns([3, 1])
    with col2:
        cumulative_view = st.radio(
            "View by:",
            ["Asset Breakdown", "Source Blockchain"],
            key="cumulative_view",
            horizontal=True
        )
    view_type = 'asset' if cumulative_view == "Asset Breakdown" else 'blockchain'
    fig_cumulative = create_cumulative_area_chart(df_daily, view_type)
    if fig_cumulative:
        st.plotly_chart(fig_cumulative, use_container_width=True)
    
    # Fee Distribution Chart
    st.subheader("Fee Distribution")
    col1, col2 = st.columns([3, 1])
    with col2:
        distribution_view = st.radio(
            "View by:",
            ["Asset Breakdown", "Source Blockchain"],
            key="distribution_view",
            horizontal=True
        )
    view_type = 'asset' if distribution_view == "Asset Breakdown" else 'blockchain'
    
    # Use df_top for asset view, df_daily for blockchain view
    data_source = df_top if view_type == 'asset' else df_daily
    fig_bar = create_horizontal_bar_chart(data_source, view_type)
    if fig_bar:
        st.plotly_chart(fig_bar, use_container_width=True)
    
    # Top Performers Over Time Chart
    st.subheader("Top Performers Over Time")
    col1, col2 = st.columns([3, 1])
    with col2:
        performers_view = st.radio(
            "View by:",
            ["Asset Breakdown", "Source Blockchain"],
            key="performers_view",
            horizontal=True
        )
    view_type = 'asset' if performers_view == "Asset Breakdown" else 'blockchain'
    fig_performers = create_top_performers_area_chart(df_daily, view_type)
    if fig_performers:
        st.plotly_chart(fig_performers, use_container_width=True)
    
    # Top Assets Table
    st.header("📊 Top 10 Assets by Total Fees Collected")
    
    if not df_top.empty:
        # Format the dataframe for display
        display_df = df_top.head(10).copy()
        
        # Add percentage column
        total_sum = display_df['total_usd'].sum()
        display_df['percentage'] = (display_df['total_usd'] / total_sum * 100)
        
        # Rename columns for display
        display_df = display_df.rename(columns={
            'asset': 'Asset',
            'total_usd': 'Total USD',
            'total_tokens': 'Total Tokens',
            'total_txs': 'Transactions',
            'num_chains': 'Source Chains',
            'percentage': '% of Total'
        })
        
        # Display table with formatting
        st.dataframe(
            display_df,
            use_container_width=True,
            column_config={
                "Total USD": st.column_config.NumberColumn(
                    format="$%.2f"
                ),
                "Total Tokens": st.column_config.NumberColumn(
                    format="%.4f"
                ),
                "Transactions": st.column_config.NumberColumn(
                    format="%d"
                ),
                "% of Total": st.column_config.NumberColumn(
                    format="%.1f%%"
                )
            }
        )
        
        # Download button
        csv = df_top.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download Full Dataset (CSV)",
            data=csv,
            file_name=f"near_intents_fees_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
    else:
        st.info("No asset data available")
    
    # Insights Section
    st.header("💡 Key Insights")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info("""
        **🚀 Rapid Growth Trajectory**
        
        The app-fee.near account has demonstrated impressive fee collection capabilities, 
        processing transactions across multiple blockchains. The daily average fee collection 
        shows strong protocol usage and healthy fee generation.
        """)
    
    with col2:
        st.success("""
        **💎 Diversified Fee Portfolio**
        
        Fees are collected across 35+ different assets from 15 blockchains. 
        The top assets (ETH, ZEC, BTC) demonstrate significant value capture, 
        while stablecoins provide stability.
        """)
    
    with col3:
        st.warning("""
        **🌐 True Multichain Operations**
        
        NEAR Intents showcases exceptional cross-chain capabilities, processing fees from 
        Ethereum, Bitcoin, Zcash, Solana, Arbitrum, Base, Tron, and other major chains.
        """)
    
    # SQL Query Viewer
    st.header("🔍 Technical Details")
    
    with st.expander("📝 View SQL Queries"):
        tab1, tab2, tab3 = st.tabs(["Summary Stats", "Top Assets", "Daily/Cumulative"])
        
        with tab1:
            summary_sql = _read_sql(SQL_SUMMARY_STATS_PATH)
            st.code(summary_sql, language="sql")
        
        with tab2:
            top_sql = _read_sql(SQL_TOP_ASSETS_PATH)
            st.code(top_sql, language="sql")
        
        with tab3:
            daily_sql = _read_sql(SQL_DAILY_CUMULATIVE_PATH)
            st.code(daily_sql, language="sql")
    
    # Footer
    st.divider()
    st.caption("Data Source: Flipside NEAR.DEFI.EZ_INTENTS | Updates every 5 minutes")
    st.caption("Built with Streamlit & Snowflake | © 2025 NEAR Protocol")

if __name__ == "__main__":
    main()