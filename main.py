import os
import pandas as pd
import streamlit as st
from flipside_handler import get_fs_data
from plotly.subplots import make_subplots
import plotly.graph_objects as go

SQL_TOP_ASSETS_PATH = os.path.join(os.path.dirname(__file__), "queries_top_assets.sql")
SQL_DAILY_CUMULATIVE_PATH = os.path.join(os.path.dirname(__file__), "queries_daily_cumulative.sql")

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

def _build_dual_axis_figure(df: pd.DataFrame):
    # Aggregate by date for transfer counts and USD volume
    if df.empty:
        return None
    if "date" not in df.columns:
        return None
    # Normalize column names that we expect from the SQL
    count_col = "transaction_count" if "transaction_count" in df.columns else None
    usd_col = "daily_usd_amount" if "daily_usd_amount" in df.columns else None
    if count_col is None or usd_col is None:
        return None

    agg = (
        df.groupby("date", as_index=False)[[count_col, usd_col]].sum().sort_values("date")
    )

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Primary axis: transfer counts (bars)
    fig.add_trace(
        go.Bar(name="Transfers", x=agg["date"], y=agg[count_col], marker_color="#4e79a7"),
        secondary_y=False,
    )

    # Secondary axis: USD volume (line)
    fig.add_trace(
        go.Scatter(name="USD Volume", x=agg["date"], y=agg[usd_col], mode="lines", line=dict(color="#f28e2b", width=2)),
        secondary_y=True,
    )

    # Spike annotations: top 3 dates by each metric
    top_n = 3
    top_counts_idx = agg[count_col].nlargest(top_n).index
    top_vol_idx = agg[usd_col].nlargest(top_n).index

    # Add spike markers for counts (primary y)
    fig.add_trace(
        go.Scatter(
            name="Count spikes",
            x=agg.loc[top_counts_idx, "date"],
            y=agg.loc[top_counts_idx, count_col],
            mode="markers",
            marker=dict(color="#e15759", size=9, symbol="diamond"),
            hovertemplate="%{x}<br>Transfers: %{y:,}<extra></extra>",
            showlegend=False,
        ),
        secondary_y=False,
    )

    # Add spike markers for volume (secondary y)
    fig.add_trace(
        go.Scatter(
            name="Volume spikes",
            x=agg.loc[top_vol_idx, "date"],
            y=agg.loc[top_vol_idx, usd_col],
            mode="markers",
            marker=dict(color="#59a14f", size=9, symbol="star"),
            hovertemplate="%{x}<br>USD: $%{y:,.0f}<extra></extra>",
            showlegend=False,
        ),
        secondary_y=True,
    )

    # Annotations with arrows
    annotations = []
    for i in top_counts_idx:
        annotations.append(dict(
            x=agg.loc[i, "date"], y=agg.loc[i, count_col],
            xref="x", yref="y",
            text="Count spike",
            showarrow=True, arrowhead=2, ax=0, ay=-30,
            font=dict(color="#e15759")
        ))
    for i in top_vol_idx:
        annotations.append(dict(
            x=agg.loc[i, "date"], y=agg.loc[i, usd_col],
            xref="x", yref="y2",
            text="Volume spike",
            showarrow=True, arrowhead=2, ax=0, ay=-30,
            font=dict(color="#59a14f")
        ))

    fig.update_layout(
        margin=dict(t=40, r=20, b=20, l=20),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        annotations=annotations,
    )
    fig.update_yaxes(title_text="Transfers", secondary_y=False)
    fig.update_yaxes(title_text="USD Volume", secondary_y=True)
    fig.update_xaxes(title_text="Date")
    return fig

def main():
    st.set_page_config(page_title="NEAR Intents Fees (Snowflake)", layout="wide")
    st.title("NEAR Intents Fees")
    st.caption("Data: Flipside NEAR.DEFI.EZ_INTENTS")

    tab1, tab2 = st.tabs(["Top assets", "Daily & cumulative fees"])

    with tab1:
        top_sql = _read_sql(SQL_TOP_ASSETS_PATH)
        if not top_sql:
            st.info("Top assets SQL file not found or empty.")
        else:
            with st.spinner("Running top assets query..."):
                try:
                    df_top = run_query_file(SQL_TOP_ASSETS_PATH)
                    st.success(f"Returned {len(df_top):,} rows")
                    st.dataframe(df_top, use_container_width=True)
                    if not df_top.empty and "total_usd" in df_top.columns and "asset" in df_top.columns:
                        top_chart = df_top[["asset", "total_usd"]].set_index("asset")
                        st.bar_chart(top_chart)
                    if not df_top.empty:
                        st.download_button(
                            "Download CSV",
                            df_top.to_csv(index=False).encode("utf-8"),
                            file_name="top_assets.csv",
                            mime="text/csv",
                        )
                except Exception as e:
                    st.error(f"{type(e).__name__}: {e}")
            with st.expander("View SQL"):
                st.code(top_sql, language="sql")

    with tab2:
        daily_sql = _read_sql(SQL_DAILY_CUMULATIVE_PATH)
        if not daily_sql:
            st.info("Daily & cumulative SQL file not found or empty.")
        else:
            with st.spinner("Running daily & cumulative query..."):
                try:
                    df_daily = run_query_file(SQL_DAILY_CUMULATIVE_PATH)
                    st.success(f"Returned {len(df_daily):,} rows")
                    # Plotly dual-axis figure: transfers vs USD volume with spike annotations
                    fig = _build_dual_axis_figure(df_daily)
                    if fig is not None:
                        st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(df_daily, use_container_width=True)
                    if not df_daily.empty:
                        st.download_button(
                            "Download CSV",
                            df_daily.to_csv(index=False).encode("utf-8"),
                            file_name="daily_cumulative.csv",
                            mime="text/csv",
                        )
                except Exception as e:
                    st.error(f"{type(e).__name__}: {e}")
            with st.expander("View SQL"):
                st.code(daily_sql, language="sql")

if __name__ == "__main__":
    main()
