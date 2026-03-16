"""
🐋 Whale Watch Dashboard
Real-time on-chain analytics tracking large cryptocurrency movements.
"""
import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import os

# Config
st.set_page_config(
    page_title="🐋 Whale Watch",
    page_icon="🐋",
    layout="wide",
    initial_sidebar_state="collapsed",
)

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "whale_watch.duckdb")


@st.cache_data(ttl=60)
def load_data():
    """Load whale data from DuckDB."""
    con = duckdb.connect(DB_PATH, read_only=True)
    
    # All enriched transactions
    txs = con.sql("SELECT * FROM int_whale_transactions_enriched ORDER BY block_number DESC").fetchdf()
    
    # Summary stats
    summary = con.sql("SELECT * FROM mart_whale_summary ORDER BY block_number DESC").fetchdf()
    
    # Quick stats
    stats = con.sql("""
        SELECT 
            COUNT(*) as total_whales,
            SUM(value_eth) as total_eth,
            AVG(value_eth) as avg_eth,
            MAX(value_eth) as max_eth,
            COUNT(DISTINCT from_address) as unique_wallets
        FROM int_whale_transactions_enriched
    """).fetchdf()
    
    con.close()
    return txs, summary, stats


# Custom dark theme CSS
st.markdown("""
<style>
    .stApp {
        background-color: #0a0e17;
        color: #e0e0e0;
    }
    .metric-card {
        background: linear-gradient(135deg, #1a1f2e 0%, #0d1117 100%);
        border: 1px solid #30363d;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
    }
    .metric-value {
        font-size: 2.2rem;
        font-weight: 700;
        color: #58a6ff;
    }
    .metric-label {
        font-size: 0.85rem;
        color: #8b949e;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    .whale-header {
        text-align: center;
        padding: 20px 0;
    }
    .whale-header h1 {
        font-size: 2.5rem;
        background: linear-gradient(90deg, #58a6ff, #bc8cff);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .whale-header p {
        color: #8b949e;
        font-size: 1.1rem;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("""
<div class="whale-header">
    <h1>🐋 Whale Watch</h1>
    <p>Real-time Ethereum whale transaction tracker</p>
</div>
""", unsafe_allow_html=True)

# Load data
try:
    txs, summary, stats = load_data()
    
    # Metric cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{int(stats['total_whales'].iloc[0])}</div>
            <div class="metric-label">Whale Transactions</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        total_eth = stats['total_eth'].iloc[0]
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{total_eth:,.1f} Ξ</div>
            <div class="metric-label">Total ETH Moved</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        max_eth = stats['max_eth'].iloc[0]
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{max_eth:,.1f} Ξ</div>
            <div class="metric-label">Largest Transaction</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        unique = int(stats['unique_wallets'].iloc[0])
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{unique}</div>
            <div class="metric-label">Unique Wallets</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Two column layout
    left, right = st.columns([2, 1])
    
    with left:
        st.subheader("🐋 Recent Whale Transactions")
        
        if not txs.empty:
            display_df = txs[['tx_hash', 'from_address', 'to_address', 'value_eth', 'whale_tier', 'flow_type']].copy()
            display_df['from_address'] = display_df['from_address'].str[:12] + '...'
            display_df['to_address'] = display_df['to_address'].str[:12] + '...'
            display_df['tx_hash'] = display_df['tx_hash'].str[:16] + '...'
            display_df['value_eth'] = display_df['value_eth'].apply(lambda x: f"{x:,.2f} Ξ")
            display_df.columns = ['TX Hash', 'From', 'To', 'Value', 'Tier', 'Flow']
            
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No whale transactions found yet. Run the pipeline!")
    
    with right:
        st.subheader("📊 Flow Distribution")
        
        if not txs.empty:
            flow_counts = txs['flow_type'].value_counts().reset_index()
            flow_counts.columns = ['Flow Type', 'Count']
            
            fig = px.pie(
                flow_counts,
                values='Count',
                names='Flow Type',
                color_discrete_sequence=['#58a6ff', '#bc8cff', '#3fb950'],
                hole=0.4,
            )
            fig.update_layout(
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font_color='#e0e0e0',
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=-0.2),
                margin=dict(t=20, b=20, l=20, r=20),
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Volume by block
    if not summary.empty and len(summary) > 1:
        st.subheader("📈 ETH Volume by Block")
        fig2 = px.bar(
            summary,
            x='block_number',
            y='total_eth_moved',
            color_discrete_sequence=['#58a6ff'],
        )
        fig2.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='#e0e0e0',
            xaxis_title="Block Number",
            yaxis_title="ETH Moved",
            margin=dict(t=20, b=40, l=40, r=20),
        )
        st.plotly_chart(fig2, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #8b949e; font-size: 0.8rem;">
        Built with Python, dbt, DuckDB, Dagster & Streamlit | Data from Etherscan V2 API
    </div>
    """, unsafe_allow_html=True)

except Exception as e:
    st.error(f"Error loading data: {e}")
    st.info("Make sure you've run the pipeline first: `python ingest/ingest.py` then `dbt run`")
