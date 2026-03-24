import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import os

# Page configuration
st.set_page_config(
    page_title="Nike Global Price Monitor",
    page_icon="None",
    layout="wide"
)

# Database connection
def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        database=os.getenv("POSTGRES_DB", "nike_dw"),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "adminpassword")
    )

st.title("Nike Global Price Monitoring Dashboard")
st.markdown("---")

# Data loading
@st.cache_data
def load_data():
    conn = get_connection()
    query = """
    SELECT 
        f.date_day,
        p.category,
        p.product_name,
        p.sku,
        p.gender,
        g.country_code,
        f.price_local,
        f.effective_price_local,
        f.price_usd,
        f.effective_price_usd,
        f.discount_amount,
        f.discount_amount_usd,
        f.discount_percentage_local
    FROM dbt_dev.fct_daily_prices f
    JOIN dbt_dev.dim_product p ON f.product_key = p.product_key
    JOIN dbt_dev.dim_geography g ON f.geography_key = g.geography_key
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

try:
    df = load_data()

    # Sidebar filters
    st.sidebar.header("Filters")
    selected_category = st.sidebar.multiselect("Category", df['category'].unique(), default=df['category'].unique())
    selected_gender = st.sidebar.multiselect("Gender", df['gender'].unique(), default=df['gender'].unique())

    filtered_df = df[df['category'].isin(selected_category) & df['gender'].isin(selected_gender)]

    # Metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Records", f"{len(filtered_df):,}")
    col2.metric("Avg Global Price (USD)", f"${filtered_df['effective_price_usd'].mean():.2f}")
    col3.metric("Categories", f"{filtered_df['category'].nunique()}")

    st.markdown("---")

    # 5 Key Business Insights
    st.subheader("5 Key Data Insights (Normalized to USD)")
    ins_col1, ins_col2 = st.columns(2)

    with ins_col1:
        # Insight 1: Most Expensive Category
        max_cat = filtered_df.groupby('category')['effective_price_usd'].mean().idxmax()
        st.write(f"1. **Category Leader**: `{max_cat}` is the most expensive category globally on average.")
        
        # Insight 2: Gender Dominance
        gender_counts = filtered_df['gender'].value_counts()
        top_gender = gender_counts.index[0]
        st.write(f"2. **Inventory Focus**: `{top_gender}` has the highest product count ({gender_counts.iloc[0]:,} SKUs).")
        
        # Insight 3: Global Price Volatility
        volatility = filtered_df['effective_price_usd'].std()
        st.write(f"3. **Price Volatility**: Standard deviation is `${volatility:.2f}`, showing significant price variety in USD.")

    with ins_col2:
        # Insight 4: Market Reach
        countries = filtered_df['country_code'].nunique()
        st.write(f"4. **Global Reach**: Monitoring active pricing across `{countries}` distinct country markets.")
        
        # Insight 5: Average Discount Context
        avg_disc = filtered_df['discount_amount_usd'].mean()
        st.write(f"5. **Discount Impact**: The average monetary discount per item is `${avg_disc:.2f}` USD.")

        # Insight 6: Price Trend
        latest_date = filtered_df['date_day'].max()
        avg_price_latest = filtered_df[filtered_df['date_day'] == latest_date]['effective_price_usd'].mean()
        st.write(f"6. **Latest Trend**: Global average is `${avg_price_latest:.2f}` (as of `{latest_date}`).")

    st.markdown("---")

    if filtered_df.empty:
        st.warning("No data matches the current filters. Please verify that the data has been ingested and transformations have run.")
    else:
        # Clean dates to avoid time-of-day artifacts in charts
        filtered_df['date_day'] = pd.to_datetime(filtered_df['date_day']).dt.date

        # Temporal Trends: Historical Price Evolution
        st.subheader("Temporal Trends: Historical Price Evolution")
        
        # Group data by date to establish an average trend line
        temporal_data = filtered_df.groupby('date_day')['effective_price_usd'].mean().reset_index()
        
        fig_time = px.area(
            temporal_data,
            x='date_day',
            y='effective_price_usd',
            labels={'date_day': 'Date', 'effective_price_usd': 'Avg Price (USD)'},
            template='plotly_dark',
            height=400,
            title="Global Average Price Evolution (USD)",
            markers=True
        )
        fig_time.update_traces(mode='lines+markers')
        st.plotly_chart(fig_time, use_container_width=True)
        st.markdown("---")

        # Row 1: The Core Metrics (Category & Diversity)
        st.subheader("Primary Analysis: Categories & Diversity")
        r1_col1, r1_col2 = st.columns(2)

        with r1_col1:
            st.markdown("#### Average Price by Category & Gender (USD)")
            fig_cat = px.bar(
                filtered_df.groupby(['category', 'gender'])['effective_price_usd'].mean().reset_index(),
                x='category',
                y='effective_price_usd',
                color='gender',
                barmode='group',
                labels={'effective_price_usd': 'Avg Price (USD)'},
                template='plotly_dark',
                height=400
            )
            st.plotly_chart(fig_cat, use_container_width=True)

        with r1_col2:
            st.markdown("#### Inventory Share by Gender")
            fig_donut = px.pie(
                filtered_df,
                names='gender',
                hole=0.4,
                template='plotly_dark',
                height=400,
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_donut.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_donut, use_container_width=True)

        st.markdown("---")

        # Row 2: Distribution and Ranking
        st.subheader("Deep Dive: Distribution & Rankings")
        r2_col1, r2_col2 = st.columns(2)

        with r2_col1:
            st.markdown("#### Price Distribution (Box Plot) by Category (USD)")
            # Sample data for performance in box plots if needed
            fig_box = px.box(
                filtered_df,
                x='category',
                y='effective_price_usd',
                color='gender',
                labels={'effective_price_usd': 'Price (USD)'},
                template='plotly_dark',
                height=400
            )
            st.plotly_chart(fig_box, use_container_width=True)

        with r2_col2:
            st.markdown("#### Top 10 Most Premium Products (USD)")
            top_10 = filtered_df.groupby('product_name')['effective_price_usd'].max().sort_values(ascending=False).head(10).reset_index()
            fig_rank = px.bar(
                top_10,
                x='effective_price_usd',
                y='product_name',
                orientation='h',
                labels={'effective_price_usd': 'Price (USD)', 'product_name': 'Product'},
                template='plotly_dark',
                height=400
            )
            fig_rank.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_rank, use_container_width=True)

        st.markdown("---")

        # Row 3: Market Comparison
        st.subheader("Market Comparison (Normalized to USD)")
        st.markdown("#### Average Price by Country Market (USD)")
        fig_market = px.bar(
            filtered_df.groupby('country_code')['effective_price_usd'].mean().reset_index().sort_values('effective_price_usd', ascending=False),
            x='country_code',
            y='effective_price_usd',
            labels={'effective_price_usd': 'Avg Price (USD)', 'country_code': 'Country'},
            template='plotly_dark',
            color='effective_price_usd',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig_market, use_container_width=True)

        with st.expander("View Raw Data Sample"):
            st.write(filtered_df.head(10))

except Exception as e:
    st.error(f"Error connecting to database: {e}")
    st.info("Make sure the database is running and transformations are completed.")
