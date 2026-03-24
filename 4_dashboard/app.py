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
        f.discount_amount,
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
    col2.metric("Avg Price", f"{filtered_df['effective_price_local'].mean():.2f}")
    col3.metric("Categories", f"{filtered_df['category'].nunique()}")

    st.markdown("---")

    # 5 Key Business Insights
    st.subheader("5 Key Data Insights")
    ins_col1, ins_col2 = st.columns(2)

    with ins_col1:
        # Insight 1: Most Expensive Category
        max_cat = filtered_df.groupby('category')['effective_price_local'].mean().idxmax()
        st.write(f"1. **Category Leader**: `{max_cat}` is the most expensive category on average.")
        
        # Insight 2: Gender Dominance
        gender_counts = filtered_df['gender'].value_counts()
        top_gender = gender_counts.index[0]
        st.write(f"2. **Inventory Focus**: `{top_gender}` has the highest product count ({gender_counts.iloc[0]:,} SKUs).")
        
        # Insight 3: Price Volatility
        volatility = filtered_df['effective_price_local'].std()
        st.write(f"3. **Price Volatility**: Standard deviation of prices is `{volatility:.2f}`, showing significant catalog variety.")

    with ins_col2:
        # Insight 4: Market Reach
        countries = filtered_df['country_code'].nunique()
        st.write(f"4. **Global Reach**: Monitoring active pricing across `{countries}` distinct country markets.")
        
        # Insight 5: Average Discount Context
        avg_disc = filtered_df['discount_amount'].mean()
        st.write(f"5. **Discount Impact**: The average monetary discount per item is `{avg_disc:.2f}` local units.")

    st.markdown("---")

    if filtered_df.empty:
        st.warning("No data matches the current filters. Please verify that the data has been ingested and transformations have run.")
    else:
        # Clean dates to avoid time-of-day artifacts in charts
        filtered_df['date_day'] = pd.to_datetime(filtered_df['date_day']).dt.date

        # Row 1: The Core Metrics (Category & Time)
        st.subheader("Primary Analysis: Categories & Trends")
        r1_col1, r1_col2 = st.columns(2)

        with r1_col1:
            st.markdown("#### Average Price by Category & Gender")
            fig_cat = px.bar(
                filtered_df.groupby(['category', 'gender'])['effective_price_local'].mean().reset_index(),
                x='category',
                y='effective_price_local',
                color='gender',
                barmode='group',
                labels={'effective_price_local': 'Avg Price'},
                template='plotly_dark',
                height=400
            )
            st.plotly_chart(fig_cat, use_container_width=True)

        with r1_col2:
            st.markdown("#### Temporal Price Distribution")
            fig_temp = px.area(
                filtered_df.groupby('date_day')['effective_price_local'].mean().reset_index(),
                x='date_day',
                y='effective_price_local',
                markers=True,
                labels={'effective_price_local': 'Avg Price', 'date_day': 'Date'},
                template='plotly_dark',
                height=400
            )
            st.plotly_chart(fig_temp, use_container_width=True)

        st.markdown("---")

        # Row 2: Distribution and Ranking
        st.subheader("Deep Dive: Distribution & Rankings")
        r2_col1, r2_col2 = st.columns(2)

        with r2_col1:
            st.markdown("#### Price Distribution (Box Plot) by Category")
            # Sample data for performance in box plots if needed
            fig_box = px.box(
                filtered_df,
                x='category',
                y='effective_price_local',
                color='gender',
                labels={'effective_price_local': 'Price'},
                template='plotly_dark',
                height=400
            )
            st.plotly_chart(fig_box, use_container_width=True)

        with r2_col2:
            st.markdown("#### Top 10 Most Premium Products")
            top_10 = filtered_df.groupby('product_name')['effective_price_local'].max().sort_values(ascending=False).head(10).reset_index()
            fig_rank = px.bar(
                top_10,
                x='effective_price_local',
                y='product_name',
                orientation='h',
                labels={'effective_price_local': 'Price', 'product_name': 'Product'},
                template='plotly_dark',
                height=400
            )
            fig_rank.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_rank, use_container_width=True)

        st.markdown("---")

        # Row 3: Market Comparison
        st.subheader("Market Comparison")
        st.markdown("#### Average Price by Country Market")
        fig_market = px.bar(
            filtered_df.groupby('country_code')['effective_price_local'].mean().reset_index().sort_values('effective_price_local', ascending=False),
            x='country_code',
            y='effective_price_local',
            labels={'effective_price_local': 'Avg Price', 'country_code': 'Country'},
            template='plotly_dark',
            color='effective_price_local',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig_market, use_container_width=True)

        with st.expander("View Raw Data Sample"):
            st.write(filtered_df.head(10))

except Exception as e:
    st.error(f"Error connecting to database: {e}")
    st.info("Make sure the database is running and transformations are completed.")
