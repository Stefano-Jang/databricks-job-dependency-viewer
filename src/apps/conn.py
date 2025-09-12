import os
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml or environment."
assert os.getenv('DAG_TABLE_NAME'), "DAG_TABLE_NAME must be set in app.yaml or environment."
DAG_TABLE_NAME = os.getenv('DAG_TABLE_NAME')

user_token = st.context.headers.get('X-Forwarded-Access-Token')

def query_databricks(query: str) -> pd.DataFrame:
    try:
        cfg = Config()  # Auto-pulls environment variables for auth
        with sql.connect( 
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
            access_token=user_token,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                df = cursor.fetchall_arrow().to_pandas()
                
                # Convert datetime columns to strings to avoid JSON serialization issues
                datetime_cols_converted = []
                for col in df.columns:
                    if df[col].dtype == 'datetime64[ns]' or pd.api.types.is_datetime64_any_dtype(df[col]):
                        # Handle NaT (Not a Time) values by converting to empty string
                        df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
                        datetime_cols_converted.append(col)
                    elif df[col].dtype == 'object':
                        # Handle any remaining timestamp objects that might be in object columns
                        has_timestamps = df[col].apply(lambda x: pd.notnull(x) and hasattr(x, 'strftime')).any()
                        if has_timestamps:
                            df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) and hasattr(x, 'strftime') else str(x) if pd.notnull(x) else '')
                            datetime_cols_converted.append(col)
                
                # Ensure boolean columns are proper Python bool (not numpy.bool_)
                for col in df.columns:
                    if df[col].dtype == 'bool':
                        df[col] = df[col].astype(bool)
                
                # Debug info for troubleshooting
                if datetime_cols_converted:
                    print(f"Converted datetime columns to strings: {datetime_cols_converted}")
                
                return df
                
    except Exception as e:
        # Re-raise with more context
        raise Exception(f"Databricks query failed: {str(e)}. Check warehouse ID and table existence.")

@st.cache_data(ttl=300)
def load_dag_data():
    """Load DAG data from Databricks with optional time filtering"""
    # Build time filter for errors if provided  
    error_time_filter = ""
    
    query = f"""
    SELECT * FROM {DAG_TABLE_NAME}
    ORDER BY result_type, id
    """
    
    return query_databricks(query)