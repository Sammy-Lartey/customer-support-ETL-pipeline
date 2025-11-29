from libs import *

class DatabaseHandler:

    def __init__(self, credentials):
        self.credentials = credentials
        self.engine = self._create_engine()

# Database Connection
    def _create_engine(self):
        creds = self.credentials
        connect_string = (
            f"postgresql+psycopg2://{creds['DB_USER']}:{creds['DB_PASSWORD']}"
            f"@{creds['DB_HOST']}:{creds['DB_PORT']}/{creds['DB_NAME']}"
        )
        return create_engine(connect_string)
    
# Write DataFrame to Database
    def write_dataframe(self, df: pd.DataFrame, table_name, schema = None, if_exists = 'replace'):

        try:
            df.to_sql(table_name, self.engine, schema=schema, if_exists=if_exists, index=False, method='multi', chunksize=1000)
            print(f"Data successfully written to {schema if schema else 'public'}.{table_name}")
        except Exception as e:
            print(f"Error writing to database: {e}")
            raise
            
        
# Execute Query
    def execute_query(self, query: str):
        try:
            with self.engine.connect() as conn:
                conn.execute(text(query))
                print("Query executed successfully")
        except Exception as e:
            print(f"Error executing query: {e}")
            raise

        