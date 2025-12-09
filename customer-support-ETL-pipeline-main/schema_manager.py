from libs import *
from logger import get_logger, log_step_start, log_step_complete, log_df_info, log_db_ops, log_error, log_warning, log_debug_info

class SchemaManager:
    def __init__(self, engine, schema_name, logger):
        self.engine = engine
        self.schema_name = schema_name
        self.logger = logger
        self.logger.info(f"SchemaManager initialized with schema: {schema_name}")

    # split customers vs complaints 
    def split_data(self, df: pd.DataFrame):
        self.logger.info("Splitting DataFrame into customers and complaints")
        customer_columns = ['number','name','gender','dateOfBirth','accountType','branch']
        complaint_columns = [
            'number','location','region','logDate','complaintSource',
            'natureOfComplaint','subject','detailsOfComplaint',
            'comment','updates','status','turnaroundTime','resolutionDate',
            'reasonForReversalRequest','assign','nameOfCcRep'
        ]

        customer_cols = [col for col in customer_columns if col in df.columns]
        complaint_cols = [col for col in complaint_columns if col in df.columns]
        self.logger.info(f"Customer columns: {customer_cols}")
        self.logger.info(f"Complaint columns: {complaint_cols}")

        customers_df = df[customer_cols].drop_duplicates().reset_index(drop=True)
        complaints_df = df[complaint_cols].copy()

        self.logger.info(f"Split complete: customers: {len(customers_df)}, complaints: {len(complaints_df)}")
        return customers_df, complaints_df
    
    # Sync profile IDs 
    def sync_profile_ids(self):
        self.logger.info("Syncing profile IDs between customers and complaints")
        with self.engine.begin() as conn:
            conn.execute(text(f"""
                UPDATE {self.schema_name}.customers c
                SET "profileId" = cl."profileId"
                FROM public.client cl
                WHERE TRIM(c."number")::varchar = ANY(ARRAY[TRIM(cl."phoneNumber"), TRIM(cl."phoneNumber2")]);

                UPDATE {self.schema_name}.complaints co
                SET "profileId" = cl."profileId"
                FROM public.client cl
                WHERE TRIM(co."number")::varchar = ANY(ARRAY[TRIM(cl."phoneNumber"), TRIM(cl."phoneNumber2")]);
            """))
        self.logger.info("Profile ID sync complete")

    # Sync number2 from public.client
    def sync_number2(self):
        self.logger.info("Syncing number2 from public.client to customers and complaints")
        with self.engine.begin() as conn:
            conn.execute(text(f"""
                UPDATE {self.schema_name}.customers c
                SET "number2" = cl."phoneNumber2"
                FROM public.client cl
                WHERE c."profileId" = cl."profileId";
                
                UPDATE {self.schema_name}.complaints co
                SET "number2" = cl."phoneNumber2"
                FROM public.client cl
                WHERE co."profileId" = cl."profileId";
            """))
        self.logger.info("Number2 sync complete")

   
    def setup_schema(self, df: pd.DataFrame, split_func=None):
        self.logger.info("Setting up schema for DataFrame")
        split_func = split_func or self.split_data
        customers_df, complaints_df = split_func(df) 
        self.logger.info(f"Split complete: customers: {len(customers_df)}, complaints: {len(complaints_df)}")

        with self.engine.begin() as conn:

            # DROP SCHEMA WITH CASCADE
            conn.execute(text(f"DROP SCHEMA IF EXISTS {self.schema_name} CASCADE;"))
            conn.execute(text(f"CREATE SCHEMA {self.schema_name};"))
            self.logger.info(f"Schema {self.schema_name} created")

            # Write tables WITHOUT profileId and number2 columns first
            customers_df.to_sql('customers', conn, schema=self.schema_name, 
                            if_exists='replace', index=False)
            self.logger.info(f"Customers table written with {len(customers_df)} rows")
            complaints_df.to_sql('complaints', conn, schema=self.schema_name, 
                            if_exists='replace', index=False)
            self.logger.info(f"Complaints table written with {len(complaints_df)} rows")

            # ADD profileId and number2 columns after table creation
            conn.execute(text(f"""
                ALTER TABLE {self.schema_name}.customers 
                ADD COLUMN IF NOT EXISTS "profileId" VARCHAR(50),
                ADD COLUMN IF NOT EXISTS "number2" VARCHAR(50);
            """))
            self.logger.info("Added profileId and number2 columns to customers table")
            
            conn.execute(text(f"""
                ALTER TABLE {self.schema_name}.complaints 
                ADD COLUMN IF NOT EXISTS "profileId" VARCHAR(50),
                ADD COLUMN IF NOT EXISTS "number2" VARCHAR(50);
            """))
            self.logger.info("Added profileId and number2 columns to complaints table")

        # now to populate the columns
        self.logger.info("Syncing profile IDs from public.client...")
        self.sync_profile_ids()
        
        self.logger.info("Syncing number2 from public.client...")
        self.sync_number2()

        self.logger.info(f"Schema {self.schema_name} setup complete")

    # Main function to split and sync
    def split_and_sync_data(self, df: pd.DataFrame):

        self.logger.info("Splitting data into customers and complaints...")
        customers_df, complaints_df = self.split_data(df) 

        self.logger.info("Syncing profile IDs...")
        self.sync_profile_ids()
        
        self.logger.info(f"Split completed: {len(customers_df)} customers, {len(complaints_df)} complaints")
        return customers_df, complaints_df