from sqlalchemy import text
import ulid
from libs import pd
import numpy as np

class DataIntegrator:

    def __init__(self, engine, schema_name: str, logger):
        self.engine = engine
        self.schema_name = schema_name
        self.logger = logger
        self.logger.info("DataIntegrator initialized with engine and schema.")

    def assign_customer_ids(self):
        with self.engine.begin() as conn:
            customers_df = pd.read_sql(f"SELECT * FROM {self.schema_name}.customers", conn)
            complaints_df = pd.read_sql(f"SELECT * FROM {self.schema_name}.complaints", conn)

        self.logger.info(f"Starting with {len(customers_df)} customers, {len(complaints_df)} complaints")
        self.logger.info(f"Customers profileId stats - Not null: {customers_df['profileId'].notna().sum()}, Null: {customers_df['profileId'].isna().sum()}")
    
        # Build profileId->ULID mapping
        profile_to_ulid = {}
        for idx, row in customers_df.iterrows():
            pid = row['profileId']
            if pd.notna(pid) and pid not in profile_to_ulid:
                profile_to_ulid[pid] = str(ulid.new())
        
        # Map phone numbers for rows without profileId
        for idx, row in customers_df.iterrows():
            if pd.isna(row['profileId']) and pd.notna(row['number']):
                phone_key = f"number:{row['number']}"
                if phone_key not in profile_to_ulid:
                    profile_to_ulid[phone_key] = str(ulid.new())

        # Resolve customerId function - FIXED QUOTE ISSUE
        def resolve_customer_id(row):
            if pd.notna(row['profileId']) and row['profileId'] in profile_to_ulid:
                return profile_to_ulid[row['profileId']]
            elif pd.notna(row['number']):
                phone_key = f"number:{row['number']}"  # Store in variable
                return profile_to_ulid.get(phone_key)  # Use variable
            return None

        # Assign customerIds
        customers_df['customerId'] = customers_df.apply(resolve_customer_id, axis=1)
        complaints_df['customerId'] = complaints_df.apply(resolve_customer_id, axis=1)

        # Remove rows with no identifiers
        no_identifiers = customers_df[
            customers_df['customerId'].isna() &
            customers_df['profileId'].isna() &
            customers_df['number'].isna()
        ]
        if len(no_identifiers) > 0:
            customers_df = customers_df[
                customers_df['customerId'].notna() |
                customers_df['profileId'].notna() |
                customers_df['number'].notna()
            ]

        # Drop duplicate customerIds
        customers_df = customers_df.drop_duplicates(subset=['customerId'])

        # Filter complaints to only include valid customerIds
        complaints_df = complaints_df[complaints_df['customerId'].isin(customers_df['customerId'])]

        # Write back to DB
        with self.engine.begin() as conn:
            customers_df.to_sql("customers", conn, schema=self.schema_name, if_exists="replace", index=False)
            complaints_df.to_sql("complaints", conn, schema=self.schema_name, if_exists="replace", index=False)

        self.logger.info(f"Customer IDs assigned: {len(customers_df)} customers, {len(complaints_df)} complaints")

    def reorder_table_columns(self):
        
        with self.engine.begin() as conn:
            # Customers table
            customers_df = pd.read_sql(f"SELECT * FROM {self.schema_name}.customers", conn)
            customer_final_order = [
                'customerId', 'profileId', 'name', 'number', 'number2',
                'gender', 'dateOfBirth', 'accountType', 'branch'
            ]
            customer_final_order = [col for col in customer_final_order if col in customers_df.columns]
            customer_final_order += [col for col in customers_df.columns if col not in customer_final_order]
            customers_df = customers_df[customer_final_order]
            customers_df.to_sql("customers", conn, schema=self.schema_name, if_exists="replace", index=False)

            # Complaints table  
            complaints_df = pd.read_sql(f"SELECT * FROM {self.schema_name}.complaints", conn)
            complaint_final_order = [
                'customerId', 'profileId', 'number', 'number2', 'location', 'region',
                'complaintSource', 'natureOfComplaint', 'subject', 'detailsOfComplaint',
                'comment', 'updates', 'status', 'logDate', 'turnaroundTime', 
                'resolutionDate', 'reasonForReversalRequest'
            ]
            complaint_final_order = [col for col in complaint_final_order if col in complaints_df.columns]
            complaint_final_order += [col for col in complaints_df.columns if col not in complaint_final_order]
            complaints_df = complaints_df[complaint_final_order]
            complaints_df.to_sql("complaints", conn, schema=self.schema_name, if_exists="replace", index=False)

        self.logger.info("Final column ordering completed!")

    def apply_constraints(self):
        
        self.logger.info("Adding database constraints...")
        with self.engine.begin() as conn:
            # Clean up NULL values
            conn.execute(text(f"""
                UPDATE {self.schema_name}.customers
                SET name = 'Unknown'
                WHERE name IS NULL;
            """))

            # Delete NULL phone numbers
            null_number_count = conn.execute(text(f"SELECT COUNT(*) FROM {self.schema_name}.customers WHERE \"number\" IS NULL")).scalar()
            if null_number_count > 0:
                conn.execute(text(f"DELETE FROM {self.schema_name}.customers WHERE \"number\" IS NULL"))
                conn.execute(text(f"DELETE FROM {self.schema_name}.complaints WHERE \"number\" IS NULL"))

            # Set NULL logDate to current date
            conn.execute(text(f"""
                UPDATE {self.schema_name}.complaints
                SET \"logDate\" = CURRENT_DATE
                WHERE \"logDate\" IS NULL;
            """))

            # Handle NULL customerIds
            null_count = conn.execute(text(f"SELECT COUNT(*) FROM {self.schema_name}.customers WHERE \"customerId\" IS NULL")).scalar()
            if null_count > 0:
                conn.execute(text(f"""
                    UPDATE {self.schema_name}.customers
                    SET \"customerId\" = CONCAT('ULID_', MD5(RANDOM()::TEXT || CLOCK_TIMESTAMP()::TEXT))
                    WHERE \"customerId\" IS NULL;
                """))

            # Verify no NULLs remain
            tables_to_check = {
                f"{self.schema_name}.customers": ["number", "name", "customerId"],
                f"{self.schema_name}.complaints": ["number", "logDate", "customerId"]
            }
            
            for table, columns in tables_to_check.items():
                for column in columns:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table} WHERE \"{column}\" IS NULL;"))
                    null_count = result.scalar()
                    if null_count > 0:
                        raise Exception(f"Cannot apply NOT NULL constraints - NULL values in {table}.{column}")

            # Add constraints
            conn.execute(text(f"ALTER TABLE {self.schema_name}.customers ADD PRIMARY KEY (\"customerId\");"))
            conn.execute(text(f"""
                ALTER TABLE {self.schema_name}.complaints
                ADD CONSTRAINT fk_complaints_customer
                FOREIGN KEY (\"customerId\")
                REFERENCES {self.schema_name}.customers(\"customerId\");
            """))
            conn.execute(text(f"""
                ALTER TABLE {self.schema_name}.customers
                ALTER COLUMN "customerId" SET NOT NULL,
                ALTER COLUMN "number" SET NOT NULL,
                ALTER COLUMN "name" SET NOT NULL;
                
                ALTER TABLE {self.schema_name}.complaints
                ALTER COLUMN "customerId" SET NOT NULL,
                ALTER COLUMN "number" SET NOT NULL,
                ALTER COLUMN "logDate" SET NOT NULL;
            """))

        self.logger.info("Database constraints applied successfully!")

    def run_full_integration(self):
        
        self.logger.info("Starting data integration pipeline...")
        self.assign_customer_ids()
        self.reorder_table_columns()
        self.apply_constraints()
        self.logger.info("Data integration pipeline completed successfully!")