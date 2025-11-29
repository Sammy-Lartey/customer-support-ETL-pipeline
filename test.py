from sqlalchemy import text
from data_prep import CustomerSupportDataPrep
from data_cleaner import DataCleaner
from db_handler import DatabaseHandler
from schema_manager import SchemaManager
from data_int import DataIntegrator
from analytics import Analytics
from config import CONFIG

def main():
    print(" CUSTOMER SUPPORT PIPELINE STARTING ... ")
    
    # 1. LOAD AND CLEAN 
    print("\n PHASE 1: Data Loading and Cleaning ")
    prep = CustomerSupportDataPrep(CONFIG['path'], CONFIG['excel_file'])
    dfs = prep.load_excel_data()
    merged = prep.merge_sheets(exclude_sheets=CONFIG['exclude_sheets'])
    
    cleaner = DataCleaner(merged)
    df = cleaner.clean_columns()
    df = cleaner.validate_and_calculate_tat()
    
    db = DatabaseHandler(CONFIG['db_credentials'])
    db.write_dataframe(df, 'customer_support')
    print(" Phase 1 complete")
    
    # 2. SCHEMA SETUP 
    print("\n PHASE 2: Schema Setup ")
    schema_mgr = SchemaManager(db.engine, CONFIG['schema'])
    
    schema_mgr.setup_schema(df)
    print(" Phase 2 complete")
    
    # 3. DATA INTEGRATION 
    print("\n PHASE 3: Data Integration ")
    integrator = DataIntegrator(db.engine, CONFIG['schema'])
    
    # Debug: Check if profileIds are populated before starting
    with db.engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) as total, COUNT(\"profileId\") as populated FROM {CONFIG['schema']}.customers WHERE \"profileId\" IS NOT NULL"))
        stats = result.fetchone()
        print(f"DEBUG: ProfileId status - Total: {stats[0]}, Populated: {stats[1]}")
    
    integrator.assign_customer_ids()
    integrator.reorder_table_columns() 
    integrator.apply_constraints()
    
    print(" Phase 3 complete")
    
    # 4. ANALYTICS
    print("\n PHASE 4: Analytics")
    analytics = Analytics(db.engine, CONFIG['schema'])
    analytics.create_indexes()
    analytics.create_views()
    analytics.create_materialized_views()
    
    print(" Phase 4 complete")
    print("\n PIPELINE COMPLETED SUCCESSFULLY ")

if __name__ == "__main__":
    main()