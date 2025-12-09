from sqlalchemy import text
from data_prep import CustomerSupportDataPrep
from data_cleaner import DataCleaner
from db_handler import DatabaseHandler
from schema_manager import SchemaManager
from data_int import DataIntegrator
from analytics import Analytics
from config import CONFIG
from logger import get_logger, log_step_start, log_step_complete, log_df_info, log_error
import time

# Get the logger instance
logger = get_logger()

def main():
    try:
        logger.info("=" * 60)
        logger.info("CUSTOMER SUPPORT PIPELINE STARTING ... ")
        logger.info("=" * 60)

        total_start_time = time.time()
        
        # 1. LOAD AND CLEAN 
        log_step_start("PHASE 1: Data Loading and Cleaning")
        phase1_start = time.time()
        
        prep = CustomerSupportDataPrep(CONFIG['path'], CONFIG['excel_file'], logger)
        dfs = prep.load_excel_data()
        logger.info(f"Loaded {len(dfs)} Excel sheets successfully.")
        
        merged = prep.merge_sheets(exclude_sheets=CONFIG['exclude_sheets'])
        logger.info(f"Merged DataFrame shape: {merged.shape}")
        
        cleaner = DataCleaner(merged, logger)
        df = cleaner.clean_columns()
        df = cleaner.validate_and_calculate_tat()
        logger.info("Data cleaned and validated successfully.")
        
        db = DatabaseHandler(CONFIG['db_credentials'], logger)
        db.write_dataframe(df, 'customer_support')
        logger.info("Data written to database successfully.")
        
        phase1_duration = time.time() - phase1_start
        log_step_complete("PHASE 1: Data Loading and Cleaning", phase1_duration)
        
        # 2. SCHEMA SETUP 
        log_step_start("PHASE 2: Schema Setup")
        phase2_start = time.time()
        
        schema_mgr = SchemaManager(db.engine, CONFIG['schema'], logger)
        schema_mgr.setup_schema(df)
        logger.info("Schema setup completed successfully.")
        
        phase2_duration = time.time() - phase2_start
        log_step_complete("PHASE 2: Schema Setup", phase2_duration)

        # 3. DATA INTEGRATION 
        log_step_start("PHASE 3: Data Integration")
        phase3_start = time.time()
        
        integrator = DataIntegrator(db.engine, CONFIG['schema'], logger)
        
        # Debug: Check if profileIds are populated before starting
        with db.engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) as total, COUNT(\"profileId\") as populated FROM {CONFIG['schema']}.customers WHERE \"profileId\" IS NOT NULL"))
            stats = result.fetchone()
            logger.debug(f"ProfileId status - Total: {stats[0]}, Populated: {stats[1]}")
        
        integrator.assign_customer_ids()
        logger.info("Customer IDs assigned successfully.")
        integrator.reorder_table_columns() 
        logger.info("Table columns reordered successfully.")
        integrator.apply_constraints()
        logger.info("Constraints applied successfully.")
        
        phase3_duration = time.time() - phase3_start
        log_step_complete("PHASE 3: Data Integration", phase3_duration)
        
        # 4. ANALYTICS
        log_step_start("PHASE 4: Analytics")
        phase4_start = time.time()
        
        analytics = Analytics(db.engine, CONFIG['schema'], logger)
        analytics.create_indexes()
        logger.info("Indexes created successfully.")
        analytics.create_views()
        logger.info("Views created successfully.")
        analytics.create_materialized_views()
        logger.info("Materialized views created successfully.")
        
        phase4_duration = time.time() - phase4_start
        log_step_complete("PHASE 4: Analytics", phase4_duration)
        
        total_duration = time.time() - total_start_time
        logger.info("=" * 60)
        logger.info(f"PIPELINE COMPLETED SUCCESSFULLY in {total_duration:.2f} seconds")
        logger.info("=" * 60)
        
    except Exception as e:
        log_error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()