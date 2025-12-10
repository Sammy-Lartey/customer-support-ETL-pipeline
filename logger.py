import logging
import sys
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime
from config import CONFIG


class ETLPLogger:
    # Singleton pattern to ensure only one logger instance
    _instance = None

    # Create a new instance of the logger if it doesn't exist
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ETLPLogger, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    # Initialize logger
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True

        # create log directory 
        logs_dir = 'logs'
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)

        # set up log file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(logs_dir, f"etlp_pipeline_{timestamp}.log")

        # get log level from config
        log_level_str = CONFIG.get('Logging_level', 'INFO').upper()
        log_level = getattr(logging, log_level_str, logging.INFO)

        # create logger
        self.logger = logging.getLogger('ETLPLogger')
        self.logger.setLevel(log_level)

        # clear any existing handlers
        self.logger.handlers.clear()

        # create formatters
        detailed_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        simple_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # create file handler for detailed logging
        file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        

        # create console handler for simple logging
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(simple_formatter)
        
        # add handlers to logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    @classmethod
    def get_logger(cls):
        # Create an instance of ETLPLogger and return its logger attribute
        return cls().logger 
    
    # log start of a pipeline step
    def log_step_start(self, step_name, **kwargs):
        self.logger.info(f"STARTING: {step_name}")
        if kwargs:
            details = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
            self.logger.info(f"Step details: {details}")

    # log completion of a pipeline step
    def log_step_complete(self, step_name, duration = None):
        msg = f"COMPLETED: {step_name}"
        if duration:
            msg += f" (Duration: {duration:.2f}s)"
        self.logger.info(msg)

    # log DataFrame information
    def log_df_info(self, df_name, df):
        self.logger.info(f"{df_name}: Shape = {df.shape}")
        self.logger.debug(f"Columns: {df.columns.tolist()}")
        self.logger.debug(f"Sample:\n{df.head(3)}")
    
    # log database operations  
    def log_db_ops(self, ops, table, rows_affected = None):
        msg = f"{ops}: {table}"
        if rows_affected is not None:
            msg += f" | Rows affected: {rows_affected}"
        self.logger.info(msg)

    # log errors
    def log_error(self, error_msg, exc_info = True):
        self.logger.error(f"ERROR: {error_msg}", exc_info=exc_info)

    # log warnings
    def log_warning(self, warning_msg, data = None):
        self.logger.warning(f"WARNING: {warning_msg}")
        if data is not None:
            self.logger.warning(f"Data: {data}")
    
    # log debug information
    def log_debug_info(self, debug_msg, data = None):
        self.logger.debug(f"DEBUG: {debug_msg}")
        if data is not None:
            self.logger.debug(f"Data: {data}")

# Convenience Functions
# Get the logger instance
def get_logger():
    return ETLPLogger.get_logger()

# Log the start of a pipeline step
def log_step_start(step_name, **kwargs):
    ETLPLogger().log_step_start(step_name, **kwargs)

# Log the completion of a pipeline step
def log_step_complete(step_name, duration = None):
    ETLPLogger().log_step_complete(step_name, duration)

# Log DataFrame information
def log_df_info(df_name, df):
    ETLPLogger().log_df_info(df_name, df)

# Log database operations
def log_db_ops(ops, table, rows_affected = None):
    ETLPLogger().log_db_ops(ops, table, rows_affected)

# Log errors
def log_error(error_msg, exc_info = True):
    ETLPLogger().log_error(error_msg, exc_info)

# Log warnings
def log_warning(warning_msg, data = None):
    ETLPLogger().log_warning(warning_msg, data)

# Log debug information
def log_debug_info(debug_msg, data = None):
    ETLPLogger().log_debug_info(debug_msg, data)