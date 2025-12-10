from libs import *
from logger import get_logger, log_step_start, log_step_complete, log_df_info, log_db_ops, log_error, log_warning, log_debug_info


class CustomerSupportDataPrep:
    
    def __init__(self, path, excel_file, logger):
        self.path = path
        self.excel_file = excel_file
        self.logger = logger

    # load Excel data 
    def load_excel_data(self):
        log_step_start("Loading Excel data", path=self.path, excel_file=self.excel_file)

        try:
            full_path = os.path.join(self.path, self.excel_file)
            
            # Check if file exists
            if not os.path.exists(full_path):
                error_msg = f"Excel file not found at {full_path}!"
                self.logger.error(error_msg)
                raise FileNotFoundError(error_msg)
            
            # Load each sheet into a DataFrame
            cs2025 = pd.ExcelFile(full_path)
            self.dataframes = {sheet_name: cs2025.parse(sheet_name) for sheet_name in cs2025.sheet_names}

            # Log information about loaded data
            self.logger.info(f"Successfully loaded {len(self.dataframes)} sheets from {self.excel_file}")
            for sheet_name, df in self.dataframes.items():
                self.logger.debug(f"Sheet '{sheet_name}': {df.shape[0]} rows, {df.shape[1]} columns")
            
            log_step_complete("Loading Excel data")
            return self.dataframes
            
        except Exception as e:
            self.logger.error(f"Failed to load Excel data: {str(e)}", exc_info=True)
            raise
    
    #  Merge sheets
    def merge_sheets(self, exclude_sheets=None):
        log_step_start("Merging sheets")

        if not hasattr(self, 'dataframes') or not self.dataframes:
            error_msg = "No data loaded!"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Exclude "Unresolved" sheet 
        if exclude_sheets is None:
            exclude_sheets = ["Unresolved"]
        self.logger.info(f"Excluding sheets: {exclude_sheets}")

        # Exclude specified sheets
        dfs = {n: df for n, df in self.dataframes.items() if n not in exclude_sheets}
        monthly_dfs = list(dfs.values())
        self.logger.info(f"Merging {len(monthly_dfs)} sheets")

        # collect all unique columns
        all_columns = set()
        for df in monthly_dfs:
            all_columns.update(df.columns)
        self.logger.info(f"Total unique columns across sheets: {len(all_columns)}") 

        # align missing columns
        for df in monthly_dfs:
            for col in all_columns:
                if col not in df.columns:
                    df[col] = np.nan
        
        self.logger.info(f"Aligned all sheets to have {len(all_columns)} columns")

        merged_df = pd.concat(monthly_dfs, ignore_index=True)
        self.logger.info(f"Merged DataFrame shape: {merged_df.shape}")
        
        log_step_complete("Merging sheets")
        return merged_df