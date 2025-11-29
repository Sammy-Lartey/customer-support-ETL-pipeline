from libs import *


class CustomerSupportDataPrep:
    
    def __init__(self, path, excel_file):
        self.path = path
        self.excel_file = excel_file

    # load Excel data 
    def load_excel_data(self):

        full_path = os.path.join(self.path,self.excel_file)
        
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"Excel file not found at {full_path}")
        
        # Load each sheet into a DataFrame
        cs2025 = pd.ExcelFile(full_path)
        self.dataframes = {sheet_name: cs2025.parse(sheet_name) for sheet_name in cs2025.sheet_names}

        return self.dataframes
    
    #  Merge sheets
    def merge_sheets(self, exclude_sheets= None):

        if not self.dataframes:
            raise ValueError("No data loaded. Call load_excel_data() first.")
        
        # Exclude "Unresolved" sheet 
        if exclude_sheets is None:
            exclude_sheets = ["Unresolved"]

        # Exclude specified sheets
        dfs = {n: df for n, df in self.dataframes.items() if n not in exclude_sheets}
        monthly_dfs = list(dfs.values())

        # collect all unique columns
        all_columns = set()
        for df in monthly_dfs:
            all_columns.update(df.columns)

        # align missing columns
        for df in monthly_dfs:
            for col in all_columns:
                if col not in df.columns:
                    df[col] = np.nan
        
        print(f"Aligned all sheets to have {len(all_columns)} columns")

        merged_df = pd.concat(monthly_dfs, ignore_index=True)
        print(f"Merged DataFrame shape: {merged_df.shape}")
        return merged_df
    