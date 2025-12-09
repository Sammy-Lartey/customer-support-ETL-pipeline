from libs import *
from logger import get_logger, log_step_start, log_step_complete, log_df_info, log_db_ops, log_error, log_warning, log_debug_info

logger = get_logger()

class DataCleaner:

    def __init__(self, df: pd.DataFrame, logger):
        self.df = df.copy()
        self.logger = logger

    # Private Helper Methods(Helper Functions)

    # Clean name column
    def _clean_name(self, name):
        if pd.isna(name) or name.strip().lower() in ['nan', 'none', 'null', '']:
            return "Unknown"
        cleaned =  str(name).strip()
        self.logger.debug(f"Cleaned name: {cleaned}")
        return cleaned
    
    # lower camel case
    def _to_lower_camel(self, s: str):
        parts = s.strip().split(' ')
        return parts[0].lower() + ''.join(word.title() for word in parts[1:])
    
    # Clean region column
    def _correct_region(self, region_name, valid_regions, threshold=80):
        if pd.isna(region_name):
            return "Unknown"
        match, score, _ = process.extractOne(region_name, valid_regions, scorer=fuzz.token_set_ratio)
        return match if score >= threshold else "Unknown"

    # Format phone number column
    def _format_phone_number(self, phone):
        if pd.isna(phone):
            return np.nan
        
        phone_str = str(phone).strip()
        if phone_str.lower() in ['nan', 'none', 'null', '']:
            return np.nan
        
        digits = re.sub(r'\D', '', phone_str)
        if not digits or len(digits) < 9:
            return np.nan
        
        if digits.startswith('0') and len(digits) == 10:
            return '+233' + digits[1:]       
        elif digits.startswith('233') and len(digits) == 12:
            return '+' + digits
        elif len(digits) == 9:
            return '+233' + digits
        
        return np.nan
    
    # Title case
    def _title_case(self, text):
        return text.title() if isinstance(text, str) else text
    

    # Main Cleaning Logic
    def clean_columns(self):
        self.logger.info("Starting column cleaning process")

        df = self.df.copy()
        log_df_info("Original DataFrame", df)

        valid_regions = [
            "Ashanti Region", "Greater Accra Region", "Northern Region", "Volta Region",
            "Central Region", "Western Region", "Upper West Region", "Upper East Region",
            "Oti Region", "Savannah Region", "Bono East Region", "Western North Region",
            "Brong Ahafo Region", "North East Region", "Ahafo Region", "Eastern Region"
        ]

        # clean name column
        if 'name' in df.columns:
            self.logger.info("Cleaning name column")
            df['name'] = df['name'].apply(self._clean_name)
            self.logger.info("Name column cleaning completed")

        # normalize column names
        self.logger.info("Normalizing column names")
        original_columns = df.columns.tolist()
        df.columns = [self._to_lower_camel(col) for col in df.columns]
        df.columns = df.columns.str.replace(' ', '', regex=False)
        self.logger.debug(f"Normalized column names: {df.columns.tolist()}")

        # rename key columns
        rename_dict = {}
        if 'tat' in df.columns:
            rename_dict['tat'] = 'turnaroundTime'
        if 'dob' in df.columns:
            rename_dict['dob'] = 'dateOfBirth'
        if rename_dict:
            self.logger.info(f"Renaming columns: {rename_dict}")
            df = df.rename(columns=rename_dict)

        # convert types
        for col in ['turnaroundTime', 'dateOfBirth']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        if 'dateOfBirth' in df.columns:
            df['dateOfBirth'] = pd.to_datetime(df['dateOfBirth'], errors='coerce')
        
        # strip whitespaces
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.strip()

        # apply title case (excluding IDs and phone columns)
        exclude_cols = ['number', 'number2', 'branch', 'customerId', 'profileId']
        for col in [c for c in df.select_dtypes(include=['object']).columns if c not in exclude_cols]:
            df[col] = df[col].apply(self._title_case)

        #correct region names
        if 'region' in df.columns:
            df['region'] = df['region'].apply(lambda x: self._correct_region(x, valid_regions))

        # format phone numbers
        if 'number' in df.columns:
            df['number'] = df['number'].apply(self._format_phone_number)
        
        # Convert date columns
        for col in ['logDate', 'resolutionDate', 'dateOfBirth']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        
        df = df.drop_duplicates()
        self.logger.info(f"Cleaned data {df.shape[0] - df.drop_duplicates().shape[0]} duplicate rows")
        log_df_info("Cleaned DataFrame", df)

        self.df = df
        return df
    

    # TAT Validation
    def validate_and_calculate_tat(self):
        self.logger.info("Validating and calculating TAT")

        df = self.df.copy()

        needed = ['logDate', 'resolutionDate', 'turnaroundTime']
        if not all(col in df.columns for col in needed):
            error_msg = f"DataFrame must contain columns: {needed}"

            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        df['logDate'] = pd.to_datetime(df['logDate'], errors='coerce')
        df['resolutionDate'] = pd.to_datetime(df['resolutionDate'], errors='coerce')

        negative = df['turnaroundTime'] < 0
        swapped = (df['logDate'].notna() & df['resolutionDate'].notna() & 
                   (df['logDate'] > df['resolutionDate']))
        missing = df['turnaroundTime'].isna()

        print(f"Invalid TATs: {negative.sum()} negative, {swapped.sum()} swapped, {missing.sum()} missing")

        # fix swapped dates
        fix = negative & swapped
        if fix.any():
            temp = df.loc[fix, 'logDate'].copy()
            df.loc[fix, 'logDate'] = df.loc[fix, 'resolutionDate']
            df.loc[fix, 'resolutionDate'] = temp
            df.loc[fix, 'turnaroundTime'] = (df.loc[fix, 'resolutionDate'] - df.loc[fix, 'logDate']).dt.days

        
        # recalculate negative values with valid dates
        still_neg = (df['turnaroundTime'] < 0)
        valid_dates = (df['logDate'].notna() & df['resolutionDate'].notna() & (df['resolutionDate'] >= df['logDate']))
        recalc = still_neg & valid_dates
        if recalc.any():
            df.loc[recalc, 'turnaroundTime'] = (df.loc[recalc, 'resolutionDate'] - df.loc[recalc, 'logDate']).dt.days

        # compute missing TATs
        calc_missing_tat = valid_dates & df['turnaroundTime'].isna()
        if calc_missing_tat.any():
            df.loc[calc_missing_tat, 'turnaroundTime'] = (df.loc[calc_missing_tat, 'resolutionDate'] - df.loc[calc_missing_tat, 'logDate']).dt.days

        # stats
        negative = (df['turnaroundTime']<0).sum()
        missing = df['turnaroundTime'].isna().sum()
        valid = df['turnaroundTime'].notna().sum()
        self.logger.info(f"Valid TATs: {valid} (negative: {negative}, swapped: {swapped.sum()}, missing: {missing})")

        self.df = df
        return df

        
