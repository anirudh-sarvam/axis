import pandas as pd
import phonenumbers
from phonenumbers import NumberParseException
from typing import Optional
import os


def validate_phone_number(phone: str, region: str = "IN") -> Optional[str]:
    if pd.isna(phone) or str(phone).strip() == "":
        return None
    
    try:
        phone_str = str(phone).strip()
        
        # Handle common formats: 91XXXXXXXXXX, +91XXXXXXXXXX, etc.
        # Remove leading 91 if present and number is longer than 10 digits
        if len(phone_str) > 10:
            if phone_str.startswith('91'):
                phone_str = phone_str[2:]
            elif phone_str.startswith('+91'):
                phone_str = phone_str[3:]
        
        # Take only first 10 digits if still longer
        if len(phone_str) > 10:
            phone_str = phone_str[:10]
        
        parsed_number = phonenumbers.parse(phone_str, region)
        if phonenumbers.is_valid_number(parsed_number):
            return str(parsed_number.national_number)
        else:
            return None
    except NumberParseException:
        return None


def _detect_file_type(file_path: str) -> str:
    """Detect file type by reading first few bytes."""
    try:
        with open(file_path, 'rb') as f:
            header = f.read(16)
            
        # Check for Excel signatures
        if header[:2] == b'PK':  # ZIP signature (xlsx, xlsm)
            return 'excel'
        elif header[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':  # OLE2 (old xls)
            return 'excel_old'
        # Check for HTML/XML
        elif header.startswith(b'<'):
            return 'html/xml'
        # Check for text
        elif header.startswith(b'\xff\xfe') or header.startswith(b'\xfe\xff'):
            return 'unicode_text'
        else:
            # Try to decode as text
            try:
                header.decode('utf-8')
                return 'text'
            except:
                return 'binary'
    except Exception:
        return 'unknown'


def _try_read_csv_multiple_encodings(
    file_path: str, excel_error: Optional[Exception]
):
    """Try reading CSV with multiple encodings."""
    encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 'utf-16']
    
    # Detect file type
    file_type = _detect_file_type(file_path)
    print(f"Detected file type: {file_type}")
    
    # If it's HTML/XML, try reading as HTML table
    if file_type == 'html/xml':
        try:
            print("Trying to read as HTML table...")
            dfs = pd.read_html(file_path)
            if dfs:
                print("Successfully read as HTML table")
                return dfs[0]  # Return first table
        except Exception as html_error:
            print(f"HTML read failed: {html_error}")
    
    # Try CSV with different encodings
    for encoding in encodings:
        try:
            print(f"Trying CSV with encoding: {encoding}")
            df = pd.read_csv(
                file_path, encoding=encoding, low_memory=False, sep=None,
                engine='python'
            )
            print(f"Successfully read file with encoding: {encoding}")
            return df
        except UnicodeDecodeError:
            continue
        except Exception as e:
            # If it's not an encoding error, try next encoding
            if 'codec' not in str(e).lower() and 'decode' not in str(e).lower():
                # Might be a parsing error, but encoding worked
                print(f"Encoding {encoding} worked but parsing failed: {e}")
                # Try with different separators
                for sep in [',', ';', '\t', '|']:
                    try:
                        df = pd.read_csv(
                            file_path, encoding=encoding,
                            sep=sep, low_memory=False
                        )
                        print(
                            f"Successfully read with encoding {encoding} "
                            f"and separator '{sep}'"
                        )
                        return df
                    except:
                        continue
                continue
    
    # If all encodings fail, raise error with file type info
    error_msg = (
        f"Could not read file as Excel or CSV with any encoding.\n"
        f"Detected file type: {file_type}"
    )
    if excel_error:
        error_msg += f"\nExcel error: {excel_error}"
    error_msg += (
        f"\nTried encodings: {', '.join(encodings)}\n"
        f"Please verify the file format. The file might be:\n"
        f"- Corrupted\n"
        f"- In a different format (HTML, XML, binary, etc.)\n"
        f"- Password protected\n"
        f"- Not actually an Excel/CSV file"
    )
    raise ValueError(error_msg)


def convert_to_rounded_int(value) -> int:
    if pd.isna(value):
        return 0
    
    try:
        return int(round(float(value)))
    except (ValueError, TypeError):
        return 0


def transform_cc_allocation_file(
    input_file_path: str,
    output_file_path: Optional[str] = None,
    password: Optional[str] = None,
    bank_name: str = ""
) -> pd.DataFrame:
    # Read input file
    # Check file extension and read accordingly
    if input_file_path.endswith('.xlsx'):
        try:
            if password:
                # For password-protected files, use msoffcrypto-tool
                try:
                    import msoffcrypto
                    import io
                    print("Attempting to read password-protected Excel file...")
                    decrypted = io.BytesIO()
                    with open(input_file_path, 'rb') as file:
                        office_file = msoffcrypto.OfficeFile(file)
                        office_file.load_key(password=password)
                        office_file.decrypt(decrypted)
                    decrypted.seek(0)
                    # Try reading first sheet, or all sheets if multiple
                    try:
                        excel_file = pd.ExcelFile(decrypted, engine='openpyxl')
                        if len(excel_file.sheet_names) > 1:
                            print(f"Multiple sheets found: {excel_file.sheet_names}")
                            print(f"Reading first sheet: {excel_file.sheet_names[0]}")
                        df = pd.read_excel(excel_file, sheet_name=0, engine='openpyxl')
                    except:
                        df = pd.read_excel(decrypted, engine='openpyxl')
                    print("Successfully read password-protected file")
                except ImportError:
                    raise ImportError(
                        "msoffcrypto-tool not installed. "
                        "Install with: pip install msoffcrypto-tool"
                    )
                except Exception as crypto_error:
                    print(
                        f"Password decryption failed: {crypto_error}\n"
                        f"Trying without password..."
                    )
                    df = pd.read_excel(input_file_path, engine='openpyxl')
            else:
                # Try reading first sheet, or all sheets if multiple
                try:
                    excel_file = pd.ExcelFile(input_file_path, engine='openpyxl')
                    if len(excel_file.sheet_names) > 1:
                        print(f"Multiple sheets found: {excel_file.sheet_names}")
                        print(f"Reading first sheet: {excel_file.sheet_names[0]}")
                    df = pd.read_excel(excel_file, sheet_name=0, engine='openpyxl')
                except:
                    df = pd.read_excel(input_file_path, engine='openpyxl')
        except Exception as e:
            # File might be corrupted or not actually Excel format
            print(
                f"Warning: Failed to read as Excel file: {e}\n"
                f"Trying CSV format with different encodings..."
            )
            df = _try_read_csv_multiple_encodings(input_file_path, e)
    elif input_file_path.endswith('.xls'):
        try:
            # Try reading first sheet if multiple sheets exist
            try:
                excel_file = pd.ExcelFile(input_file_path, engine='xlrd')
                if len(excel_file.sheet_names) > 1:
                    print(f"Multiple sheets found: {excel_file.sheet_names}")
                    print(f"Reading first sheet: {excel_file.sheet_names[0]}")
                df = pd.read_excel(excel_file, sheet_name=0, engine='xlrd')
            except:
                df = pd.read_excel(input_file_path, engine='xlrd')
        except ImportError:
            raise ImportError(
                "xlrd not installed. Install with: pip install xlrd"
            )
        except Exception as e:
            print(
                f"Warning: xlrd failed ({e}), trying CSV format..."
            )
            df = _try_read_csv_multiple_encodings(input_file_path, e)
    else:
        # Assume CSV for other extensions
        df = _try_read_csv_multiple_encodings(input_file_path, None)
    
    print(f"Loaded {len(df)} records from {input_file_path}")
    print(f"Original columns: {list(df.columns)}")
    
    # Strip whitespace from column names
    df.columns = df.columns.str.strip()
    
    # Drop unnamed columns that might exist
    unnamed_cols = [col for col in df.columns if str(col).startswith('Unnamed')]
    if unnamed_cols:
        df = df.drop(columns=unnamed_cols)
        print(f"Dropped {len(unnamed_cols)} unnamed columns")
    
    # Remove completely empty rows
    initial_len = len(df)
    df = df.dropna(how='all')
    if len(df) < initial_len:
        print(f"Removed {initial_len - len(df)} completely empty rows")
    
    # Remove rows where all key columns are empty (if they exist)
    key_cols = ['user_identifier', 'ACCOUNT_NO', '#ACCOUNT_NO', 'ACCNO', 'ACCNO2']
    existing_key_cols = [col for col in key_cols if col in df.columns]
    if existing_key_cols:
        before_len = len(df)
        df = df.dropna(subset=existing_key_cols, how='all')
        if len(df) < before_len:
            print(f"Removed {before_len - len(df)} rows with empty account numbers")
    
    # Define column mappings (original -> standard) - case-insensitive matching
    # First, create a case-insensitive mapping
    column_mappings = {
        '#ACCOUNT_NO': 'user_identifier',
        'ACCOUNT_NO': 'user_identifier',
        'Account_No': 'user_identifier',
        'Account No': 'user_identifier',
        'ACCNO': 'user_identifier',
        'ACCNO2': 'user_identifier',
        'LASTSTMTBAL': 'total_amount_due',
        'Last Statement Balance': 'total_amount_due',
        'MIN_AMT_DUE': 'min_amount_due',
        'Min_Amt_Due': 'min_amount_due',
        'Min Amt Due': 'min_amount_due',
        'MINIMUM_AMOUNT_DUE': 'min_amount_due',
        'Minimum Amount Due': 'min_amount_due',
        'CURRENT_MOBILE': 'user_mobile_number',
        'Current_Mobile': 'user_mobile_number',
        'Current Mobile': 'user_mobile_number',
        'MOBILE': 'user_mobile_number',
        'MOBILE_NO': 'user_mobile_number',
        'Mobile No': 'user_mobile_number',
        'PHONE': 'user_mobile_number',
        'Phone': 'user_mobile_number',
        'CONTACT_NO': 'user_mobile_number',
        'Contact No': 'user_mobile_number',
        'RISK_SEGMENT': 'risk_segment',
        'Risk_Segment': 'risk_segment',
        'RISK SEGMENT': 'risk_segment',
        'Risk Segment': 'risk_segment',
        'RISK_SEG': 'risk_segment',
        'Risk_Seg': 'risk_segment',
        'Risk Seg': 'risk_segment',
        'Risk': 'risk_segment',
        'RISK': 'risk_segment',
        'risk': 'risk_segment',
    }

    # Create case-insensitive mapping
    rename_map = {}
    df_cols_lower = {col.lower(): col for col in df.columns}
    
    for old_col, new_col in column_mappings.items():
        old_col_lower = old_col.lower()
        if old_col in df.columns:
            # Exact match
            rename_map[old_col] = new_col
        elif old_col_lower in df_cols_lower:
            # Case-insensitive match
            actual_col = df_cols_lower[old_col_lower]
            rename_map[actual_col] = new_col

    df.rename(columns=rename_map, inplace=True)

    # Validate required columns - try case-insensitive matching first
    required_cols = ['user_identifier', 'total_amount_due', 'min_amount_due']
    missing_cols = []
    
    for req_col in required_cols:
        if req_col not in df.columns:
            # Try case-insensitive match
            found = False
            for col in df.columns:
                if col.lower() == req_col.lower():
                    df.rename(columns={col: req_col}, inplace=True)
                    found = True
                    break
            if not found:
                missing_cols.append(req_col)
    
    if missing_cols:
        print(f"Available columns: {list(df.columns)}")
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Convert amounts to rounded integers
    df['total_amount_due'] = df['total_amount_due'].apply(convert_to_rounded_int)
    df['min_amount_due'] = df['min_amount_due'].apply(convert_to_rounded_int)

    # Validate phone numbers
    if 'user_mobile_number' not in df.columns:
        raise ValueError(
            f"No phone column could be mapped to user_mobile_number. "
            f"Available columns: {list(df.columns)}"
        )
    df['user_mobile_number'] = df['user_mobile_number'].astype(str)
    df['user_mobile_number'] = df['user_mobile_number'].apply(validate_phone_number)

    valid_phones = df['user_mobile_number'].notna().sum()
    print(f"Valid phone numbers: {valid_phones}/{len(df)} ({valid_phones/len(df)*100:.1f}%)")

    # Convert user_identifier to string
    df['user_identifier'] = df['user_identifier'].astype(str).str.strip()

    # Check and log risk_segment column if present
    if 'risk_segment' not in df.columns:
        raise ValueError(
            f"No risk_segment column found after column mapping. "
            f"Available columns: {list(df.columns)}"
        )
    risk_seg_count = df['risk_segment'].notna().sum()
    print(f"\nRisk segment column found: {risk_seg_count}/{len(df)} records have risk_segment values")
    if risk_seg_count > 0:
        print(f"Risk segment distribution:")
        print(df['risk_segment'].value_counts(dropna=False).head(10))

    # Add bank_name column based on CITI_MIGR_FLAG if present, otherwise use provided bank_name
    citi_flag_col = None
    for col in df.columns:
        if col.strip().upper() == 'CITI_MIGR_FLAG':
            citi_flag_col = col
            break

    if citi_flag_col:
        df['bank_name'] = df[citi_flag_col].astype(str).str.strip().str.upper().map({
            'Y': 'citi',
            'N': 'axis'
        })
        unmapped = df['bank_name'].isna().sum()
        if unmapped > 0:
            print(f"Warning: {unmapped} rows have unknown CITI_MIGR_FLAG values, defaulting to 'axis'")
            df['bank_name'] = df['bank_name'].fillna('axis')
        print(f"\nBank name distribution (from {citi_flag_col}):")
        print(df['bank_name'].value_counts())
    else:
        raise ValueError(
            "CITI_MIGR_FLAG column not found in the input file. "
            f"Available columns: {list(df.columns)}"
        )

    print(f"\nFinal record count: {len(df)}")
    print(f"Final columns: {list(df.columns)}")
    print(f"\nSample data:\n{df.head()}")

    # Save if output path provided
    if output_file_path:
        df.to_csv(output_file_path, index=False)
        print(f"Saved transformed file to: {output_file_path}")

    return df


def generate_risk_cohorts(df: pd.DataFrame, base_output_path: str):
    """
    Generate three cohort files based on risk_segment:
    1. L cohort (Low risk - e.g., "1. L")
    2. M cohort (Medium risk - e.g., "2. M")
    3. High and above cohort (H, H2, H2 or C<200k, etc. - e.g., "3. H", "4. H2", "4. H2 or C<200k")
    
    Args:
        df: Transformed DataFrame
        base_output_path: Base path for output files (without extension)
    """
    # Find risk_segment column (case-insensitive)
    risk_seg_col = None
    
    # First check if it was already renamed to risk_segment
    if 'risk_segment' in df.columns:
        risk_seg_col = 'risk_segment'
    else:
        # Look for risk segment column with various naming patterns
        possible_patterns = [
            'risk_segment', 'risk segment', 'risk_seg', 'riskseg', 
            'risk seg', 'risksegment', 'risk', 'RISK', 'Risk'
        ]
        
        for col in df.columns:
            col_lower = col.lower().strip()
            # Check exact matches first (case-insensitive)
            if col_lower in [p.lower() for p in possible_patterns]:
                risk_seg_col = col
                break
            # Then check if column contains both 'risk' and 'seg'
            elif 'risk' in col_lower and 'seg' in col_lower:
                risk_seg_col = col
                break
            # Also check for just 'risk' (exact match, case-insensitive)
            elif col_lower == 'risk':
                risk_seg_col = col
                break
    
    if risk_seg_col is None:
        raise ValueError(
            f"No risk_segment column found. Available columns: {list(df.columns)}"
        )
    
    # Convert risk_segment to string for filtering, handling NaN values
    # Replace NaN/empty with empty string for string operations
    df[risk_seg_col] = df[risk_seg_col].fillna('').astype(str).str.strip()
    
    # Normalize risk values to uppercase for consistent matching
    risk_values = df[risk_seg_col].str.upper()
    
    # Show unique risk values found
    unique_risks = df[risk_seg_col].value_counts(dropna=False).head(20)
    print(f"\nUnique risk values found (top 20):")
    print(unique_risks)
    
    # Create L cohort (Low risk)
    # Patterns: "1. L", "L", "LOW", "low", etc. (but not containing "H" or "HIGH" or "CRITICAL")
    l_mask = (
        risk_values.str.contains(r'\.\s*L\b', case=False, na=False, regex=True) |
        risk_values.str.contains(r'\bL\b', case=False, na=False, regex=True) |
        (risk_values == 'L') |
        risk_values.str.contains('LOW', case=False, na=False)
    ) & ~risk_values.str.contains('HIGH', case=False, na=False) & ~risk_values.str.contains('CRITICAL', case=False, na=False) & ~risk_values.str.contains('H', case=False, na=False)
    df_l = df[l_mask].copy()
    
    # Create M cohort (Medium risk)
    # Patterns: "2. M", "M", "MEDIUM", "medium", etc. (but not containing "H", "HIGH", "CRITICAL", "L", or "LOW")
    m_mask = (
        risk_values.str.contains(r'\.\s*M\b', case=False, na=False, regex=True) |
        risk_values.str.contains(r'\bM\b', case=False, na=False, regex=True) |
        (risk_values == 'M') |
        risk_values.str.contains('MEDIUM', case=False, na=False)
    ) & ~risk_values.str.contains('HIGH', case=False, na=False) & ~risk_values.str.contains('CRITICAL', case=False, na=False) & ~risk_values.str.contains('H', case=False, na=False) & ~risk_values.str.contains('LOW', case=False, na=False) & ~risk_values.str.contains('L', case=False, na=False)
    df_m = df[m_mask].copy()
    
    # Create High and above cohort (H, H2, High, Critical, etc.)
    # HIGH and CRITICAL are grouped together in the same H cohort
    # Patterns: "3. H", "4. H2", "HIGH", "CRITICAL", "critical", "high", etc.
    h_mask = (
        risk_values.str.contains('CRITICAL', case=False, na=False) |  # Critical risk
        risk_values.str.contains('HIGH', case=False, na=False) |      # High risk (same cohort as critical)
        (risk_values.str.contains('H', case=False, na=False) &        # H, H2, etc. (but not HIGH which is already matched)
         ~risk_values.str.contains('HIGH', case=False, na=False))
    )
    df_h = df[h_mask].copy()
    
    # Assign records not in any cohort to Low (L) cohort
    all_cohort_mask = l_mask | m_mask | h_mask
    not_in_cohort = df[~all_cohort_mask]
    if len(not_in_cohort) > 0:
        print(f"\nInfo: {len(not_in_cohort)} records not assigned to any cohort")
        print(f"Sample risk_segment values: {not_in_cohort[risk_seg_col].unique()[:10]}")
        print(f"Assigning these records to Low (L) cohort...")
        # Add unassigned records to L cohort
        df_l = pd.concat([df_l, not_in_cohort], ignore_index=True)
        print(f"Added {len(not_in_cohort)} records to L cohort")
    
    # Generate output paths
    base_name = base_output_path.rsplit('.', 1)[0] if '.' in base_output_path else base_output_path
    ext = base_output_path.rsplit('.', 1)[1] if '.' in base_output_path else 'csv'
    
    cohorts = {'L': df_l, 'M': df_m, 'H': df_h}
    
    # Split each cohort by bank_name (axis/citi) if the column exists
    has_bank_name = 'bank_name' in df.columns
    if has_bank_name:
        bank_names = df['bank_name'].unique()
    
    print("\n" + "="*80)
    print("RISK SEGMENT COHORT GENERATION SUMMARY")
    print("="*80)
    print(f"Total records: {len(df):,}")
    
    for cohort_name, cohort_df in cohorts.items():
        if has_bank_name:
            for bank in sorted(bank_names):
                bank_df = cohort_df[cohort_df['bank_name'] == bank].copy()
                if len(bank_df) == 0:
                    print(f"\n{bank.upper()} - {cohort_name} Cohort: 0 records (skipped)")
                    continue
                output_path = f"{base_name}_{bank}_cohort_{cohort_name}.{ext}"
                bank_df.to_csv(output_path, index=False)
                print(f"\n{bank.upper()} - {cohort_name} Cohort: {len(bank_df):,} records")
                print(f"  Saved to: {output_path}")
                print(f"  Risk values: {bank_df[risk_seg_col].value_counts(dropna=False).head(5).to_dict()}")
        else:
            if len(cohort_df) == 0:
                print(f"\n{cohort_name} Cohort: 0 records (skipped)")
                continue
            output_path = f"{base_name}_cohort_{cohort_name}.{ext}"
            cohort_df.to_csv(output_path, index=False)
            print(f"\n{cohort_name} Cohort: {len(cohort_df):,} records")
            print(f"  Saved to: {output_path}")
            print(f"  Risk values: {cohort_df[risk_seg_col].value_counts(dropna=False).head(5).to_dict()}")


def main():
    input_file = r"D:\Coding\sarvam\stop_calls\new_allocation_files\mar_cc\Post Due Date Cards Cycle 19 Allocation Mar'2026 (Sarvam).xlsx"
    password = "1"  # Password for protected Excel files
    bank_name = "axis"  # Bank name to fill in the bank_name column (leave blank to skip adding the column)
    
    if not os.path.exists(input_file):
        print(f"Error: Input file not found: {input_file}")
        return

    output_file = f"{input_file.split('.')[0]}_transformed.csv"

    df = transform_cc_allocation_file(input_file, output_file, password=password, bank_name=bank_name)
    
    print("\n=== Summary ===")
    print(f"Total records: {len(df)}")
    print(f"Total outstanding sum: Rs.{df['total_amount_due'].sum():,.0f}")
    print(f"Min amount due sum: Rs.{df['min_amount_due'].sum():,.0f}")
    
    print("\n=== Amount Distribution ===")
    print(df['total_amount_due'].describe())
    
    # Generate risk cohorts if risk_segment column exists
    generate_risk_cohorts(df, output_file)


if __name__ == "__main__":
    main()
