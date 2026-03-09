import pandas as pd
import numpy as np

# 1. Define file parameters
file_path = 'rice.xlsx'
target_sheet = 'AOTODAY_Harvesting and Producti'

# 2. Load the data 
# usecols="B,BT,BU,BV" explicitly grabs Location, Area, Yield, and Production
df = pd.read_excel(
    file_path, 
    sheet_name=target_sheet, 
    skiprows=7, 
    usecols="B,BT,BU,BV", 
    header=None
) 

# Rename columns so they are easy to call in the loop
df.columns = ['Location', 'Area', 'Yield', 'Production']

# 3. Define target provinces
target_provinces = ['AKLAN', 'ANTIQUE', 'CAPIZ', 'GUIMARAS', 'ILOILO']

parsed_data = []
current_province = None

# Helper function to clean numeric data (removes commas, handles text/blanks)
def clean_numeric(val):
    clean_val = pd.to_numeric(str(val).replace(',', ''), errors='coerce')
    return 0 if pd.isna(clean_val) else clean_val

# 4. Loop through the rows 
for index, row in df.iterrows():
    loc = str(row['Location']).strip()
    
    # Skip completely empty rows
    if pd.isna(row['Location']) or loc == '' or loc.lower() == 'nan':
        continue
        
    # Check if the row is a new Province header
    if loc.upper() in target_provinces:
        current_province = loc.title()
        continue 
        
    # Ignore summary rows or Negros Occidental
    elif loc.upper() in ['NEGROS OCCIDENTAL', 'WESTERN VISAYAS', 'REGION VI', 'TOTAL']:
        if loc.upper() == 'NEGROS OCCIDENTAL':
             current_province = None 
        continue
        
    else:
        # If it's a municipality and we are under a target province, save it
        if current_province is not None:
            parsed_data.append({
                'Province': current_province,
                'Municipality': loc.title(),
                'Area': clean_numeric(row['Area']),
                'Yield': clean_numeric(row['Yield']),
                'Production': clean_numeric(row['Production'])
            })

# 5. Create final DataFrame and Export
df_clean = pd.DataFrame(parsed_data)
output_file = 'Cleaned_Rice_Data_Panay_Guimaras.csv'
df_clean.to_csv(output_file, index=False)

print(f"Success! Data parsed and saved to: {output_file}")
print("\nFirst 5 rows of output:")
print(df_clean.head())