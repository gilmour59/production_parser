import pandas as pd
import re

def process_swine_data(file_path, sheet_name, output_file, shapefile_list_csv):
    """
    Parses DA Swine Excel data, extracts Production and Inventory,
    forward-fills blank provinces, and preserves exact Shapefile capitalization.
    """
    print(f"🐖 Loading Swine data from '{file_path}' (Sheet: '{sheet_name}')...")
    
    try:
        # skiprows=1 means data starts exactly at row 2
        # A=Province, B=Municipality, F=Production, G=Inventory
        df = pd.read_excel(
            file_path, 
            sheet_name=sheet_name,
            skiprows=1, 
            usecols="A,B,F,G", 
            header=None
        ) 
    except Exception as e:
        print(f"❌ Error loading Excel file: {e}")
        return

    df.columns = ['Province', 'Municipality', 'Production', 'Inventory']
    
    # Forward-fill the Province column because the DA leaves it blank after the first entry
    df['Province'] = df['Province'].ffill()

    # ---------------------------------------------------------
    # THE CORRECTION DICTIONARY
    # Format: "UPPERCASE DA NAME": "Exact Name in Shapefile"
    # ---------------------------------------------------------
    name_corrections = {
        "ILOILO CITY": "City of Iloilo",
        "PASSI CITY": "City of Passi",
        "ROXAS CITY": "City of Roxas",
        "ROXAS": "City of Roxas", 
        "SAN JOSE DE BUENAVISTA": "San Jose",
        "VALDERAMA": "Valderrama", # Added to catch the DA's missing 'r'
        # Hyphenated corrections (Corrected to match Shapefile's Capitalization)
        "MA-AYON": "Ma-Ayon",
        "MAAYON": "Ma-Ayon",
        "SAPI-AN": "Sapi-An",
        "SAPIAN": "Sapi-An",
        "LAUA-AN": "Laua-An",
        "ANINI-Y": "Anini-Y",
        "TIBIAO": "Tibiao",
        "LAUAAN": "Laua-An",
    }

    # Load shapefile master list FIRST to get exact intended spellings
    try:
        df_shp = pd.read_csv(shapefile_list_csv)
        shp_name_map = {str(name).strip().upper(): str(name).strip() for name in df_shp['adm3_en']}
    except Exception as e:
        print(f"❌ Error loading Shapefile list '{shapefile_list_csv}': {e}")
        return

    parsed_data = []

    def clean_numeric(val):
        clean_val = pd.to_numeric(str(val).replace(',', '').strip(), errors='coerce')
        return 0.0 if pd.isna(clean_val) else clean_val

    for index, row in df.iterrows():
        loc_original = str(row['Municipality']).strip()
        # Clean up any hidden double-spaces from the DA Excel file before matching
        loc_upper = re.sub(r'\s+', ' ', loc_original).upper()
        
        # Skip completely empty rows
        if pd.isna(row['Municipality']) or loc_original == '' or loc_original.lower() == 'nan':
            continue
            
        prov_original = str(row['Province']).strip().title()
        final_muni_name = loc_original # Default fallback
        
        # 1. Check if it needs a manual correction
        if loc_upper in name_corrections:
            final_muni_name = name_corrections[loc_upper]
        
        # 2. Check direct mapping to shapefile
        elif loc_upper in shp_name_map:
            final_muni_name = shp_name_map[loc_upper]

        parsed_data.append({
            'Province': prov_original,
            'Municipality': final_muni_name,
            'Production': clean_numeric(row['Production']),
            'Inventory': clean_numeric(row['Inventory'])
        })

    df_clean = pd.DataFrame(parsed_data)

    # =========================================================
    # STRICT VALIDATION CHECK
    # =========================================================
    print(f"Validating municipalities against shapefile master list...")
    
    da_munis = set(df_clean['Municipality'])
    shp_munis_exact = set(df_shp['adm3_en'].astype(str).str.strip())
    
    unmatched = da_munis - shp_munis_exact

    if len(unmatched) > 0:
        print("\n❌ ERROR: Mismatches found! The CSV will NOT be generated.")
        print("The following municipalities from the dataset don't exactly match your shapefile:")
        for muni in unmatched:
            print(f" - {muni}")
        print("\nPlease add these to the 'name_corrections' dictionary in UPPERCASE.")
        return 
        
    # =========================================================
    # FINAL EXPORT
    # =========================================================
    df_clean.to_csv(output_file, index=False)
    
    print(f"\n✅ Success! Swine data extracted, forward-filled, and perfectly formatted.")
    print(f"📁 Saved ready-to-map data to: {output_file}")
    print(f"📊 Total municipalities processed: {len(df_clean)}")

if __name__ == "__main__":
    process_swine_data(
        file_path='swine.xlsx', 
        sheet_name='Sheet1', # Ensure this matches your tab name
        output_file='Cleaned_Swine_Panay_Guimaras.csv',
        shapefile_list_csv='Shapefile_Muni_List.csv' 
    )