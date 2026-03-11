import pandas as pd

def process_lowland_vegetable(file_path, sheet_name, output_file, shapefile_list_csv):
    """
    Parses DA Lowland Vegetable Excel data, extracts Area, Production, and Yield,
    fixes spelling mismatches, preserves exact Shapefile capitalization, 
    and automatically reverse-lookups the Province.
    """
    print(f"🥬 Loading Lowland Vegetable data from '{file_path}' (Sheet: '{sheet_name}')...")
    
    try:
        # skiprows=2 means data starts exactly at row 3
        # A=Municipality, B=Area, C=Production, D=Yield
        df = pd.read_excel(
            file_path, 
            sheet_name=sheet_name,
            skiprows=2, 
            usecols="A,B,C,D", 
            header=None
        ) 
    except Exception as e:
        print(f"❌ Error loading Excel file: {e}")
        return

    df.columns = ['Location', 'Area', 'Production', 'Yield']

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
        # Hyphenated corrections
        "MA-AYON": "Ma-Ayon",
        "SAPI-AN": "Sapi-An",
        "LAUA-AN": "Laua-An",
        "ANINI-Y": "Anini-Y",
        "TIBIAO": "Tibiao",
        "LAUAAN": "Laua-An"
    }

    # Load shapefile master list FIRST to get exact intended spellings AND their Provinces
    try:
        df_shp = pd.read_csv(shapefile_list_csv)
        shp_name_map = {str(name).strip().upper(): str(name).strip() for name in df_shp['adm3_en']}
        
        # Create a reverse-lookup dictionary for Provinces: e.g. "CITY OF ROXAS" -> "Capiz"
        shp_prov_map = {str(row['adm3_en']).strip().upper(): str(row['adm2_en']).strip().title() for index, row in df_shp.iterrows()}
    except Exception as e:
        print(f"❌ Error loading Shapefile list '{shapefile_list_csv}': {e}")
        return

    parsed_data = []

    def clean_numeric(val):
        clean_val = pd.to_numeric(str(val).replace(',', '').strip(), errors='coerce')
        return 0.0 if pd.isna(clean_val) else clean_val

    import re

    for index, row in df.iterrows():
        loc_original = str(row['Location']).strip()
        # Clean up any hidden double-spaces from the DA Excel file before matching
        loc_upper = re.sub(r'\s+', ' ', loc_original).upper()
        
        if pd.isna(row['Location']) or loc_original == '' or loc_original.lower() == 'nan':
            continue
        if loc_upper in ['LOCATION', 'PROVINCE/MUNICIPALITY', 'PROVINCE / MUNICIPALITY']:
            continue
            
        # Ignore regions/provinces if they accidentally appear as stray rows
        if loc_upper in ['AKLAN', 'ANTIQUE', 'CAPIZ', 'GUIMARAS', 'ILOILO', 'NEGROS OCCIDENTAL', 'WESTERN VISAYAS', 'REGION VI', 'TOTAL', 'GRAND TOTAL']:
            continue
            
        final_muni_name = loc_original # Default fallback
        
        # 1. Check if it needs a manual correction
        if loc_upper in name_corrections:
            final_muni_name = name_corrections[loc_upper]
        
        # 2. Check direct mapping to shapefile
        elif loc_upper in shp_name_map:
            final_muni_name = shp_name_map[loc_upper]

        # 3. Automatically Look up the Province from the Shapefile List!
        current_province = shp_prov_map.get(final_muni_name.upper(), "Unknown")

        parsed_data.append({
            'Province': current_province,
            'Municipality': final_muni_name,
            'Area': clean_numeric(row['Area']),
            'Production': clean_numeric(row['Production']),
            'Yield': clean_numeric(row['Yield'])
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
    
    print(f"\n✅ Success! Lowland Vegetable data extracted and perfectly formatted.")
    print(f"📁 Saved ready-to-map data to: {output_file}")
    print(f"📊 Total municipalities processed: {len(df_clean)}")

if __name__ == "__main__":
    process_lowland_vegetable(
        file_path='vegetables.xlsx', 
        sheet_name='vegetables', # Make sure this matches your exact sheet name
        output_file='Cleaned_Lowland_Vegetable_Panay_Guimaras.csv',
        shapefile_list_csv='Shapefile_Muni_List.csv' 
    )