import pandas as pd
import re

def process_coffee_data(file_path, sheet_name, output_file, shapefile_list_csv):
    """
    Parses DA Coffee data, uses the Shapefile as the absolute master list,
    and automatically injects 0.0 for any municipality the DA forgot to include.
    """
    print(f"☕ Loading Coffee data from '{file_path}' (Sheet: '{sheet_name}')...")
    
    # =========================================================
    # 1. LOAD THE SHAPEFILE MASTER LIST FIRST
    # =========================================================
    try:
        df_shp = pd.read_csv(shapefile_list_csv)
        shp_master = {}
        for index, row in df_shp.iterrows():
            muni_exact = str(row['adm3_en']).strip()
            prov_exact = str(row['adm2_en']).strip().title()
            
            # Use UPPERCASE as the universal matching key
            shp_master[muni_exact.upper()] = {
                'Municipality': muni_exact,
                'Province': prov_exact
            }
    except Exception as e:
        print(f"❌ Error loading Shapefile list '{shapefile_list_csv}': {e}")
        return

    # =========================================================
    # 2. LOAD THE DA COFFEE EXCEL DATA
    # =========================================================
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

    # Correction Dictionary (For fundamentally different names)
    name_corrections = {
        "ILOILO CITY": "CITY OF ILOILO",
        "PASSI CITY": "CITY OF PASSI",
        "ROXAS CITY": "CITY OF ROXAS",
        "ROXAS": "CITY OF ROXAS", 
        "SAN JOSE DE BUENAVISTA": "SAN JOSE",
        "VALDERAMA": "VALDERRAMA",
        # Hyphenated corrections mapped to UPPERCASE for the engine
        "MAAYON": "MA-AYON",
        "SAPIAN": "SAPI-AN",
        "LAUAAN": "LAUA-AN",
        "SAPIA-AN": "SAPI-AN",
        "PANIT-AN": "PANITAN",
    }

    def clean_numeric(val):
        clean_val = pd.to_numeric(str(val).replace(',', '').strip(), errors='coerce')
        return 0.0 if pd.isna(clean_val) else clean_val

    # =========================================================
    # 3. EXTRACT KNOWN DATA
    # =========================================================
    known_data = {}
    
    for index, row in df.iterrows():
        loc_original = str(row['Location']).strip()
        loc_upper = re.sub(r'\s+', ' ', loc_original).upper() # Converts "DIngle" to "DINGLE" automatically!
        
        # Skip empty rows or headers
        if pd.isna(row['Location']) or loc_original == '' or loc_original.lower() == 'nan':
            continue
        if loc_upper in ['LOCATION', 'MUNICIPALITY', 'PROVINCE / MUNICIPALITY', 'AREA', 'PRODUCTION', 'YIELD']:
            continue
            
        # Ignore regions/provinces if they appear as stray rows
        if loc_upper in ['AKLAN', 'ANTIQUE', 'CAPIZ', 'GUIMARAS', 'ILOILO', 'NEGROS OCCIDENTAL', 'WESTERN VISAYAS', 'REGION VI', 'TOTAL', 'GRAND TOTAL']:
            continue
            
        # Apply manual corrections if the DA used a completely different name
        if loc_upper in name_corrections:
            loc_upper = name_corrections[loc_upper]
        
        # Save the valid DA data to our temporary dictionary
        known_data[loc_upper] = {
            'Area': clean_numeric(row['Area']),
            'Production': clean_numeric(row['Production']),
            'Yield': clean_numeric(row['Yield'])
        }

    # =========================================================
    # 4. BUILD THE 100% COMPLETE DATASET (INJECTING ZEROS)
    # =========================================================
    parsed_data = []
    
    # We loop through your shapefile, NOT the DA file!
    for muni_upper, info in shp_master.items():
        if muni_upper in known_data:
            # We have data from the DA!
            parsed_data.append({
                'Province': info['Province'],
                'Municipality': info['Municipality'], # Exact Shapefile Case!
                'Area': known_data[muni_upper]['Area'],
                'Production': known_data[muni_upper]['Production'],
                'Yield': known_data[muni_upper]['Yield']
            })
        else:
            # The DA missed this town! Injecting Zeros.
            parsed_data.append({
                'Province': info['Province'],
                'Municipality': info['Municipality'], # Exact Shapefile Case!
                'Area': 0.0,
                'Production': 0.0,
                'Yield': 0.0
            })

    df_clean = pd.DataFrame(parsed_data)

    # =========================================================
    # 5. FINAL EXPORT
    # =========================================================
    df_clean.to_csv(output_file, index=False)
    
    print(f"\n✅ Success! Coffee data extracted.")
    print(f"🧩 Missing municipalities automatically detected and filled with 0.0")
    print(f"📊 Total municipalities processed: {len(df_clean)} (This should exactly match your shapefile!)")
    print(f"📁 Saved ready-to-map data to: {output_file}")

if __name__ == "__main__":
    process_coffee_data(
        file_path='COFFEE 2025 PRODUCTION_AREA_YIELD.xlsx', 
        sheet_name='REGION VI', # Based on your uploaded file name
        output_file='Cleaned_Coffee_Panay_Guimaras.csv',
        shapefile_list_csv='Shapefile_Muni_List.csv' 
    )