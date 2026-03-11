import pandas as pd

def process_mango_cacao(file_path, sheet_name, output_file_cacao, output_file_mango, shapefile_list_csv):
    """
    Parses DA Mango & Cacao Excel data, extracts metrics, fixes spelling mismatches,
    preserves exact Shapefile capitalization (e.g. "City of Roxas").
    """
    print(f"🥭🍫 Loading Mango & Cacao data from '{file_path}' (Sheet: '{sheet_name}')...")
    
    try:
        df = pd.read_excel(
            file_path, 
            sheet_name=sheet_name,
            usecols="B,C,D,E,G,H,I", 
            header=None
        ) 
    except Exception as e:
        print(f"❌ Error loading Excel file: {e}")
        return

    df.columns = [
        'Location', 
        'Cacao_Area', 'Cacao_Production', 'Cacao_Yield', 
        'Mango_Area', 'Mango_Production', 'Mango_Yield'
    ]

    target_provinces = ['AKLAN', 'ANTIQUE', 'CAPIZ', 'GUIMARAS', 'ILOILO']

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

    # Load shapefile master list FIRST to get exact intended spellings
    try:
        df_shp = pd.read_csv(shapefile_list_csv)
        
        # Create a dictionary to map UPPERCASE names back to EXACT Shapefile names
        # Example: "CITY OF ROXAS" -> "City of Roxas"
        shp_name_map = {str(name).strip().upper(): str(name).strip() for name in df_shp['adm3_en']}
        shp_munis_upper = set(shp_name_map.keys())
        
    except Exception as e:
        print(f"❌ Error loading Shapefile list '{shapefile_list_csv}': {e}")
        return

    cacao_data = []
    mango_data = []
    current_province = None

    def clean_numeric(val):
        clean_val = pd.to_numeric(str(val).replace(',', '').strip(), errors='coerce')
        return 0.0 if pd.isna(clean_val) else clean_val

    for index, row in df.iterrows():
        loc_original = str(row['Location']).strip()
        loc_upper = loc_original.upper()
        
        if pd.isna(row['Location']) or loc_original == '' or loc_original.lower() == 'nan':
            continue
        if loc_upper in ['LOCATION', 'PROVINCE/MUNICIPALITY', 'CACAO', 'MANGO']:
            continue
            
        # Identify Province Headers
        if loc_upper in target_provinces:
            # We can format the province nicely, it doesn't affect the join
            current_province = loc_original.title() 
            continue 
            
        elif loc_upper in ['NEGROS OCCIDENTAL', 'WESTERN VISAYAS', 'REGION VI', 'TOTAL', 'GRAND TOTAL']:
            if loc_upper == 'NEGROS OCCIDENTAL':
                 current_province = None 
            continue
            
        else:
            if current_province is not None:
                final_muni_name = loc_original # Default fallback
                
                # 1. Check if it needs a manual correction (e.g. "ROXAS CITY" -> "City of Roxas")
                if loc_upper in name_corrections:
                    final_muni_name = name_corrections[loc_upper]
                
                # 2. If no manual correction, try to match it directly to the shapefile mapping
                # This perfectly preserves things like "Ma-ayon" or "City of Roxas"
                elif loc_upper in shp_name_map:
                    final_muni_name = shp_name_map[loc_upper]

                cacao_data.append({
                    'Province': current_province,
                    'Municipality': final_muni_name,
                    'Area': clean_numeric(row['Cacao_Area']),
                    'Production': clean_numeric(row['Cacao_Production']),
                    'Yield': clean_numeric(row['Cacao_Yield'])
                })
                
                mango_data.append({
                    'Province': current_province,
                    'Municipality': final_muni_name,
                    'Area': clean_numeric(row['Mango_Area']),
                    'Production': clean_numeric(row['Mango_Production']),
                    'Yield': clean_numeric(row['Mango_Yield'])
                })

    df_cacao = pd.DataFrame(cacao_data)
    df_mango = pd.DataFrame(mango_data)

    # =========================================================
    # STRICT VALIDATION CHECK
    # =========================================================
    print(f"Validating municipalities against shapefile master list...")
    
    # Check using exact spellings
    da_munis = set(df_cacao['Municipality'])
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
    df_cacao.to_csv(output_file_cacao, index=False)
    df_mango.to_csv(output_file_mango, index=False)
    
    print(f"\n✅ Success! Data extracted, separated, and exact shapefile capitalization preserved.")
    print(f"📁 Saved Cacao data to: {output_file_cacao}")
    print(f"📁 Saved Mango data to: {output_file_mango}")
    print(f"📊 Total municipalities processed: {len(df_cacao)}")

if __name__ == "__main__":
    process_mango_cacao(
        file_path='mango_cacao.xlsx', 
        sheet_name='Sheet1', 
        output_file_cacao='Cleaned_Cacao_Panay_Guimaras.csv',
        output_file_mango='Cleaned_Mango_Panay_Guimaras.csv',
        shapefile_list_csv='Shapefile_Muni_List.csv' 
    )