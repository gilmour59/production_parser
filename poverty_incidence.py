import pandas as pd

def process_poverty_data(file_path, sheet_name, output_file, shapefile_list_csv):
    """
    Parses PSA Poverty Incidence Excel data, forward-fills province names,
    fixes spelling mismatches, and STRICTLY VALIDATES against the shapefile list.
    """
    print(f"📉 Loading Poverty Incidence data from '{file_path}' (Sheet: {sheet_name})...")
    
    try:
        # skiprows=6 starts exactly at row 7
        # usecols="B:D" targets Province, Municipality, and Poverty Incidence
        df = pd.read_excel(
            file_path, 
            sheet_name=sheet_name,
            skiprows=6, 
            usecols="B:D", 
            header=None
        ) 
    except Exception as e:
        print(f"❌ Error loading Excel file: {e}")
        return

    df.columns = ['Province', 'Municipality', 'Poverty_Incidence']
    
    # Forward-fill the Province column (PSA leaves it blank for subsequent rows)
    df['Province'] = df['Province'].ffill()

    target_provinces = ['Aklan', 'Antique', 'Capiz', 'Guimaras', 'Iloilo']

    # ---------------------------------------------------------
    # THE CORRECTION DICTIONARY
    # Format: "Name in PSA Data": "Exact Name in Shapefile"
    # ---------------------------------------------------------
    name_corrections = {
        "Iloilo City": "City of Iloilo",
        "Passi City": "City of Passi",
        "Roxas City": "City of Roxas",
        "Roxas": "City of Roxas", 
        "Ma-Ayon": "Ma-Ayon", 
        "Sapi-An": "Sapi-An",
        "Laua-An": "Laua-An",
        "Anini-Y": "Anini-Y",
        "San Jose De Buenavista": "San Jose",
        "Ma-ayon": "Ma-Ayon",
        "Sapi-an": "Sapi-An",
        "Laua-an": "Laua-An",
        "Anini-y": "Anini-Y",
        "Tibiao": "Tibiao",
        "Lauaan": "Laua-An",
        "Sapia-an": "Sapi-An"
    }

    parsed_data = []

    def clean_numeric(val):
        clean_val = pd.to_numeric(str(val).replace(',', '').strip(), errors='coerce')
        return 0 if pd.isna(clean_val) else clean_val

    for index, row in df.iterrows():
        prov = str(row['Province']).strip().title()
        loc = str(row['Municipality']).strip()
        
        # Skip empty rows or rows outside our target provinces
        if pd.isna(row['Municipality']) or loc == '' or loc.lower() == 'nan':
            continue
            
        if prov in target_provinces:
            # 1. Clean and Title Case the PSA name
            muni_name = loc
            
            # 2. Check if this name needs to be fixed for QGIS
            if muni_name in name_corrections:
                muni_name = name_corrections[muni_name]

            # 3. Save the formatted data
            parsed_data.append({
                'Province': prov,
                'Municipality': muni_name,
                'Poverty_Incidence': clean_numeric(row['Poverty_Incidence'])
            })

    df_clean = pd.DataFrame(parsed_data)

    # =========================================================
    # STRICT VALIDATION CHECK
    # =========================================================
    print(f"Validating municipalities against shapefile master list ('{shapefile_list_csv}')...")
    
    try:
        df_shp = pd.read_csv(shapefile_list_csv)
        shp_munis = set(df_shp['adm3_en'].astype(str).str.strip())
    except Exception as e:
        print(f"❌ Error loading Shapefile list '{shapefile_list_csv}': {e}")
        return

    psa_munis = set(df_clean['Municipality'])
    unmatched = psa_munis - shp_munis

    if len(unmatched) > 0:
        print("\n❌ ERROR: Mismatches found! The CSV will NOT be generated.")
        print("The following municipalities from the Poverty data don't exist in your shapefile:")
        for muni in unmatched:
            print(f" - {muni}")
        print("\nPlease add these to the 'name_corrections' dictionary in the script and run again.")
        return 
        
    # =========================================================
    # FINAL EXPORT
    # =========================================================
    df_clean.to_csv(output_file, index=False)
    
    print(f"\n✅ Success! Poverty Incidence data extracted, validated, and perfectly formatted.")
    print(f"📁 Saved ready-to-map data to: {output_file}")
    print(f"📊 Total municipalities processed: {len(df_clean)}")

if __name__ == "__main__":
    process_poverty_data(
        # Replace 'poverty_incidence.xlsx' with your exact file name if it differs
        file_path='poverty_incidence.xlsx', 
        sheet_name='2023_NoHUC_Maguindanao grouped', # Change to your target sheet name
        output_file='Cleaned_Poverty_Incidence.csv',
        shapefile_list_csv='Shapefile_Muni_List.csv' 
    )