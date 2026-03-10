import pandas as pd
import os

def process_corn_data(file_path, sheet_name, output_file, shapefile_list_csv):
    """
    Parses DA Corn Excel reports, extracts Area, Yield, and Production for Yellow Corn,
    fixes spelling mismatches, and STRICTLY VALIDATES against the shapefile list.
    """
    print(f"🌽 Loading Yellow Corn data from '{file_path}', sheet: '{sheet_name}'...")
    
    try:
        # skiprows=9 means the data starts exactly at Row 10
        # usecols="B,BE,BF,BG" targets Location and Yellow Corn Grand Totals
        df = pd.read_excel(
            file_path, 
            sheet_name=sheet_name, 
            skiprows=9, 
            usecols="B,BE,BF,BG", 
            header=None
        ) 
    except Exception as e:
        print(f"❌ Error loading Excel file: {e}")
        return

    df.columns = ['Location', 'Area', 'Yield', 'Production']
    target_provinces = ['AKLAN', 'ANTIQUE', 'CAPIZ', 'GUIMARAS', 'ILOILO']

    # ---------------------------------------------------------
    # THE CORRECTION DICTIONARY
    # Format: "Name in DA Data": "Exact Name in Shapefile"
    # ---------------------------------------------------------
    name_corrections = {
        "Iloilo City": "City Of Iloilo",
        "Passi City": "City Of Passi",
        "Roxas City": "City Of Roxas",
        "Roxas": "City Of Roxas", 
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
    current_province = None

    def clean_numeric(val):
        clean_val = pd.to_numeric(str(val).replace(',', '').strip(), errors='coerce')
        return 0 if pd.isna(clean_val) else clean_val

    for index, row in df.iterrows():
        loc = str(row['Location']).strip()
        
        if pd.isna(row['Location']) or loc == '' or loc.lower() == 'nan':
            continue
            
        if loc.upper() in target_provinces:
            current_province = loc.title()
            continue 
            
        elif loc.upper() in ['NEGROS OCCIDENTAL', 'WESTERN VISAYAS', 'REGION VI', 'TOTAL', 'GRAND TOTAL']:
            if loc.upper() == 'NEGROS OCCIDENTAL':
                 current_province = None 
            continue
            
        else:
            if current_province is not None:
                # 1. Clean and Title Case the DA name
                muni_name = loc.title()
                
                # 2. Check if this name needs to be fixed for QGIS
                if muni_name in name_corrections:
                    muni_name = name_corrections[muni_name]

                # 3. Save the formatted data
                parsed_data.append({
                    'Province': current_province,
                    'Municipality': muni_name,
                    'Area': clean_numeric(row['Area']),
                    'Yield': clean_numeric(row['Yield']),
                    'Production': clean_numeric(row['Production'])
                })

    df_clean = pd.DataFrame(parsed_data)

    # =========================================================
    # STRICT VALIDATION CHECK
    # =========================================================
    print(f"Validating municipalities against shapefile master list ('{shapefile_list_csv}')...")
    
    try:
        df_shp = pd.read_csv(shapefile_list_csv)
        shp_munis = set(df_shp['adm3_en'].astype(str).str.strip().str.title())
    except Exception as e:
        print(f"❌ Error loading Shapefile list '{shapefile_list_csv}': {e}")
        return

    da_munis = set(df_clean['Municipality'])
    unmatched = da_munis - shp_munis

    if len(unmatched) > 0:
        print("\n❌ ERROR: Mismatches found! The CSV will NOT be generated.")
        print("The following municipalities from the Corn data don't exist in your shapefile:")
        for muni in unmatched:
            print(f" - {muni}")
        print("\nPlease add these to the 'name_corrections' dictionary in the script and run again.")
        return 
        
    # =========================================================
    # FINAL EXPORT
    # =========================================================
    df_clean.to_csv(output_file, index=False)
    
    print(f"\n✅ Success! Corn data extracted, validated, and perfectly formatted.")
    print(f"📁 Saved ready-to-map data to: {output_file}")
    print(f"📊 Total municipalities processed: {len(df_clean)}")


if __name__ == "__main__":
    process_corn_data(
        file_path='corn.xlsx', 
        sheet_name='Summary', # Change this if you want a specific quarter like 'Q3'
        output_file='Cleaned_Yellow_Corn_Panay_Guimaras.csv',
        shapefile_list_csv='Shapefile_Muni_List.csv' 
    )