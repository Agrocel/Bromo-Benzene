import re
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import *
import shutil
from Database.db_con import connection_to_database

#Raw file Path
raw_folder = r"Z:\Bromo-Benzene-PP\Data\Raw"
processed_folder = r"Z:\Bromo-Benzene-PP\Data\Processed"

# Connecttion for database
engine = connection_to_database()

try:
    with engine.connect() as connection:
        print("✅ Database connection successful!")
except Exception as e:
    print(f"❌ Database connection failed: {e}")
    sys.exit(1) # Exit the script if the connection fails

files = [f for f in os.listdir(raw_folder) if f.endswith((".xlsx",".xls","XLSX"))]


for file in files:

    file_path = os.path.join(raw_folder,file)

    
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        print(f"Error reading {file}: {e}")
        continue

    df.columns = (
    df.columns.str.strip()  # remove leading/trailing spaces
              .str.replace(r'[^0-9a-zA-Z]+', '_', regex=True) 
              .str.replace(r'_+$', '', regex=True)
              
    )
    print(df.columns)
    required_columns = ['IndianPort', 'Mode_of_Shipment', 'Cush', 'Invoice_No', 'Item_No',      
       'Bill_No', 'Four_Digit', 'Date', 'Hs_Code', 'Product_Name', 'Quantity', 
       'Unit', 'Item_Rate_INR', 'Item_Rate_INV', 'Currency',
       'Total_Amount_Invoice_FC', 'FOB_INR', 'IEC', 'Indian_Company',
       'Address', 'City', 'Foreign_Company', 'Foreign_Country', 'Foreign_Port'
       ]
    
    for col in required_columns:
        if col not in df.columns:
            print(f"columns {col} not found in , adding Null value")
            df[col] = None
    
    df = df[required_columns]
    df['Imported_at'] = datetime.now()
    df['Source_file_name'] = file

    try:
        df.to_sql(name='raw_data_exp', con=engine, if_exists="append", index=False)
        print(f"Imported {file} successfully")
    except Exception as e:
        print(f"Error importing {file}: {e}")
        continue

    # Move file to Processed
    shutil.move(file_path, os.path.join(processed_folder,file))
    