from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
from pathlib import Path
import re
import json


def extract_hyderabad_data():
    # Load the CSV file
    df = pd.read_csv('include/rawdata/Makaan_Properties_Buy.csv',encoding='ISO-8859-1')
    df['City_name'] = df['City_name'].astype(str).str.strip().str.lower()
    df['Locality_Name'] = df['Locality_Name'].astype(str).str.strip().str.lower()
    hyderabad_df = df[
        (df['Locality_Name'] == 'hyderabad') | (df['City_name'] == 'hyderabad')
    ]
    hyderabad_df.to_csv('include/rawdata/hyderabad_properties.csv', index=False)


def extract_important_fields():

    def convert_relative_time(relative_str, reference_date=None):
        """
        Convert relative time strings like '1 day ago' to datetime objects.
        """
        if reference_date is None:
            reference_date = datetime.now()
        
        parts = relative_str.split()
        if len(parts) != 3 or parts[2] != 'ago':
            return reference_date  # return reference date if format is unexpected
        
        try:
            quantity = int(parts[0])
            unit = parts[1].lower()
            
            if unit.startswith('day'):
                delta = timedelta(days=quantity)
            elif unit.startswith('month'):
                delta = relativedelta(months=quantity)
            elif unit.startswith('year'):
                delta = relativedelta(years=quantity)
            elif unit.startswith('hour'):
                delta = timedelta(hours=quantity)
            elif unit.startswith('minute'):
                delta = timedelta(minutes=quantity)
            elif unit.startswith('second'):
                delta = timedelta(seconds=quantity)
            else:
                return reference_date  # unknown unit
            
            return reference_date - delta
        except:
            return reference_date  # return reference date if parsing fails
        
    file_path = 'include/rawdata/past_data.csv'  # Replace with your actual file path
    df = pd.read_csv(file_path, encoding='ISO-8859-1')
    important_columns = [
        'Property_id',         # Unique ID
        'Locality_Name',       # Locality for trend basis
        'Latitude',            # Geo location
        'Longitude',           # Geo location
        'Price_per_unit_area', # Needed for trend calculation
        'Posted_On',           # Posting date reference
        'Property_type',       # Metadata
        'Property_status',     # Metadata
        'Size',                # Metadata
        'Price',               # Metadata
        'Builder_name',        # Metadata
        'City_name'            # Context
    ]
    important_df = df[important_columns]
    important_df.columns = important_df.columns.str.strip()
    REFERENCE_DATE = datetime(2021, 6, 12)  # Example: June 1, 2024
    # Convert the 'Posted_On' column using the reference date
    important_df['Posted_On'] = important_df['Posted_On'].apply(lambda x: convert_relative_time(x, REFERENCE_DATE))
    # Format the datetime as desired
    important_df['Posted_On'] = important_df['Posted_On'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str.slice(0, 23)
    print("Conversion complete using reference date:", REFERENCE_DATE)
    print(important_df['Posted_On'])  #
    important_df.to_csv('include/cleaned/past_data_hyd_cleaned.csv', index=False)


def clean_hyd_csv():
    df = pd.read_csv('include/rawdata/hyderabad_properties.csv', encoding='ISO-8859-1')
    def clean_numeric(val): 
        if pd.isnull(val):
            return None
        val = re.sub(r'[^\d.]', '', str(val))  # Remove everything except numbers and dot
        return float(val) if val else None
    def extract_bhk(val):
        if pd.isnull(val):
            return None
        match = re.search(r'\d+', str(val))
        return int(match.group()) if match else None
    def to_boolean(val):
        if str(val).strip().lower() in ['true', 'yes', '1']:
            return True
        elif str(val).strip().lower() in ['false', 'no', '0']:
            return False
        return None

    df["Price_per_unit_area"] = df["Price_per_unit_area"].apply(clean_numeric)
    df["Price"] = df["Price"].apply(clean_numeric)
    df["Size"] = df["Size"].apply(clean_numeric)
    df["No_of_BHK"] = df["No_of_BHK"].apply(extract_bhk)

    # Convert all boolean-ish columns
    boolean_columns = [
        "is_furnished", "is_plot", "is_RERA_registered", "is_Apartment",
        "is_ready_to_move", "is_commercial_Listing", "is_PentaHouse", "is_studio"
    ]
    for col in boolean_columns:
        df[col] = df[col].apply(to_boolean)

    # Save cleaned version
    df.to_csv('include/rawdata/past_data.csv', index=False)
    print("Cleaned and transformed data saved to 'property_data_cleaned.csv'")



def clean_json():
    with open('include/rawdata/scraped_data.json', 'r', encoding='utf-8') as f:
        projects = json.load(f)

    cleaned = []
    for p in projects[:900]:
        entry = {}

        # 1. Static fields
        for k in ["id","name","subtitle","description","posted_date",
                "is_active_property","is_most_contacted",
                "price_display_value","emi","from_url"]:
            entry[k] = p.get(k)

        # 1.5 Add image url here formatted 

        # 2. Location
        loc = p.get("address",{})
        entry["address"] = loc.get("address")
        entry["long_address"] = loc.get("longAddress")
        for geo in ["city","locality","region","state"]:
            entry[geo] = p.get("polygons_hash",{}).get(geo,{}).get("name")

        coords = p.get("coords",[None,None])
        entry["latitude"], entry["longitude"] = coords[0], coords[1]

        # 3. Pricing & size
        pi = p.get("property_information",{})
        entry.update({
        "min_price": p.get("min_price"),
        "max_price": p.get("max_price"),
        "currency": p.get("currency"),
        "property_area": pi.get("area"),
        "property_price": pi.get("price"),
        "bedrooms": pi.get("bedrooms"),
        "bathrooms": pi.get("bathrooms"),
        })
        entry["avg_price_per_sqft"] = next((f['description']
                                for f in p.get("features",[])
                                if f["label"]=="Avg. Price"), None)
        entry["possession_date"] = next((f['description']
                                for f in p.get("features",[])
                                if f["label"]=="Possession Starts"), None)
        entry["size_range"] = next((f['description']
                                for f in p.get("features",[])
                                if f["label"]=="Sizes"), None)

        # 4. Configs
        configs = [{
            "label": cfg.get("label"),
            "range": cfg.get("range")
        } for cfg in p.get("details", {}).get("config", {}).get("propertyConfig", [])]

        entry["configurations"] = {cfg.get("label").lower().replace(" ","_"): cfg.get("range") for cfg in configs}
        # for cfg in p.get("details",{}).get("config",{}).get("propertyConfig",[]):
        #     lbl = cfg.get("label","").lower().replace(" ","_")
        #     entry[f"config_{lbl}_range"] = cfg.get("range")

        # 5. Tags
        entry["tags"] = {
            "all": p.get("property_tags", [])
        }

        # 6. Dynamic amenities
        entry["amenities"] = {}
        for block in p.get("details", {}).get("amenities", []):
            key = block.get("type", "").lower().replace(" ", "_")
            entry["amenities"][key] = block.get("data", [])

        # 7. Seller
        s = p.get("seller",[{}])[0]
        entry.update({
        "seller_name": s.get("name"),
        "seller_type": s.get("type"),
        "seller_phone": s.get("phone",{}).get("partialValue"),
        "seller_is_paid": s.get("isPaid")
        })

        # 8. Nearby #(top 2)# places
        entry["nearby_places"] = []
        for n in p.get("near_by_places", []):
            entry["nearby_places"].append({
                "type": n.get("establishmentType"),
                "name": n.get("name"),
                "distance_km": n.get("distance"),
                "travel_time_min": n.get("duration")
            })

        # 9. Media
        media = {"main_image_url": None, "floorplan_image_url": None, "brochure_url": None}
        for block in p.get("details", {}).get("images", []):
            imgs = block.get("images", [])
            if block.get("type") == "property" and imgs:
                media["main_image_url"] = imgs[0].get("src")
        # floorplan might be a separate type
        for block in p.get("details", {}).get("images", []):
            if block.get("type") == "floorPlan":
                imgs = block.get("images", [])
                if imgs:
                    media["floorplan_image_url"] = imgs[0].get("src")

        # 10. Brochure
        entry["brochure_url"] = p.get("details",{}).get("brochure",{}).get("pdf")

        # 11. Formatted image URL
        original_image_url = p.get("image_url")
        if original_image_url:
            try:
                # Convert to formatted image URL
                formatted_url = original_image_url.replace(
                    "is1-2.housingcdn.com", "housing-images.n7net.in"
                ).replace(
                    "/version/", "/fs/"
                )
                entry["image_url"] = formatted_url
            except Exception as e:
                entry["image_url"] = None

        # 12. Full project URL
        raw_url = p.get("url")
        if raw_url and raw_url.startswith("/"):
            entry["url"] = f"https://housing.com{raw_url}"
        else:
            entry["url"] = raw_url  # fallback

        cleaned.append(entry)


    # Write cleaned JSON
    with open('include/cleaned/clean_real_estate.json','w',encoding='utf-8') as f:
        json.dump(cleaned, f, indent=2, ensure_ascii=False)

    print("Cleaned JSON written: clean_real_estate.json")


def upload_files(**context):
    # Configuration
    aws_conn_id = 'aws_default'
    bucket_name = 'bucket-for-airflow-s3'
    local_dir = 'include/cleaned'
    
    # Initialize S3Hook
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    
    # Ensure local directory exists
    if not Path(local_dir).is_dir():
        print(f"Local directory {local_dir} does not exist.")
        return
    
    uploaded_files = 0
    
    for file_path in Path(local_dir).glob('*'):
        if not file_path.is_file():
            continue  # Skip directories
        
        filename = file_path.name
        ext = file_path.suffix.lower()

        # Decide S3 directory based on file type
        if ext == '.csv':
            s3_key = f"csv/{filename}"
        elif ext == '.json':
            s3_key = f"json/{filename}"
        else:
            print(f"Skipping unsupported file type: {filename}")
            continue

        print(f"Uploading {file_path} to s3://{bucket_name}/{s3_key}")
        
        try:
            s3_hook.load_file(
                filename=str(file_path),
                key=s3_key,
                bucket_name=bucket_name,
                replace=True
            )
            uploaded_files += 1
        except Exception as e:
            print(f"Failed to upload {filename}: {e}")
    
    if uploaded_files == 0:
        print(f"No files uploaded to s3://{bucket_name}")
    else:
        print(f"Successfully uploaded {uploaded_files} files to s3://{bucket_name}")



# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}



with DAG(
    dag_id = 'pipeline',
    description = 'It\'s not simple anymore',
    default_args = default_args,
    # schedule_interval='59 10 * * *',
    catchup=False,
    start_date=datetime(2023, 1, 1),

) as dag:
    
    extract_task = PythonOperator(
        task_id='clean_data',
        python_callable= extract_hyderabad_data,
        provide_context=True,
    )

    clean_hyd_csv_task = PythonOperator(
        task_id='clean_hyd_csv',
        python_callable=clean_hyd_csv,
        provide_context=True,
    )

    important_fields_task = PythonOperator(
        task_id='extract_important_fields',
        python_callable=extract_important_fields,
        provide_context=True,
    )


    clean_json_task = PythonOperator(
        task_id='clean_json',
        python_callable=clean_json,
        provide_context=True,
    )

    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_files,
        provide_context=True,
    )

    copy_to_snowflake_task = CopyFromExternalStageToSnowflakeOperator(
        task_id='copy_from_s3_to_snowflake',
        snowflake_conn_id='snowflake_default',
        stage='REALESTATE.PROPERTY.real_estate_stage_csv',  
        table='REALESTATE.BRONZE.HYDERABAD_DATA_PAST',  
        file_format='(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY= \'\"\' SKIP_HEADER = 1)', 
        pattern='.*',  
    )

    # Set task dependencies
    extract_task >> clean_hyd_csv_task >> important_fields_task >>  clean_json_task >> upload_task >> copy_to_snowflake_task
    # Set the order of execution
    # extract_task >> clean_hyd_csv_task >> important_fields_task >> clean_json_task >> upload_task >> copy_to_snowflake_task