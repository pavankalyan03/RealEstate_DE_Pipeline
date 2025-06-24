from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pathlib import Path
import json


def clean_json(**context):
    with open('include/rawdata/scraped_data.json', 'r', encoding='utf-8') as f:
        projects = json.load(f)

    cleaned = []
    for p in projects:
        entry = {}

        # 1. Static fields
        for k in ["id","name","subtitle","description","posted_date",
                "is_active_property","is_most_contacted",
                "price_display_value","emi","from_url"]:
            entry[k] = p.get(k)


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
        seller_field = p.get("seller")
        if isinstance(seller_field, list):
            s = seller_field[0] if seller_field else {}
        elif isinstance(seller_field, dict):
            s = seller_field
        else:
            s = {}
        entry.update({
        "seller_name": s.get("name"),
        "seller_type": s.get("type"),
        "seller_phone": s.get("phone", {}).get("partialValue"),
        "seller_is_paid": s.get("isPaid")
        })

        # 8. Nearby places in nested json
        entry["nearby_places"] = []
        for n in p.get("near_by_places", []):
            entry["nearby_places"].append({
                "type": n.get("establishmentType"),
                "name": n.get("name"),
                "distance_km": n.get("distance"),
                "travel_time_min": n.get("duration")
            })

        # 9. Image
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


def upload_into_stage(**context):
    aws_conn_id = 'aws_default'
    bucket_name = 'bucket-for-airflow-s3'
    local_dir = 'include/cleaned'

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    uploaded_files = 0  # Initialize the counter

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
