import os
import io
import json
import pandas as pd
import requests
from datetime import datetime
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from PIL import Image
from io import BytesIO

# === Config ===
EXCEL_FILE_ID = "1rD9zX-kpSd4AZ9Gb62IkHv0oDOIZxRkP"   # <-- Replace with your Drive File ID
LOCAL_EXCEL_FILE = "local-data.xlsx"
# Use environment variable if set, else default
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service_account.json")

# GitHub Pages URL (update with your repo!)
GITHUB_PAGES_URL = "https://yourusername.github.io/drama-collection/"

# Image settings
COVER_WIDTH = 600
COVER_HEIGHT = 900

# === Step 1: Fetch Excel from Google Drive ===
def fetch_excel_from_drive(file_id, output_file=LOCAL_EXCEL_FILE):
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(SERVICE_ACCOUNT_FILE)
    if gauth.credentials is None:
        gauth.LocalWebserverAuth()  # for local testing only
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()

    drive = GoogleDrive(gauth)
    file = drive.CreateFile({'id': file_id})
    file.GetContentFile(output_file)
    print(f"✅ Downloaded Excel from Google Drive → {output_file}")

# === Helpers ===
def format_date(val):
    if pd.isna(val):
        return None
    if isinstance(val, pd.Timestamp):
        return val.strftime("%d-%m-%Y")
    return str(val)

def safe_filename(name):
    return name.replace(" ", "_").replace("/", "_")

def resize_image(content, filepath):
    try:
        img = Image.open(BytesIO(content))
        img = img.convert("RGB")
        img = img.resize((COVER_WIDTH, COVER_HEIGHT), Image.LANCZOS)
        img.save(filepath, format="JPEG", quality=95)
        return True
    except Exception as e:
        print(f"⚠️ Failed to resize image {filepath}: {e}")
    return False

def placeholder_image(show_name, year):
    folder = "images"
    os.makedirs(folder, exist_ok=True)
    filename = f"{safe_filename(show_name)}_{year}.jpg"
    filepath = os.path.join(folder, filename)
    if not os.path.exists(filepath):
        # create a blank placeholder
        img = Image.new("RGB", (COVER_WIDTH, COVER_HEIGHT), color=(200, 200, 200))
        img.save(filepath, format="JPEG", quality=80)
    return f"{GITHUB_PAGES_URL}images/{filename}"

# === Step 2: Excel → JSON Transformation ===
def excel_to_objects(excel_file, sheet_name):
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    df.columns = [col.strip().lower() for col in df.columns]

    key_map = {
        "no": "showID",
        "series title": "showName",
        "started date": "watchStartedOn",
        "finished date": "watchEndedOn",
        "year": "releasedYear",
        "total episodes": "totalEpisodes",
        "original language": "nativeLanguage",
        "language": "watchedLanguage",
        "ratings": "ratings",
        "catagory": "genres",
        "category": "genres",
        "original network": "network",
    }

    again_idx = next((i for i, col in enumerate(df.columns) if "again watched" in col), None)
    if again_idx is None:
        return []

    json_data = []
    for _, row in df.iterrows():
        obj = {}
        for col in df.columns[:again_idx]:
            value = row[col]
            key = key_map.get(col, col)

            if key == "showID":
                base = {"sheet1": 1000, "sheet2": 2000, "mini drama": 3000}.get(sheet_name.lower(), 0)
                obj[key] = base + int(value)
            elif key == "showName":
                obj[key] = " ".join(str(value).split()) if pd.notna(value) else None
            elif key in ["watchStartedOn", "watchEndedOn"]:
                obj[key] = format_date(value)
            elif key == "genres":
                obj[key] = [w.strip().capitalize() for w in str(value).split(",")] if pd.notna(value) else []
            elif key == "network":
                obj[key] = [w.strip() for w in str(value).split(",")] if pd.notna(value) else []
            else:
                obj[key] = str(value).strip() if pd.notna(value) else None

        obj["showType"] = "Mini Drama" if sheet_name.lower() == "mini drama" else "Drama"
        obj["country"] = (
            "South Korea" if obj.get("nativeLanguage") == "Korean"
            else "China" if obj.get("nativeLanguage") == "Chinese"
            else None
        )
        obj["againWatchedDates"] = [format_date(v) for v in row[again_idx:] if pd.notna(v)]
        obj["updatedOn"] = datetime.now().strftime("%d %B %Y")

        ratings = int(obj.get("ratings", 0) or 0)
        obj["topRatings"] = ratings * len(obj["againWatchedDates"]) * 100
        obj["Duration"] = None

        # --- Cover image ---
        show_name = obj.get("showName")
        year = obj.get("releasedYear")
        if show_name and year:
            obj["showImage"] = placeholder_image(show_name, year)
        else:
            obj["showImage"] = None

        obj_ordered = {
            "showID": obj.get("showID"),
            "showName": obj.get("showName"),
            "showImage": obj.get("showImage"),
            "watchStartedOn": obj.get("watchStartedOn"),
            "watchEndedOn": obj.get("watchEndedOn"),
            "releasedYear": obj.get("releasedYear"),
            "totalEpisodes": obj.get("totalEpisodes"),
            "showType": obj.get("showType"),
            "nativeLanguage": obj.get("nativeLanguage"),
            "watchedLanguage": obj.get("watchedLanguage"),
            "country": obj.get("country"),
            "comments": obj.get("comments"),
            "ratings": obj.get("ratings"),
            "genres": obj.get("genres"),
            "network": obj.get("network"),
            "againWatchedDates": obj.get("againWatchedDates"),
            "updatedOn": obj.get("updatedOn"),
            "topRatings": obj.get("topRatings"),
            "Duration": obj.get("Duration"),
        }
        json_data.append(obj_ordered)

    return json_data

# === Step 3: Update JSON with backup ===
def update_json_from_excel(excel_file, json_file, sheet_names):
    new_data = []
    available_sheets = pd.ExcelFile(excel_file).sheet_names
    for sheet in sheet_names:
        if sheet not in available_sheets:
            print(f"⚠️ Skipping missing sheet: {sheet}")
            continue
        new_data.extend(excel_to_objects(excel_file, sheet_name=sheet))

    if os.path.exists(json_file):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                content = f.read().strip()
                old_data = json.loads(content) if content else []
        except (json.JSONDecodeError, ValueError):
            print(f"⚠️ Warning: {json_file} invalid. Starting fresh.")
            old_data = []
    else:
        old_data = []

    old_dict = {item["showID"]: item for item in old_data if "showID" in item}
    new_dict = {item["showID"]: item for item in new_data if "showID" in item}

    changed_objects = []

    for sid, new_obj in new_dict.items():
        if sid in old_dict:
            if old_dict[sid] != new_obj:
                changed_objects.append(old_dict[sid])
                old_dict[sid] = new_obj
        else:
            old_dict[sid] = new_obj

    for sid, old_obj in list(old_dict.items()):
        if sid not in new_dict:
            changed_objects.append(old_obj)
            del old_dict[sid]

    merged_data = sorted(old_dict.values(), key=lambda x: x.get("showID", 0))

    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(merged_data, f, indent=4, ensure_ascii=False)

    if changed_objects:
        timestamp = datetime.now().strftime("%d%m%Y_%H%M")
        os.makedirs("backups", exist_ok=True)
        backup_file = os.path.join("backups", f"{timestamp}.json")
        with open(backup_file, "w", encoding="utf-8") as f:
            json.dump(changed_objects, f, indent=4, ensure_ascii=False)
        print(f"✅ JSON updated. Old/Deleted moved to {backup_file}")
    else:
        if not old_data:
            print(f"✅ JSON created at {json_file}")
        else:
            print("✅ No changes detected")

# === Main ===
if __name__ == "__main__":
    fetch_excel_from_drive(EXCEL_FILE_ID, LOCAL_EXCEL_FILE)
    update_json_from_excel(
        LOCAL_EXCEL_FILE,
        "seriesData.json",
        sheet_names=["Sheet1", "Sheet2", "Mini Drama"]
    )
