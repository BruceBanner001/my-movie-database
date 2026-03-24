import os, time, re, json, sys, traceback, io
from datetime import datetime, timedelta, timezone
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import cloudscraper

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

try:
    from duckduckgo_search import DDGS
except ImportError:
    pass

# Setup Timezone (IST)
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    return datetime.now(IST)

TODAY_DATE = now_ist().strftime("%d-%m-%Y")
REPORTS_DIR = "reports"

SCRAPER = cloudscraper.create_scraper()
SCRAPER.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
STATE_FILE = "title_validator_state.json"

def normalize_title(t):
    if not t or str(t).strip() == "N/A": return ""
    return re.sub(r"[^a-z0-9]", "", str(t).lower().strip())

def search_and_get_title(search_term, year, site):
    clean_search = re.sub(r"\b(?:Season|Part|S)\s*\d+\b|\s+\d+$", "", str(search_term), flags=re.IGNORECASE).strip()
    queries =[
        f'"{search_term}" {year} site:{site}.com',
        f'"{clean_search}" {year} site:{site}.com'
    ]
    try:
        with DDGS() as dd:
            for query in queries:
                results = list(dd.text(query, max_results=3))
                for res in results:
                    url = res.get("href", "")
                    if any(bad in url for bad in["/reviews", "/recs", "/photos", "?lang=", "/characters", "/episodes"]):
                        continue

                    r = SCRAPER.get(url, timeout=10)
                    if r.status_code == 200:
                        soup = BeautifulSoup(r.text, "html.parser")
                        title = None
                        if site == "asianwiki":
                            h1 = soup.find("h1", class_="firstHeading")
                            if h1: title = h1.get_text(strip=True).replace(" (Drama)", "")
                        elif site == "mydramalist":
                            h1 = soup.find("h1", class_="film-title")
                            if h1: title = h1.get_text(strip=True)
                        elif site == "imdb":
                            h1 = soup.find("h1")
                            if h1: title = h1.get_text(strip=True)

                        if title:
                            title = re.sub(r"\s*\(\d{4}\)$", "", title).strip()
                            return title, site
                time.sleep(2) # Prevent Rate Limits
    except Exception as e:
        pass
    return "N/A", site

def fetch_excel_from_gdrive_bytes(file_id, creds_path):
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=["https://www.googleapis.com/auth/drive.readonly"])
    service = build("drive", "v3", credentials=creds)
    try:
        request = service.files().get_media(fileId=file_id)
    except Exception:
        request = service.files().export_media(fileId=file_id, mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done: _, done = downloader.next_chunk()
    fh.seek(0)
    return fh

def write_report(state, current_run_seconds, run_start_time, is_paused, max_fetches, fetches_used):
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    trigger_type = "Manual" if is_manual else "Automatic"
    
    current_gh_run = os.environ.get('GITHUB_RUN_NUMBER', 'Local')
    first_run = state.get('first_run_id', current_gh_run)
    end_time_ist = now_ist().strftime("%d %B %Y - %I:%M:%S %p")
    
    if is_paused:
        total_seconds = int(current_run_seconds)
        run_display = f"{current_gh_run}"
        start_time_str = run_start_time.strftime("%d %B %Y - %I:%M:%S %p")
        run_label = "⏱️ Run Time      : "
        batch_label = f"🔄 Current Batch : {state.get('batch_run_count', 1)}"
        status_msg = "✅ Partial Batch completed successfully"
        batch_msg = "⏳ Batch Processing in Progress..."
    else:
        total_seconds = int(state.get('cumulative_time_seconds', 0) + current_run_seconds)
        run_display = f"{first_run} - {current_gh_run}" if str(first_run) != str(current_gh_run) else f"{current_gh_run}"
        start_time_str = state.get('global_start_time')
        run_label = "⏱️ Total Runtime : "
        batch_label = f"🔄 Total Batches : {state.get('batch_run_count', 1)} Run{'s' if state.get('batch_run_count', 1) != 1 else ''}"
        status_msg = "✅ Workflow Batch completed successfully"
        batch_msg = "🏁 Final Batch Completed"

    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    if hours > 0: runtime_str = f"{hours} Hour{'s' if hours > 1 else ''} {minutes} Minute{'s' if minutes != 1 else ''} {seconds} Second{'s' if seconds != 1 else ''}"
    elif minutes > 0: runtime_str = f"{minutes} Minute{'s' if minutes != 1 else ''} {seconds} Second{'s' if seconds != 1 else ''}"
    else: runtime_str = f"{seconds} Second{'s' if seconds != 1 else ''}"

    lines =[
        status_msg, batch_msg,
        "══════════════════════════════════════════════════════",
        "📊 Drama Title Validator – Execution Report",
        "══════════════════════════════════════════════════════", "",
        f"🚀 Workflow Type : {trigger_type}",
        f"🔁 RUN           : {run_display}",
        f"⏰ Start Time    : {start_time_str}",
        f"⏰ End Time      : {end_time_ist}",
        f"{run_label}{runtime_str}",
        f"⚙️ Max Process   : {max_fetches} Row Per Run",
        batch_label, ""
    ]

    total_skipped = total_recs = total_perfect = total_fixed = 0
    
    for sheet, data in state["report"].items():
        if not any([data["new_recs"], data["perfect"], data["user_fixed"], data["skipped"]]): continue
        lines.extend(["━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", f"🗂️ === {sheet} ===", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"])
        
        if data["new_recs"]:
            lines.append("\n✨ Brand New Recommendations Found (Action Required):")
            lines.extend(data["new_recs"])
            total_recs += len(data["new_recs"])
            
        if data["perfect"]:
            lines.append("\n✅ Perfect Matches (Newly Scanned):")
            lines.extend(data["perfect"])
            total_perfect += len(data["perfect"])
            
        if data["user_fixed"]:
            lines.append("\n🔄 User Fixed & Re-Verified (You changed these in Excel1!):")
            lines.extend(data["user_fixed"])
            total_fixed += len(data["user_fixed"])
            
        if data["skipped"] > 0:
            lines.append(f"\n⏭️ Skipped (Already Verified Previously):\n- {data['skipped']} dramas skipped instantly.")
            total_skipped += data["skipped"]
        lines.append("")

    summary_title = "📊 Overall Cumulative Summary" if not is_paused else "📊 Summary (Current Batch Only)"
    lines.extend([
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", summary_title, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"🌐 Internet Fetches Used : {fetches_used} (Out of {max_fetches} limit)",
        f"⏭️ Total Skipped Fast    : {total_skipped}",
        f"✨ Total Recommendations : {total_recs}",
        f"✅ Total Perfect Matches : {total_perfect}",
        f"🔄 Total User Fixes Seen : {total_fixed}",
        "\n🏁 Workflow finished successfully" if not is_paused else "\n⚠️ BATCH LIMIT REACHED: The script paused safely."
    ])

    report_text = "\n".join(lines)
    print(report_text)

    # Save outputs
    os.makedirs(REPORTS_DIR, exist_ok=True)
    ts = now_ist().strftime("%d_%B_%Y_%H%M")
    
    if is_paused:
        report_name = f"{ts}_PARTIAL_{current_gh_run}_REPORT.txt"
    else:
        report_name = f"{ts}_FINAL_{first_run}-{current_gh_run}_REPORT.txt" if str(first_run) != str(current_gh_run) else f"{ts}_FINAL_{current_gh_run}_REPORT.txt"
        
    report_path = os.path.join(REPORTS_DIR, report_name)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)

    step_summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if step_summary:
        with open(step_summary, 'a', encoding='utf-8') as f:
            f.write("```text\n" + report_text + "\n```\n")

    mail_date = now_ist().strftime("%d %B %Y %I:%M %p IST")
    email_subject = f"[{trigger_type}] Title Validation {mail_date} Report"
    with open("EMAIL_SUBJECT.txt", "w", encoding='utf-8') as ef: 
        ef.write(email_subject)

def main():
    run_start_time = now_ist()
    MAX_FETCHES = int(os.environ.get("MAX_FETCHES", "50"))
    fetches_used = 0
    limit_reached = False
    current_gh_run = os.environ.get('GITHUB_RUN_NUMBER', 'Local')

    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f: state = json.load(f)
    else:
        state = {
            "sheet_idx": 0, "row_idx": 0, "report": {},
            "global_start_time": run_start_time.strftime("%d %B %Y - %I:%M:%S %p"),
            "cumulative_time_seconds": 0,
            "first_run_id": current_gh_run, "batch_run_count": 1
        }

    gc = gspread.service_account(filename="GDRIVE_SERVICE_ACCOUNT.json")
    with open("EXCEL_FILE_ID.txt", "r") as f: main_excel_id = f.read().strip()

    excel_bytes = fetch_excel_from_gdrive_bytes(main_excel_id, "GDRIVE_SERVICE_ACCOUNT.json")
    xl = pd.ExcelFile(io.BytesIO(excel_bytes.getvalue()))

    check_sh = gc.open_by_key(os.environ.get("CHECK_TITLES_EXCEL_ID"))
    try:
        ws_out = check_sh.worksheet("Check Titles")
        existing_df = get_as_dataframe(ws_out, evaluate_formulas=True).dropna(how="all")
    except gspread.exceptions.WorksheetNotFound:
        ws_out = check_sh.add_worksheet(title="Check Titles", rows="1000", cols="20")
        existing_df = pd.DataFrame()

    cache = {}
    if not existing_df.empty and "Sheet Name" in existing_df.columns:
        for _, row in existing_df.iterrows():
            key = f"{row.get('Sheet Name')}_{int(row.get('Show ID', 0))}"
            cache[key] = {
                "Show Name": str(row.get("Show Name", "")),
                "Recommended Title Name": str(row.get("Recommended Title Name", "N/A")),
                "row_data": row.to_dict()
            }

    sheets_to_process =[s.strip() for s in os.environ.get("SHEETS", "Sheet1;Sheet2").split(";") if s.strip()]
    results =[]
    
    for s_idx in range(state["sheet_idx"], len(sheets_to_process)):
        if limit_reached: break
        sheet_name = sheets_to_process[s_idx]
        report_sheet = state["report"].setdefault(sheet_name, {"new_recs": [], "perfect":[], "user_fixed":[], "skipped": 0})

        target_sheet = next((s for s in xl.sheet_names if s.strip().lower() == sheet_name.strip().lower()), None)
        if not target_sheet: continue

        df_in = pd.read_excel(xl, sheet_name=target_sheet)
        subset_cols = [c for c in ["Show ID", "No"] if c in df_in.columns]
        if subset_cols: df_in = df_in.dropna(how="all", subset=subset_cols)

        start_r = state["row_idx"] if s_idx == state["sheet_idx"] else 0

        for r_idx in range(start_r, len(df_in)):
            if fetches_used >= MAX_FETCHES:
                limit_reached = True
                state["sheet_idx"] = s_idx
                state["row_idx"] = r_idx
                break

            row = df_in.iloc[r_idx]
            try:
                sid = int(row.get("No") or row.get("Show ID", 0))
                title = str(row.get("Series Title") or row.get("Show Name", "")).strip()
                year = int(row.get("Year") or row.get("Released Year", 0))
                lang = str(row.get("Original Language") or row.get("Native Language", "Korean")).strip().capitalize()
            except ValueError: continue

            if sid == 0 or not title or title.lower() == "nan": continue

            norm_title = normalize_title(title)
            cache_key = f"{sheet_name}_{sid}"
            cached_data = cache.get(cache_key)

            if cached_data:
                cached_title = cached_data["Show Name"]
                cached_rec = cached_data["Recommended Title Name"]
                
                if title == cached_title:
                    report_sheet["skipped"] += 1
                    continue
                
                if cached_rec != "N/A" and norm_title == normalize_title(cached_rec):
                    report_sheet["user_fixed"].append(f"- [ID {sid}] **{cached_title}** -> Now perfectly matches: {title}")
                    updated_row = cached_data["row_data"].copy()
                    updated_row["Show Name"] = title
                    updated_row["Title Recommendation"] = "No"
                    updated_row["Recommended Title Name"] = "N/A"
                    updated_row["Last Update Date"] = TODAY_DATE
                    results.append(updated_row)
                    continue

            fetches_used += 1

            is_asian = lang.lower() in ["korean", "chinese", "japanese", "thai", "taiwanese", "filipino"]
            aw_title, mdl_title, imdb_title = "N/A", "N/A", "N/A"
            source_used = ""

            if is_asian:
                mdl_title, _ = search_and_get_title(title, year, "mydramalist")
                if mdl_title != "N/A":
                    rec_title = mdl_title
                    source_used = "MyDramaList"
                else:
                    aw_title, _ = search_and_get_title(title, year, "asianwiki")
                    rec_title = aw_title
                    source_used = "AsianWiki"
            else:
                imdb_title, _ = search_and_get_title(title, year, "imdb")
                rec_title = imdb_title
                source_used = "IMDb"

            needs_rec = "No" if normalize_title(rec_title) == norm_title else "Yes"
            if needs_rec == "No" or rec_title == "N/A":
                rec_title = "N/A"
                report_sheet["perfect"].append(f"-[ID {sid}] **{title}** -> Perfect!")
            else:
                report_sheet["new_recs"].append(f"- [ID {sid}] **{title}** ➔ Recommend: {rec_title} (Source: {source_used})")

            results.append({
                "Sheet Name": sheet_name, "Show ID": sid, "Show Name": title,
                "Released Year": year, "Language": lang, "Last Update Date": TODAY_DATE,
                "asianwiki": aw_title, "mydramalist": mdl_title, "imdb": imdb_title,
                "Title Recommendation": needs_rec, "Recommended Title Name": rec_title
            })

    if results:
        new_df = pd.DataFrame(results)
        if not existing_df.empty and "Sheet Name" in existing_df.columns:
            existing_df["Show ID"] = pd.to_numeric(existing_df["Show ID"], errors="coerce")
            new_df["Show ID"] = pd.to_numeric(new_df["Show ID"], errors="coerce")
            existing_df.set_index(["Sheet Name", "Show ID"], inplace=True)
            new_df.set_index(["Sheet Name", "Show ID"], inplace=True)
            combined_df = new_df.combine_first(existing_df).reset_index()
        else:
            combined_df = new_df
        
        ws_out.clear()
        set_with_dataframe(ws_out, combined_df.fillna("N/A"))

    current_run_seconds = (now_ist() - run_start_time).total_seconds()

    if limit_reached:
        state["batch_run_count"] += 1
        state["cumulative_time_seconds"] += current_run_seconds
        with open("RESUME_FLAG.txt", "w") as f: f.write("CONTINUE")
        with open(STATE_FILE, "w", encoding="utf-8") as f: json.dump(state, f)
    else:
        if os.path.exists(STATE_FILE): os.remove(STATE_FILE)

    write_report(state, current_run_seconds, run_start_time, limit_reached, MAX_FETCHES, fetches_used)

if __name__ == "__main__":
    try: main()
    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}")
        traceback.print_exc()
        
        report_path = os.path.join(REPORTS_DIR, "CRASH_REPORT.txt")
        os.makedirs(REPORTS_DIR, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f: f.write(f"❌ Script Crashed!\nError Message:\n{str(e)}")
        
        with open("EMAIL_SUBJECT.txt", "w", encoding="utf-8") as f: f.write("[CRASH] Title Validation Failed")
        sys.exit(1)