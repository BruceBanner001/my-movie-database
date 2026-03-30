import os, time, re, json, sys, traceback, io, random
from datetime import datetime, timedelta, timezone
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import cloudscraper

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Safely import the new ddgs package
try:
    from ddgs import DDGS
except ImportError:
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

FORCE_CHECK = os.environ.get("FORCE_CHECK", "false").lower() == "true"

# Anti-Blocker: Spoof a real Windows Chrome browser to bypass Status 202/403
SCRAPER = cloudscraper.create_scraper(
    browser={
        'browser': 'chrome',
        'platform': 'windows',
        'desktop': True
    }
)

STATE_FILE = "title_validator_state.json"

LANG_TO_COUNTRY = {
    "korean": "South Korea",
    "chinese": "China",
    "japanese": "Japan",
    "thai": "Thailand",
    "taiwanese": "Taiwan",
    "filipino": "Philippines"
}

def search_and_verify_title(search_term, expected_year, lang, expected_eps, sheet_name, site):
    clean_search = re.sub(r"\(.*?\)", "", str(search_term)).strip()
    
    # Strip Season for IMDb (Western Shows) to find the Master Page
    if site == "imdb":
        clean_search = re.sub(r"\b(?:Season|Part|S)\s*\d+\b|\s+\d+$", "", clean_search, flags=re.IGNORECASE).strip()
    
    queries =[]
    if expected_year and expected_year != 0:
        queries.append(f'{clean_search} {expected_year} site:{site}.com')
    queries.append(f'{clean_search} site:{site}.com')
    
    expected_country = LANG_TO_COUNTRY.get(str(lang).lower().strip())
    expected_type = "movie" if "movie" in sheet_name.lower() else "drama"
    
    generic_reason = "No results found on search engine"
    deep_failure_reason = "" # Stores the exact reason if we open a page and it fails

    for query in queries:
        results =[]
        for attempt in range(2):
            try:
                with DDGS() as dd:
                    results = list(dd.text(query, max_results=5)) 
                if results: break 
            except Exception:
                time.sleep(5 * (attempt + 1)) 
                
        for res in results:
            raw_url = res.get("href", "").lower()
            url = ""
            
            # --- URL CHOPPING (The Sub-Page Killer) ---
            if site == "mydramalist":
                match = re.search(r'(mydramalist\.com/[0-9]+-[^/]+)', raw_url)
                if match:
                    url = "https://" + match.group(1)
                else:
                    generic_reason = "Only found non-drama pages (actors/articles)"
                    continue
            elif site == "imdb":
                match = re.search(r'(imdb\.com/title/tt[0-9]+)', raw_url)
                if match:
                    url = "https://www." + match.group(1) + "/"
                else:
                    generic_reason = "Only found non-title pages"
                    continue

            try:
                r = SCRAPER.get(url, timeout=12)
                if r.status_code == 200:
                    soup = BeautifulSoup(r.text, "html.parser")
                    title = None
                    scraped_year = 0
                    scraped_country = ""
                    scraped_type = ""
                    scraped_eps = 0
                    
                    # --- DEEP VERIFICATION LOGIC ---
                    if site == "mydramalist":
                        h1 = soup.find("h1", class_="film-title")
                        if h1: title = h1.get_text(strip=True)
                        
                        # 1. Scrape Country
                        country_tag = soup.find('b', string=re.compile(r'Country:?'))
                        if country_tag and country_tag.next_sibling:
                            scraped_country = re.sub(r'\s+', ' ', str(country_tag.next_sibling)).strip().lower()
                            
                        # 2. Scrape Year
                        aired_tag = soup.find('b', string=re.compile(r'Aired:?'))
                        if aired_tag and aired_tag.parent:
                            match = re.search(r'\b(19|20)\d{2}\b', aired_tag.parent.get_text())
                            if match: scraped_year = int(match.group())
                            
                        # 3. Scrape Type
                        type_tag = soup.find('b', string=re.compile(r'Type:?'))
                        if type_tag and type_tag.next_sibling:
                            scraped_type = type_tag.next_sibling.strip().lower()
                            
                        # 4. Scrape Episodes
                        eps_tag = soup.find('b', string=re.compile(r'Episodes:?'))
                        if eps_tag and eps_tag.next_sibling:
                            match = re.search(r'\d+', str(eps_tag.next_sibling))
                            if match: scraped_eps = int(match.group())

                    elif site == "imdb":
                        h1 = soup.find("h1")
                        if h1: title = h1.get_text(strip=True)
                        
                        if soup.title:
                            match = re.search(r'\b(19|20)\d{2}\b', soup.title.get_text())
                            if match: scraped_year = int(match.group())

                    if not title: 
                        deep_failure_reason = "Failed to parse page title"
                        continue
                        
                    title = re.sub(r"\s*\(\d{4}\)$", "", title).strip()

                    # ==========================================
                    # THE ULTIMATE SECURITY CHECKPOINT
                    # ==========================================
                    
                    # A. VERIFY COUNTRY
                    if site == "mydramalist" and expected_country:
                        if expected_country.lower() not in scraped_country:
                            deep_failure_reason = f"Country Mismatch - Expected '{expected_country}', Found '{scraped_country.title()}'"
                            continue 

                    # B. VERIFY YEAR (Tolerance of ±1 year)
                    if expected_year != 0 and scraped_year != 0:
                        if abs(expected_year - scraped_year) > 1:
                            deep_failure_reason = f"Year Mismatch - Expected '{expected_year}', Found '{scraped_year}'"
                            continue 

                    # C. VERIFY TYPE (Only for MDL)
                    if site == "mydramalist":
                        if expected_type == "movie" and "movie" not in scraped_type:
                            deep_failure_reason = f"Type Mismatch - Expected Movie, Found {scraped_type.title()}"
                            continue
                        if expected_type == "drama":
                            # Reject explicitly wrong types
                            if "movie" in scraped_type or "special" in scraped_type or "tv show" in scraped_type:
                                deep_failure_reason = f"Type Mismatch - Expected Drama, Found {scraped_type.title()}"
                                continue

                    # D. VERIFY EPISODES (Smart Tolerance)
                    if site == "mydramalist" and expected_eps > 0 and scraped_eps > 0:
                        if expected_eps == scraped_eps:
                            pass # Exact Match
                        elif expected_eps == scraped_eps * 2 or scraped_eps == expected_eps * 2:
                            pass # Netflix Split / Double Match
                        elif abs(expected_eps - scraped_eps) <= 2:
                            pass # Censorship Cut Match
                        else:
                            deep_failure_reason = f"Episodes Mismatch - Expected '{expected_eps}', Found '{scraped_eps}'"
                            continue

                    # If it survives the gauntlet, it's the correct drama!
                    return title, site, url, "Success"
                else:
                    deep_failure_reason = f"Website blocked connection (Status {r.status_code})"
            except Exception:
                deep_failure_reason = "Website blocked connection / Timeout"
                pass
                
        time.sleep(random.uniform(2.5, 4.5))
    
    # Return the most specific reason found, or the generic reason
    final_reason = deep_failure_reason if deep_failure_reason else generic_reason
    return "N/A", site, "N/A", final_reason

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

def unique_list(lst):
    return list(dict.fromkeys(lst))

def combine_reports(d1, d2):
    res = {}
    for k in set(d1.keys()).union(d2.keys()):
        res[k] = {
            "new_recs": unique_list(d1.get(k, {}).get("new_recs",[]) + d2.get(k, {}).get("new_recs",[])),
            "perfect": d1.get(k, {}).get("perfect", 0) + d2.get(k, {}).get("perfect", 0),
            "user_fixed": unique_list(d1.get(k, {}).get("user_fixed",[]) + d2.get(k, {}).get("user_fixed",[])),
            "not_found_asian": unique_list(d1.get(k, {}).get("not_found_asian",[]) + d2.get(k, {}).get("not_found_asian",[])),
            "not_found_non_asian": unique_list(d1.get(k, {}).get("not_found_non_asian",[]) + d2.get(k, {}).get("not_found_non_asian",[])),
            "skipped": d1.get(k, {}).get("skipped", 0) + d2.get(k, {}).get("skipped", 0)
        }
    return res

def write_report(current_report, state, current_run_seconds, run_start_time, is_paused, max_fetches, fetches_used):
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    trigger_type = "Manual" if is_manual else "Automatic"
    current_gh_run = os.environ.get('GITHUB_RUN_NUMBER', 'Local')

    def build_report_text(rep_data, is_cumulative):
        first_run = state.get('first_run_id', current_gh_run)
        end_time_ist = now_ist().strftime("%d %B %Y - %I:%M:%S %p")
        
        if is_cumulative:
            total_seconds = int(state.get('cumulative_time_seconds', 0) + current_run_seconds)
            run_display = f"{first_run} - {current_gh_run}" if str(first_run) != str(current_gh_run) else f"{current_gh_run}"
            start_time_str = state.get('global_start_time')
            run_label = "⏱️ Total Runtime : "
            batch_label = f"🔄 Total Batches : {state.get('batch_run_count', 1)} Run{'s' if state.get('batch_run_count', 1) != 1 else ''}"
            status_msg = "✅ Workflow Batch completed successfully"
            batch_msg = "🏁 Final Batch Completed"
        else:
            total_seconds = int(current_run_seconds)
            run_display = f"{current_gh_run}"
            start_time_str = run_start_time.strftime("%d %B %Y - %I:%M:%S %p")
            run_label = "⏱️ Run Time      : "
            batch_label = f"🔄 Current Batch : {state.get('batch_run_count', 1)}"
            if is_paused:
                status_msg = "✅ Partial Batch completed successfully"
                batch_msg = "⏳ Batch Processing in Progress..."
            else:
                status_msg = "✅ Workflow Batch completed successfully"
                batch_msg = "🏁 Final Batch Completed"

        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        if hours > 0: runtime_str = f"{hours} Hour{'s' if hours > 1 else ''} {minutes} Minute{'s' if minutes != 1 else ''} {seconds} Second{'s' if seconds != 1 else ''}"
        elif minutes > 0: runtime_str = f"{minutes} Minute{'s' if minutes != 1 else ''} {seconds} Second{'s' if seconds != 1 else ''}"
        else: runtime_str = f"{seconds} Second{'s' if seconds != 1 else ''}"

        force_check_str = " (FORCE CHECK ON 🚨)" if FORCE_CHECK else ""

        lines =[
            status_msg, batch_msg,
            "══════════════════════════════════════════════════════",
            f"📊 Drama Title Validator – Execution Report{force_check_str}",
            "══════════════════════════════════════════════════════", "",
            f"🚀 Workflow Type : {trigger_type}",
            f"🔁 RUN           : {run_display}",
            f"⏰ Start Time    : {start_time_str}",
            f"⏰ End Time      : {end_time_ist}",
            f"{run_label}{runtime_str}",
            f"⚙️ Max Process   : {max_fetches} Row Per Run",
            batch_label, ""
        ]

        total_skipped = total_recs = total_perfect = total_fixed = total_not_found = 0
        
        for sheet, data in rep_data.items():
            if not any([data["new_recs"], data["perfect"], data["user_fixed"], data["not_found_asian"], data["not_found_non_asian"], data["skipped"]]): continue
            lines.extend(["━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", f"🗂️ === {sheet} ===", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"])
            
            if data["new_recs"]:
                lines.append("\n✨ Brand New Recommendations Found (Action Required):")
                for i in unique_list(data["new_recs"]): lines.append(i)
                total_recs += len(unique_list(data["new_recs"]))
                
            if data["perfect"] > 0:
                lines.append(f"\n✅ Perfect Matches (Newly Scanned - 100% Accurate!):\n- {data['perfect']} records matched perfectly.")
                total_perfect += data["perfect"]
                
            if data["user_fixed"]:
                lines.append("\n🔄 User Fixed & Re-Verified (You changed these in Excel!):")
                for i in unique_list(data["user_fixed"]): lines.append(i)
                total_fixed += len(unique_list(data["user_fixed"]))

            if data["not_found_asian"]:
                lines.append("\n⚠️ Not Found (Asian / MyDramaList):")
                for i in unique_list(data["not_found_asian"]): lines.append(i)
                total_not_found += len(unique_list(data["not_found_asian"]))
                
            if data["not_found_non_asian"]:
                lines.append("\n⚠️ Not Found (Non-Asian / IMDb):")
                for i in unique_list(data["not_found_non_asian"]): lines.append(i)
                total_not_found += len(unique_list(data["not_found_non_asian"]))
                
            if data["skipped"] > 0:
                lines.append(f"\n⏭️ Skipped Fast (Already Verified Previously):\n- {data['skipped']} dramas skipped instantly.")
                total_skipped += data["skipped"]
            lines.append("")

        summary_title = "📊 Overall Cumulative Summary" if is_cumulative else "📊 Summary (Current Batch Only)"
        internet_scanned = total_recs + total_perfect + total_not_found
        
        lines.extend([
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", summary_title, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"🌐 Internet Scans Done   : {internet_scanned}",
            f"⏭️ Total Skipped Fast    : {total_skipped}",
            f"✨ Total Recommendations : {total_recs}",
            f"✅ Total Perfect Matches : {total_perfect}",
            f"🔄 Total User Fixes Seen : {total_fixed}",
            f"⚠️ Total Not Found       : {total_not_found}"
        ])

        if is_paused and not is_cumulative:
            lines.extend(["", "⚠️ BATCH LIMIT REACHED: The script paused safely.", "GitHub Actions will trigger next run automatically."])
        elif is_cumulative or not is_paused:
            lines.extend(["", "🏁 Workflow finished successfully"])

        return "\n".join(lines)

    console_output = build_report_text(current_report, is_cumulative=False)
    print(console_output)

    step_summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if step_summary:
        with open(step_summary, 'a', encoding='utf-8') as f:
            f.write(f"### 📊 Title Validation Output (Run: {current_gh_run})\n```text\n" + console_output + "\n```\n")

    os.makedirs(REPORTS_DIR, exist_ok=True)
    ts = now_ist().strftime("%d_%B_%Y_%H%M")
    
    cumulative_report = combine_reports(state.get("report_data", {}), current_report)

    if is_paused:
        report_name = f"{ts}_TITLE_CHECK_PARTIAL_{current_gh_run}_REPORT.txt"
        report_path = os.path.join(REPORTS_DIR, report_name)
        with open(report_path, "w", encoding="utf-8") as f: f.write(console_output)
    else:
        first_run = state.get('first_run_id', current_gh_run)
        report_name = f"{ts}_TITLE_CHECK_FINAL_{first_run}-{current_gh_run}_REPORT.txt" if str(first_run) != str(current_gh_run) else f"{ts}_TITLE_CHECK_FINAL_{current_gh_run}_REPORT.txt"
        report_path = os.path.join(REPORTS_DIR, report_name)
        file_output = build_report_text(cumulative_report, is_cumulative=True)
        with open(report_path, "w", encoding="utf-8") as f: f.write(file_output)

    mail_date = now_ist().strftime("%d %B %Y %I:%M %p IST")
    email_subject = f"[{trigger_type}] Title Validation {mail_date} Report"
    with open("EMAIL_SUBJECT.txt", "w", encoding='utf-8') as ef: ef.write(email_subject)

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
            "sheet_idx": 0, "row_idx": 0, "report_data": {},
            "global_start_time": run_start_time.strftime("%d %B %Y - %I:%M:%S %p"),
            "cumulative_time_seconds": 0,
            "first_run_id": current_gh_run, "batch_run_count": 1
        }

    gc = gspread.service_account(filename="GDRIVE_SERVICE_ACCOUNT.json")
    with open("EXCEL_FILE_ID.txt", "r") as f: main_excel_id = f.read().strip()

    excel_bytes = fetch_excel_from_gdrive_bytes(main_excel_id, "GDRIVE_SERVICE_ACCOUNT.json")
    xl = pd.ExcelFile(io.BytesIO(excel_bytes.getvalue()))

    # --- RETRY LOGIC ADDED FOR INITIALIZING GOOGLE SHEETS ---
    for attempt in range(3):
        try:
            check_sh = gc.open_by_key(os.environ.get("CHECK_TITLES_EXCEL_ID"))
            try:
                ws_out = check_sh.worksheet("Check Titles")
                existing_df = get_as_dataframe(ws_out, evaluate_formulas=True).dropna(how="all")
            except gspread.exceptions.WorksheetNotFound:
                ws_out = check_sh.add_worksheet(title="Check Titles", rows="1000", cols="20")
                existing_df = pd.DataFrame()
            break
        except Exception as e:
            if attempt < 2:
                print(f"⚠️ Google API Connection Issue (Init). Retrying in 5 seconds...")
                time.sleep(5)
            else:
                raise e

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
    current_report = {}
    processed_ids_this_run = set()
    
    for s_idx in range(state["sheet_idx"], len(sheets_to_process)):
        if limit_reached: break
        
        sheet_name = sheets_to_process[s_idx]
        report_sheet = current_report.setdefault(sheet_name, {"new_recs":[], "perfect": 0, "user_fixed":[], "not_found_asian":[], "not_found_non_asian":[], "skipped": 0})

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
                
                # Dynamic fetch for Total Episodes
                eps_val = row.get("Total Episodes", 0)
                try: expected_eps = int(eps_val)
                except ValueError: expected_eps = 0

            except ValueError: continue

            if sid == 0 or not title or title.lower() == "nan": continue
            
            cache_key = f"{sheet_name}_{sid}"
            if cache_key in processed_ids_this_run: continue
            processed_ids_this_run.add(cache_key)

            cached_data = cache.get(cache_key)

            # --- SMART CACHE / SKIP LOGIC ---
            if cached_data and not FORCE_CHECK:
                cached_title = cached_data["Show Name"]
                cached_rec = cached_data["Recommended Title Name"]
                
                if title == cached_title:
                    report_sheet["skipped"] += 1
                    continue
                
                if cached_rec != "N/A" and title == cached_rec:
                    report_sheet["user_fixed"].append(f"-[ID {sid}] **{cached_title}** -> Now perfectly matches: {title}")
                    
                    updated_row = cached_data["row_data"].copy()
                    updated_row["Show Name"] = title
                    updated_row["Title Recommendation"] = "No"
                    updated_row["Recommended Title Name"] = "N/A"
                    updated_row["Last Update Date"] = TODAY_DATE
                    results.append(updated_row)
                    continue

            # --- INTERNET FETCHING ---
            fetches_used += 1

            is_asian = lang.lower() in ["korean", "chinese", "japanese", "thai", "taiwanese", "filipino"]
            mdl_title, imdb_title = "N/A", "N/A"
            source_used = ""
            rec_title = "N/A"
            source_link = "N/A"
            reason = ""

            if is_asian:
                mdl_title, source_used, source_link, reason = search_and_verify_title(title, year, lang, expected_eps, sheet_name, "mydramalist")
                if mdl_title != "N/A":
                    rec_title = mdl_title
            else:
                imdb_title, source_used, source_link, reason = search_and_verify_title(title, year, lang, expected_eps, sheet_name, "imdb")
                if imdb_title != "N/A":
                    rec_title = imdb_title
                    
            if source_used == "mydramalist": source_used = "MyDramaList"
            if source_used == "imdb": source_used = "IMDb"

            # --- STRICT MATCHING LOGIC ---
            if rec_title == "N/A":
                needs_rec = "No" 
                msg = f"-[ID {sid}] **{title}** -> Not Found `(Reason: {reason})`"
                if is_asian:
                    report_sheet["not_found_asian"].append(msg)
                else:
                    report_sheet["not_found_non_asian"].append(msg)
            elif rec_title == title:
                needs_rec = "No"
                rec_title = "N/A"
                report_sheet["perfect"] += 1
            else:
                needs_rec = "Yes"
                report_sheet["new_recs"].append(f"-[ID {sid}] **{title}** ➔ Recommend: {rec_title} (Source: {source_used})")

            results.append({
                "Sheet Name": sheet_name, 
                "Show ID": sid, 
                "Show Name": title,
                "Released Year": year, 
                "Recommended Title Name": rec_title,
                "Source Link": source_link,
                "Language": lang, 
                "Last Update Date": TODAY_DATE,
                "mydramalist": mdl_title, 
                "imdb": imdb_title, 
                "Title Recommendation": needs_rec
            })

    if results:
        new_df = pd.DataFrame(results)
        
        ordered_cols =[
            "Sheet Name", "Show ID", "Show Name", "Released Year", 
            "Recommended Title Name", "Source Link", "Language", 
            "Last Update Date", "mydramalist", "imdb", "Title Recommendation"
        ]
        
        for col in ordered_cols:
            if col not in new_df.columns:
                new_df[col] = "N/A"
        new_df = new_df[ordered_cols]

        if not existing_df.empty and "Sheet Name" in existing_df.columns:
            for col in ordered_cols:
                if col not in existing_df.columns:
                    existing_df[col] = "N/A"
            existing_df = existing_df[ordered_cols]
            
            existing_df["Show ID"] = pd.to_numeric(existing_df["Show ID"], errors="coerce")
            new_df["Show ID"] = pd.to_numeric(new_df["Show ID"], errors="coerce")
            existing_df.set_index(["Sheet Name", "Show ID"], inplace=True)
            new_df.set_index(["Sheet Name", "Show ID"], inplace=True)
            combined_df = new_df.combine_first(existing_df).reset_index()
        else:
            combined_df = new_df
        
        combined_df = combined_df[ordered_cols]
        
        # --- RETRY LOGIC ADDED FOR SAVING TO GOOGLE SHEETS ---
        for attempt in range(3):
            try:
                ws_out.clear()
                set_with_dataframe(ws_out, combined_df.fillna("N/A"))
                break # Success!
            except Exception as e:
                if attempt < 2:
                    print(f"⚠️ Google API Connection Issue (Write). Retrying in 5 seconds...")
                    time.sleep(5)
                else:
                    raise e # Re-raise error if it fails 3 times

    current_run_seconds = (now_ist() - run_start_time).total_seconds()
    state["report_data"] = combine_reports(state.get("report_data", {}), current_report)

    if limit_reached:
        state["batch_run_count"] += 1
        state["cumulative_time_seconds"] += current_run_seconds
        with open("RESUME_FLAG.txt", "w") as f: f.write("CONTINUE")
        with open(STATE_FILE, "w", encoding="utf-8") as f: json.dump(state, f)
    else:
        if os.path.exists(STATE_FILE): os.remove(STATE_FILE)

    write_report(current_report, state, current_run_seconds, run_start_time, limit_reached, MAX_FETCHES, fetches_used)

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