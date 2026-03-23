import os, time, re, json, sys, traceback
from datetime import datetime, timedelta, timezone
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import cloudscraper

try:
    from duckduckgo_search import DDGS
except ImportError:
    pass

# Setup Timezone (IST)
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    return datetime.now(IST)

TODAY_DATE = now_ist().strftime("%d-%m-%Y")

# Setup Scraper
SCRAPER = cloudscraper.create_scraper()
SCRAPER.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})

STATE_FILE = "title_validator_state.json"

def normalize_title(t):
    if not t or str(t).strip() == "N/A":
        return ""
    return re.sub(r"[^a-z0-9]", "", str(t).lower().strip())

def search_and_get_title(search_term, year, site):
    clean_search = re.sub(
        r"\b(?:Season|Part|S)\s*\d+\b|\s+\d+$",
        "",
        str(search_term),
        flags=re.IGNORECASE,
    ).strip()
    queries =[
        f'"{search_term}" {year} site:{site}.com',
        f'"{clean_search}" {year} site:{site}.com',
    ]
    try:
        with DDGS() as dd:
            for query in queries:
                results = list(dd.text(query, max_results=3))
                for res in results:
                    url = res.get("href", "")
                    if any(
                        bad in url
                        for bad in[
                            "/reviews",
                            "/recs",
                            "/photos",
                            "?lang=",
                            "/characters",
                            "/episodes",
                        ]
                    ):
                        continue

                    r = SCRAPER.get(url, timeout=10)
                    if r.status_code == 200:
                        soup = BeautifulSoup(r.text, "html.parser")
                        title = None
                        if site == "asianwiki":
                            h1 = soup.find("h1", class_="firstHeading")
                            if h1:
                                title = h1.get_text(strip=True).replace(" (Drama)", "")
                        elif site == "mydramalist":
                            h1 = soup.find("h1", class_="film-title")
                            if h1:
                                title = h1.get_text(strip=True)
                        elif site == "imdb":
                            h1 = soup.find("h1")
                            if h1:
                                title = h1.get_text(strip=True)

                        if title:
                            title = re.sub(r"\s*\(\d{4}\)$", "", title).strip()
                            return title
                time.sleep(3)  # Safe delay to prevent DDG Rate Limits
    except Exception as e:
        print(f"Error searching {site}: {e}")
    return "N/A"

def main():
    print(f"🚀 Starting Title Validation on {TODAY_DATE} (IST)")

    # Batch Limits Setup
    MAX_FETCHES = int(os.environ.get("MAX_FETCHES", "50"))
    fetches = 0
    limit_reached = False

    # Load Relay-Race State
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
        print(
            f"📂 Resuming from Sheet Index: {state['sheet_idx']}, Row: {state['row_idx']}"
        )
    else:
        state = {
            "sheet_idx": 0,
            "row_idx": 0,
            "email_data": {"perfect":[], "updated": [], "new_rec":[]},
            "total_scanned": 0,
        }

    # Authenticate with Google Sheets
    gc = gspread.service_account(filename="GDRIVE_SERVICE_ACCOUNT.json")

    with open("EXCEL_FILE_ID.txt", "r") as f:
        main_excel_id = f.read().strip()
    
    # Open both sheets
    main_sh = gc.open_by_key(main_excel_id)
    check_excel_id = os.environ.get("CHECK_TITLES_EXCEL_ID")
    check_sh = gc.open_by_key(check_excel_id)

    # Prepare Outbound Sheet
    try:
        ws_out = check_sh.worksheet("Check Titles")
        existing_df = get_as_dataframe(ws_out, evaluate_formulas=True).dropna(how="all")
    except gspread.exceptions.WorksheetNotFound:
        ws_out = check_sh.add_worksheet(title="Check Titles", rows="1000", cols="20")
        existing_df = pd.DataFrame()

    past_history = {}
    if not existing_df.empty and "Sheet Name" in existing_df.columns:
        for _, row in existing_df.iterrows():
            key = f"{row.get('Sheet Name')}_{row.get('Show ID')}"
            past_history[key] = {
                "rec_title": str(row.get("Recommended Title Name", "N/A")),
                "last_date": str(row.get("Last Update Date", TODAY_DATE)),
            }

    sheets_to_process =[
        s.strip()
        for s in os.environ.get("SHEETS", "Sheet1;Sheet2").split(";")
        if s.strip()
    ]
    results = []
    email_data = state["email_data"]

    for s_idx in range(state["sheet_idx"], len(sheets_to_process)):
        if limit_reached:
            break
        sheet_name = sheets_to_process[s_idx]

        try:
            ws_in = main_sh.worksheet(sheet_name)
            df_in = get_as_dataframe(ws_in).dropna(how="all", subset=["Show ID", "No"])
        except Exception:
            continue

        # Resume from the exact row where the last batch left off
        start_r = state["row_idx"] if s_idx == state["sheet_idx"] else 0

        for r_idx in range(start_r, len(df_in)):
            row = df_in.iloc[r_idx]
            try:
                sid = int(row.get("No") or row.get("Show ID", 0))
                title = str(row.get("Series Title") or row.get("Show Name", "")).strip()
                year = int(row.get("Year") or row.get("Released Year", 0))
                lang = (
                    str(
                        row.get("Original Language")
                        or row.get("Native Language", "Korean")
                    )
                    .strip()
                    .capitalize()
                )
            except ValueError:
                continue

            if sid == 0 or not title or title.lower() == "nan":
                continue

            # Increment trackers
            fetches += 1
            state["total_scanned"] += 1
            print(f"🔍 Checking [{fetches}/{MAX_FETCHES}]: {title} ({year})")

            aw_title, mdl_title, imdb_title = "N/A", "N/A", "N/A"
            is_asian = lang.lower() in[
                "korean",
                "chinese",
                "japanese",
                "thai",
                "taiwanese",
                "filipino",
            ]

            if is_asian:
                mdl_title = search_and_get_title(title, year, "mydramalist")
                if lang.lower() == "korean":
                    aw_title = search_and_get_title(title, year, "asianwiki")
            else:
                imdb_title = search_and_get_title(title, year, "imdb")

            norm_orig = normalize_title(title)
            can_aw = (
                (norm_orig == normalize_title(aw_title)) if aw_title != "N/A" else "N/A"
            )
            can_mdl = (
                (norm_orig == normalize_title(mdl_title))
                if mdl_title != "N/A"
                else "N/A"
            )
            can_imdb = (
                (norm_orig == normalize_title(imdb_title))
                if imdb_title != "N/A"
                else "N/A"
            )

            rec_title = "N/A"
            if is_asian:
                if mdl_title != "N/A":
                    rec_title = mdl_title
                elif aw_title != "N/A":
                    rec_title = aw_title
            else:
                if imdb_title != "N/A":
                    rec_title = imdb_title

            needs_rec = "No" if normalize_title(rec_title) == norm_orig else "Yes"
            if needs_rec == "No":
                rec_title = "N/A"

            key = f"{sheet_name}_{sid}"
            last_date = TODAY_DATE

            if key in past_history:
                past_rec = past_history[key]["rec_title"]
                if past_rec != "nan" and past_rec != "N/A":
                    if needs_rec == "Yes" and rec_title != past_rec:
                        last_date = TODAY_DATE
                        email_data["updated"].append(
                            f"[{sheet_name} - ID {sid}] **{title}**\n   *Old:* {past_rec}\n   *New:* {rec_title}"
                        )
                    else:
                        last_date = past_history[key]["last_date"]
                        if needs_rec == "No":
                            email_data["perfect"].append(title)
                else:
                    if needs_rec == "Yes":
                        last_date = TODAY_DATE
                        email_data["new_rec"].append(
                            f"[{sheet_name} - ID {sid}] **{title}** ➔ Recommend: {rec_title}"
                        )
                    else:
                        last_date = past_history[key]["last_date"]
                        email_data["perfect"].append(title)
            else:
                if needs_rec == "Yes":
                    email_data["new_rec"].append(
                        f"[{sheet_name} - ID {sid}] **{title}** ➔ Recommend: {rec_title}"
                    )
                else:
                    email_data["perfect"].append(title)

            results.append(
                {
                    "Sheet Name": sheet_name,
                    "Show ID": sid,
                    "Show Name": title,
                    "Released Year": year,
                    "Language": lang,
                    "Last Update Date": last_date,
                    "asianwiki": aw_title,
                    "Can find from asianwiki": can_aw,
                    "mydramalist": mdl_title,
                    "Can find from mydramalist": can_mdl,
                    "imdb": imdb_title,
                    "Can find from imdb": can_imdb,
                    "Title Recommendation": needs_rec,
                    "Recommended Title Name": rec_title,
                }
            )

            # Check if we hit the limit
            if fetches >= MAX_FETCHES:
                limit_reached = True
                state["sheet_idx"] = s_idx
                state["row_idx"] = r_idx + 1
                break

    # Write back using UPSERT
    if results:
        new_df = pd.DataFrame(results)
        if not existing_df.empty and "Sheet Name" in existing_df.columns:
            existing_df["Show ID"] = pd.to_numeric(
                existing_df["Show ID"], errors="coerce"
            )
            new_df["Show ID"] = pd.to_numeric(new_df["Show ID"], errors="coerce")

            existing_df.set_index(["Sheet Name", "Show ID"], inplace=True)
            new_df.set_index(["Sheet Name", "Show ID"], inplace=True)

            combined_df = new_df.combine_first(existing_df).reset_index()
        else:
            combined_df = new_df

        ws_out.clear() 
        set_with_dataframe(ws_out, combined_df.fillna("N/A"))
        print("✅ Successfully upserted this batch to the Google Sheet!")

    # Relay-Race Handling
    if limit_reached:
        print(
            f"🛑 Batch limit reached ({MAX_FETCHES} fetches). Saving state to resume next run."
        )
        with open("RESUME_FLAG.txt", "w") as f:
            f.write("CONTINUE_NEXT_BATCH")
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f)

        # Batch Output Body
        with open("email_body.txt", "w", encoding="utf-8") as f:
            f.write(
                f"⏳ Title Validation is running in BATCH MODE. Paused at {state['total_scanned']} scanned titles."
            )

    else:
        print("🏁 All scanning completely finished!")
        if os.path.exists(STATE_FILE):
            os.remove(STATE_FILE)  # Clean up state

        # Generate Final Email Body
        total = state["total_scanned"]
        body = f"Hello!\n\nThe Title Verification workflow has successfully finished scanning your sheets.\n\n"
        body += f"📊 Scan Summary:\n- Total Titles Scanned: {total}\n"
        body += f"- Perfect Matches (No change needed): {len(email_data['perfect'])}\n"
        body += f"- New Recommendations Found: {len(email_data['new_rec'])}\n"
        body += f"- Titles Updated Since Last Run: {len(email_data['updated'])}\n\n"

        if email_data["updated"]:
            body += (
                "🔄 Titles Changed/Updated Today:\n"
                + "\n".join(email_data["updated"])
                + "\n\n"
            )
        if email_data["new_rec"]:
            body += (
                "✨ Brand New Recommendations Added:\n"
                + "\n".join([f"- {i}" for i in email_data["new_rec"]])
                + "\n\n"
            )
        if email_data["perfect"]:
            body += "✅ No Action Needed (Titles are perfect):\n"
            body += (
                ", ".join(email_data["perfect"][:15])
                + (" ... and more." if len(email_data["perfect"]) > 15 else "")
                + "\n\n"
            )

        body += "All data has been saved to the 'Check Titles' sheet in your Google Excel file!"

        with open("email_body.txt", "w", encoding="utf-8") as f:
            f.write(body)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}")
        traceback.print_exc()
        
        # Guarantees the email action won't fail with ENOENT
        with open("email_body.txt", "w", encoding="utf-8") as f:
            f.write("❌ The Title Validation script encountered a critical error and crashed!\n\n")
            f.write(f"Error Message:\n{str(e)}\n\n")
            f.write("Please check your GitHub Actions log for the full traceback details.")
        
        sys.exit(1)