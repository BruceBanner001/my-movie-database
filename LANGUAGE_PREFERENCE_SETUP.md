# 🌍 Language & Source Logic
**Author:** BRUCE

This document explains how the automation engine handles different languages and site priorities for your movie database.

## 1. Scraping Priorities (Example)
The script automatically chooses the best website to find details based on the show's native language:

| Language | Primary Source | Secondary Source |
| :--- | :--- | :--- |
| **Korean** | AsianWiki | MyDramaList |
| **Japanese** | AsianWiki | MyDramaList |
| **Chinese/Thai** | MyDramaList | N/A |

**Example:** If you add a Korean drama, the script will first try to get the "Synopsis" from AsianWiki. If it fails, it falls back to MyDramaList.

## 2. Multi-Language Support (English & Tamil)
The engine is optimized for a global audience while maintaining your personal preferences:
* **Metadata Fetching:** All technical details (Cast, Synopsis, Duration) are fetched in **English**.
* **Tamil Character Support:** You can safely write in **Tamil** inside the `Comments` or `Other Names` columns in your Excel sheet. 
* **Encoding:** The script uses `utf-8` encoding, meaning your Tamil text will be saved perfectly in `seriesData.json` without turning into garbled text.

## 3. Country Mapping Logic
The script automatically fills in the "Country" field based on the language:
* **"Korean"** ➔ South Korea
* **"Chinese"** ➔ China
* **"Japanese"** ➔ Japan