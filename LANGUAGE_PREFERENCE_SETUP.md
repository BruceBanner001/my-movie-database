# Adding New Language or Site Preferences

The script `create_update_backup_delete.py` fetches synopsis and images by checking site preferences per language.

## Where to add preferences
In the script, look for the section where `site_prefs` is defined:
```python
site_prefs = {
    "Korean": ["asianwiki", "mydramalist"],
    "Chinese": ["mydramalist", "baidu"],
    # Add more languages here...
}
```

## Steps to add a new language
1. Open `create_update_backup_delete.py`.
2. Find the `site_prefs` dictionary.
3. Add a new entry, e.g.:
   ```python
   "Japanese": ["mydramalist", "asianwiki"],
   ```
4. Save and commit.

## Adding site-specific parsing
- In `parse_synopsis_from_html`, add new rules under:
  ```python
  if "asianwiki" in url:
      # existing parser
  elif "mydramalist" in url:
      # existing parser
  elif "newsite" in url:
      # implement parser for the new site
  ```

## Notes
- Always test after adding preferences.
- If the site has a clear `div` or `p` for synopsis, use BeautifulSoup to extract it.
- Append a `(Source: SiteName)` suffix if not automatically included.
