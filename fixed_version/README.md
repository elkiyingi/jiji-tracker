# Jiji.ug Deal Tracker v2 (Fixed Version)

Private-seller deal monitor for Cars and Land on Jiji.ug.

## Security Fixes & Changes

- Fixed vulnerability in `index.html` allowing public database edits via Anon Key.
- Repaired `scraper.py` to correctly extract ad numbers from the detailed page and block large dealerships automatically.
- Fixed 1,000-ad re-scraping loop issue by adding `limit(3000)` to the DB query.
- Increased `MAX_SEARCH_PAGES` to capture slightly more deals without exhausting the FlareSolverr timeout.

## File structure

```
├── scraper.py                     # Main scraper (Playwright + BS4)
├── database.sql                   # Supabase schema (run once)
├── requirements.txt               # Python deps
├── index.html                     # Frontend dashboard (GitHub Pages)
└── .github/workflows/scrape.yml   # Hourly GitHub Actions job
```
