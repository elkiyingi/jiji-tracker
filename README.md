# Jiji.ug Deal Tracker v2

Private-seller deal monitor for Cars and Land on Jiji.ug.

## How the deal logic works

1. **Scraper collects search results** for all configured queries.
2. **For each new ad**, the scraper visits the individual listing page to:
   - Read Jiji's own **"Market price: USh X ~ Y"** range displayed on the listing.
   - Read the **seller's total ad count** (visible on the listing page).
3. **Broker filter**: any seller with **more than 3 total ads** is silently skipped.
4. **Deal flag**: if the listing price is **below Jiji's market floor** (the low end of their range), it's marked as a deal and a Telegram alert fires.

## File structure

```
├── scraper.py                     # Main scraper (Playwright + BS4)
├── database.sql                   # Supabase schema (run once)
├── requirements.txt               # Python deps
├── index.html                     # Frontend dashboard (GitHub Pages)
└── .github/workflows/scrape.yml   # Hourly GitHub Actions job
```

## Setup

### 1 — Supabase
1. Create a project at https://supabase.com
2. SQL Editor → paste and run `database.sql`
3. Settings → API → copy **Project URL**, **anon key**, and **service_role key**

### 2 — Telegram Bot
1. Message `@BotFather` → `/newbot` → copy the token
2. Start a chat with your bot (or add to a channel)
3. Get your chat ID:
   - Personal: message `@userinfobot`
   - Channel: `https://api.telegram.org/bot<TOKEN>/getUpdates` after sending a message

### 3 — GitHub Secrets
In your repo → Settings → Secrets and variables → Actions:

| Secret              | Value                                       |
|---------------------|---------------------------------------------|
| `SUPABASE_URL`      | `https://xxxx.supabase.co`                  |
| `SUPABASE_KEY`      | **service_role** key (scraper needs writes) |
| `TELEGRAM_TOKEN`    | Your bot token                              |
| `TELEGRAM_CHAT_ID`  | Your chat/channel ID                        |

### 4 — Frontend
1. Open `index.html`, replace:
   ```js
   const SUPABASE_URL      = "https://YOUR_PROJECT_ID.supabase.co";
   const SUPABASE_ANON_KEY = "YOUR_SUPABASE_ANON_KEY";
   ```
   Use the **anon** key here (read-only, safe for public pages).
2. Settings → Pages → Source: main branch / root
3. Live at `https://<username>.github.io/<repo>/`

### 5 — Test locally
```bash
pip install -r requirements.txt
python -m playwright install chromium

export SUPABASE_URL="https://xxxx.supabase.co"
export SUPABASE_KEY="your-service-role-key"
export TELEGRAM_TOKEN="your-token"
export TELEGRAM_CHAT_ID="your-chat-id"

python scraper.py
```

## Extending queries

In `scraper.py`:
```python
SEARCH_QUERIES = [
    {"query": "Toyota RAV4",   "category": "cars"},
    {"query": "Gayaza",        "category": "land"},
    # add more here
]
```

## Dashboard features

| Feature | Where |
|---|---|
| Filter by category (Cars / Land) | Filters bar |
| Search by title keyword | Filters bar |
| Max price slider | Filters bar |
| Sort by newest / price / biggest discount | Filters bar |
| Hot Deals tab (auto-archived) | Tab nav |
| Watchlist tab (manually pinned) | Tab nav |
| Pin any ad + add a personal note | Each card |
| Market price range bar on each card | Each card |
| Seller name + ad count | Each card |
| Discount % vs Jiji market floor | Each card |
