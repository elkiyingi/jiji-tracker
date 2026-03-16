-- ═══════════════════════════════════════════════════════════════
-- Jiji Deals v2 — Supabase / PostgreSQL Schema
-- Run this in Supabase Dashboard → SQL Editor
-- ═══════════════════════════════════════════════════════════════

-- Auto-update extension
create extension if not exists moddatetime schema extensions;

-- ─── Main table ───────────────────────────────────────────────
create table if not exists public.jiji_deals (
    id                bigserial       primary key,

    -- Core listing data
    title             text            not null,
    price             bigint,                             -- UGX integer
    location          text,
    image_url         text,
    ad_url            text            not null unique,    -- prevents duplicates
    category          text            not null check (category in ('cars', 'land')),
    query             text,                               -- which search query found it

    -- Seller info (from detail page)
    seller_name       text,
    seller_ad_count   int             not null default 1, -- ≤3 = private seller

    -- Jiji's own market price range (from detail page)
    market_price_low  bigint,                             -- floor of Jiji range
    market_price_high bigint,                             -- ceiling of Jiji range

    -- Deal flags
    is_deal           boolean         not null default false,
    deal_reason       text,

    -- User curation
    is_pinned         boolean         not null default false,  -- manually pinned
    pin_note          text,                                    -- user's personal note
    is_archived       boolean         not null default false,  -- soft-delete / dismissed

    -- Timestamps
    created_at        timestamptz     not null default now(),
    updated_at        timestamptz     not null default now()
);

-- Auto-update updated_at
create trigger handle_updated_at
before update on public.jiji_deals
for each row execute procedure moddatetime(updated_at);

-- ─── Indexes ──────────────────────────────────────────────────
create index if not exists idx_jd_category    on public.jiji_deals (category);
create index if not exists idx_jd_price        on public.jiji_deals (price);
create index if not exists idx_jd_is_deal      on public.jiji_deals (is_deal);
create index if not exists idx_jd_is_pinned    on public.jiji_deals (is_pinned);
create index if not exists idx_jd_is_archived  on public.jiji_deals (is_archived);
create index if not exists idx_jd_seller_count on public.jiji_deals (seller_ad_count);
create index if not exists idx_jd_created_at   on public.jiji_deals (created_at desc);

-- Full-text search on title
create index if not exists idx_jd_title_fts
    on public.jiji_deals
    using gin (to_tsvector('english', title));

-- ─── Row Level Security ───────────────────────────────────────
alter table public.jiji_deals enable row level security;

-- Public read (anon key used by frontend)
create policy "Allow public read"
    on public.jiji_deals for select
    using (true);

-- Service role can insert (scraper)
create policy "Allow service insert"
    on public.jiji_deals for insert
    with check (true);

-- Allow frontend to update pinned/note/archived via anon key
-- (tighten this to specific columns via a function if you want stricter security)
create policy "Allow public update curation fields"
    on public.jiji_deals for update
    using (true)
    with check (true);

-- ─── Useful views ─────────────────────────────────────────────

-- All confirmed deals (below Jiji market floor, private seller)
create or replace view public.jiji_hot_deals as
select *
from public.jiji_deals
where is_deal = true
  and is_archived = false
order by created_at desc;

-- User's pinned / watchlist
create or replace view public.jiji_watchlist as
select *
from public.jiji_deals
where is_pinned = true
  and is_archived = false
order by updated_at desc;

-- Price comparison helper: shows deal discount vs market floor
create or replace view public.jiji_deals_with_discount as
select *,
    case
        when price is not null and market_price_low is not null and market_price_low > 0
        then round(((market_price_low::numeric - price) / market_price_low) * 100, 1)
        else null
    end as discount_pct
from public.jiji_deals
where is_archived = false
order by discount_pct desc nulls last;

-- ─── Sanity check (run manually) ──────────────────────────────
-- select category, count(*), avg(price), avg(seller_ad_count)
-- from public.jiji_deals
-- group by category;
