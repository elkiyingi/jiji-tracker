-- ═══════════════════════════════════════════════════════════════
-- Jiji Deals v2 (Fixed) — Supabase / PostgreSQL Schema
-- ═══════════════════════════════════════════════════════════════

create extension if not exists moddatetime schema extensions;

create table if not exists public.jiji_deals (
    id                bigserial       primary key,
    title             text            not null,
    price             bigint,                             
    location          text,
    image_url         text,
    ad_url            text            not null unique,    
    category          text            not null check (category in ('cars', 'land')),
    query             text,                               
    seller_name       text,
    seller_ad_count   int             not null default 1, 
    market_price_low  bigint,                             
    market_price_high bigint,                             
    is_deal           boolean         not null default false,
    deal_reason       text,
    is_pinned         boolean         not null default false,  
    pin_note          text,                                    
    is_archived       boolean         not null default false,  
    created_at        timestamptz     not null default now(),
    updated_at        timestamptz     not null default now()
);

create trigger handle_updated_at
before update on public.jiji_deals
for each row execute procedure moddatetime(updated_at);

create index if not exists idx_jd_category    on public.jiji_deals (category);
create index if not exists idx_jd_price        on public.jiji_deals (price);
create index if not exists idx_jd_is_deal      on public.jiji_deals (is_deal);
create index if not exists idx_jd_is_archived  on public.jiji_deals (is_archived);
create index if not exists idx_jd_created_at   on public.jiji_deals (created_at desc);

create index if not exists idx_jd_title_fts
    on public.jiji_deals
    using gin (to_tsvector('english', title));

alter table public.jiji_deals enable row level security;

-- Public read only (anon key)
create policy "Allow public read"
    on public.jiji_deals for select
    using (true);

-- Service role can insert (scraper backend)
create policy "Allow service insert"
    on public.jiji_deals for insert
    with check (true);

-- REMOVED: Vulnerable "Allow public update curation fields" policy.
-- The frontend is exclusively using localStorage for favorites and notes.

create or replace view public.jiji_hot_deals as
select *
from public.jiji_deals
where is_deal = true
  and is_archived = false
order by created_at desc;

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
