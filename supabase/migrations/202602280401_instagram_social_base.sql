create table if not exists jne.candidato_redes_sociales (
    id bigserial primary key,
    id_hoja_vida bigint not null references jne.candidatos(id_hoja_vida) on delete cascade,
    plataforma text not null check (plataforma in ('instagram')),
    username text not null,
    profile_url text not null,
    source text not null default 'manual' check (source in ('manual', 'auto_discovery', 'api_onboarded')),
    is_oficial boolean not null default false,
    is_public boolean null,
    notes text null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (id_hoja_vida, plataforma, username)
);

create index if not exists idx_candidato_redes_sociales_hoja on jne.candidato_redes_sociales(id_hoja_vida);
create index if not exists idx_candidato_redes_sociales_platform on jne.candidato_redes_sociales(plataforma);

create table if not exists jne.instagram_ingesta_runs (
    id uuid primary key default gen_random_uuid(),
    id_hoja_vida bigint null references jne.candidatos(id_hoja_vida) on delete set null,
    username text null,
    mode text not null check (mode in ('discovery', 'onboarded')),
    status text not null check (status in ('running', 'failed', 'completed')),
    started_at timestamptz not null default now(),
    finished_at timestamptz null,
    metrics jsonb not null default '{}'::jsonb,
    error_message text null
);

create index if not exists idx_instagram_ingesta_runs_hoja on jne.instagram_ingesta_runs(id_hoja_vida);
create index if not exists idx_instagram_ingesta_runs_status on jne.instagram_ingesta_runs(status);

create table if not exists jne.instagram_profiles_snapshot (
    id bigserial primary key,
    run_id uuid null references jne.instagram_ingesta_runs(id) on delete set null,
    id_hoja_vida bigint not null references jne.candidatos(id_hoja_vida) on delete cascade,
    username text not null,
    ig_user_id text null,
    account_type text null,
    display_name text null,
    biography text null,
    website text null,
    profile_picture_url text null,
    followers_count integer null,
    follows_count integer null,
    media_count integer null,
    payload jsonb not null,
    captured_at timestamptz not null default now()
);

create index if not exists idx_instagram_profiles_snapshot_lookup
    on jne.instagram_profiles_snapshot(id_hoja_vida, username, captured_at desc);

create table if not exists jne.instagram_media_snapshot (
    id bigserial primary key,
    run_id uuid null references jne.instagram_ingesta_runs(id) on delete set null,
    id_hoja_vida bigint not null references jne.candidatos(id_hoja_vida) on delete cascade,
    username text not null,
    media_id text not null,
    media_type text null,
    media_product_type text null,
    caption text null,
    permalink text null,
    media_url text null,
    thumbnail_url text null,
    timestamp_utc timestamptz null,
    comments_count integer null,
    like_count integer null,
    view_count integer null,
    payload jsonb not null,
    captured_at timestamptz not null default now()
);

create index if not exists idx_instagram_media_snapshot_lookup
    on jne.instagram_media_snapshot(id_hoja_vida, username, captured_at desc);
create index if not exists idx_instagram_media_snapshot_media_id
    on jne.instagram_media_snapshot(media_id);

create or replace view jne.v_instagram_profile_latest as
select distinct on (id_hoja_vida, username)
    id,
    run_id,
    id_hoja_vida,
    username,
    ig_user_id,
    account_type,
    display_name,
    biography,
    website,
    profile_picture_url,
    followers_count,
    follows_count,
    media_count,
    payload,
    captured_at
from jne.instagram_profiles_snapshot
order by id_hoja_vida, username, captured_at desc, id desc;

create or replace view jne.v_instagram_media_latest as
select distinct on (id_hoja_vida, media_id)
    id,
    run_id,
    id_hoja_vida,
    username,
    media_id,
    media_type,
    media_product_type,
    caption,
    permalink,
    media_url,
    thumbnail_url,
    timestamp_utc,
    comments_count,
    like_count,
    view_count,
    payload,
    captured_at
from jne.instagram_media_snapshot
order by id_hoja_vida, media_id, captured_at desc, id desc;
