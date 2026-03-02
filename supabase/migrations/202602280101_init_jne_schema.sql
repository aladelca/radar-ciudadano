create extension if not exists pgcrypto;

create schema if not exists jne;

create table if not exists jne.ingesta_runs (
    id uuid primary key default gen_random_uuid(),
    process_id integer not null,
    tipo_eleccion_id integer null,
    status text not null check (status in ('running', 'failed', 'completed')),
    started_at timestamptz not null default now(),
    finished_at timestamptz null,
    candidates_read integer not null default 0,
    candidates_persisted integer not null default 0,
    errors_count integer not null default 0,
    metadata jsonb not null default '{}'::jsonb
);

create table if not exists jne.candidatos (
    id_hoja_vida bigint primary key,
    run_id uuid not null references jne.ingesta_runs(id),
    id_proceso_electoral integer not null,
    id_tipo_eleccion integer not null,
    id_organizacion_politica integer null,
    organizacion_politica text null,
    numero_documento text null,
    nombre_completo text null,
    cargo text null,
    estado text null,
    numero_candidato integer null,
    postula_departamento text null,
    postula_provincia text null,
    postula_distrito text null,
    tx_guid_archivo_origen text null,
    tx_guid_foto text null,
    tx_nombre_foto text null,
    raw_payload jsonb not null,
    updated_at timestamptz not null default now()
);

create index if not exists idx_candidatos_proceso_tipo on jne.candidatos(id_proceso_electoral, id_tipo_eleccion);
create index if not exists idx_candidatos_org on jne.candidatos(id_organizacion_politica);
create index if not exists idx_candidatos_estado on jne.candidatos(estado);

create table if not exists jne.hoja_vida_raw (
    id_hoja_vida bigint primary key references jne.candidatos(id_hoja_vida),
    run_id uuid not null references jne.ingesta_runs(id),
    payload_hash text not null,
    payload jsonb not null,
    fetched_at timestamptz not null default now()
);

create table if not exists jne.anotaciones_marginales (
    id_anotacion_marginal text primary key,
    id_hoja_vida bigint not null references jne.candidatos(id_hoja_vida),
    run_id uuid not null references jne.ingesta_runs(id),
    item_index integer not null,
    payload jsonb not null,
    fetched_at timestamptz not null default now()
);

create table if not exists jne.expedientes_candidato (
    id_expediente text primary key,
    id_hoja_vida bigint not null references jne.candidatos(id_hoja_vida),
    run_id uuid not null references jne.ingesta_runs(id),
    item_index integer not null,
    payload jsonb not null,
    fetched_at timestamptz not null default now()
);

create table if not exists jne.sentencias_penales (
    id_sentencia_penal text primary key,
    id_hoja_vida bigint not null references jne.candidatos(id_hoja_vida),
    run_id uuid not null references jne.ingesta_runs(id),
    item_index integer not null,
    payload jsonb not null,
    fetched_at timestamptz not null default now()
);

create table if not exists jne.sentencias_obligaciones (
    id_sentencia_obligacion text primary key,
    id_hoja_vida bigint not null references jne.candidatos(id_hoja_vida),
    run_id uuid not null references jne.ingesta_runs(id),
    item_index integer not null,
    payload jsonb not null,
    fetched_at timestamptz not null default now()
);

create table if not exists jne.declaracion_ingresos (
    id_declaracion_ingreso text primary key,
    id_hoja_vida bigint not null references jne.candidatos(id_hoja_vida),
    run_id uuid not null references jne.ingesta_runs(id),
    item_index integer not null,
    payload jsonb not null,
    fetched_at timestamptz not null default now()
);

create table if not exists jne.bienes_inmuebles (
    id_bien_inmueble text primary key,
    id_hoja_vida bigint not null references jne.candidatos(id_hoja_vida),
    run_id uuid not null references jne.ingesta_runs(id),
    item_index integer not null,
    payload jsonb not null,
    fetched_at timestamptz not null default now()
);

create table if not exists jne.bienes_muebles (
    id_bien_mueble text primary key,
    id_hoja_vida bigint not null references jne.candidatos(id_hoja_vida),
    run_id uuid not null references jne.ingesta_runs(id),
    item_index integer not null,
    payload jsonb not null,
    fetched_at timestamptz not null default now()
);

create table if not exists jne.otros_bienes_muebles (
    id_otro_bien_mueble text primary key,
    id_hoja_vida bigint not null references jne.candidatos(id_hoja_vida),
    run_id uuid not null references jne.ingesta_runs(id),
    item_index integer not null,
    payload jsonb not null,
    fetched_at timestamptz not null default now()
);

create table if not exists jne.titularidad_acciones (
    id_titularidad_accion text primary key,
    id_hoja_vida bigint not null references jne.candidatos(id_hoja_vida),
    run_id uuid not null references jne.ingesta_runs(id),
    item_index integer not null,
    payload jsonb not null,
    fetched_at timestamptz not null default now()
);

