create table if not exists jne.plan_gobierno_runs (
    id uuid primary key default gen_random_uuid(),
    process_id integer not null,
    tipo_eleccion_id integer null,
    status text not null check (status in ('running', 'failed', 'completed')),
    started_at timestamptz not null default now(),
    finished_at timestamptz null,
    candidates_read integer not null default 0,
    candidates_persisted integer not null default 0,
    plans_resolved integer not null default 0,
    pdf_texts_extracted integer not null default 0,
    errors_count integer not null default 0,
    metadata jsonb not null default '{}'::jsonb
);

create table if not exists jne.planes_gobierno (
    id_plan_gobierno bigint primary key,
    run_id uuid not null references jne.plan_gobierno_runs(id),
    id_proceso_electoral integer null,
    id_tipo_eleccion integer null,
    tipo_eleccion text null,
    id_organizacion_politica integer null,
    organizacion_politica text null,
    tipo_plan text null,
    id_jurado_electoral integer null,
    jurado_electoral text null,
    cod_expediente_ext text null,
    url_completo text null,
    url_resumen text null,
    payload jsonb not null,
    updated_at timestamptz not null default now()
);

create index if not exists idx_planes_gobierno_proceso_tipo
    on jne.planes_gobierno (id_proceso_electoral, id_tipo_eleccion);
create index if not exists idx_planes_gobierno_organizacion
    on jne.planes_gobierno (id_organizacion_politica);

create table if not exists jne.candidato_plan_gobierno (
    id_hoja_vida bigint primary key references jne.candidatos(id_hoja_vida),
    run_id uuid not null references jne.plan_gobierno_runs(id),
    id_proceso_electoral integer not null,
    id_tipo_eleccion integer not null,
    id_organizacion_politica integer null,
    id_solicitud_lista bigint null,
    id_plan_gobierno bigint null references jne.planes_gobierno(id_plan_gobierno),
    estado text not null,
    error_message text null,
    payload_detalle_para_candidato jsonb null,
    fetched_at timestamptz not null default now()
);

create index if not exists idx_candidato_plan_gobierno_plan
    on jne.candidato_plan_gobierno (id_plan_gobierno);
create index if not exists idx_candidato_plan_gobierno_proceso_tipo
    on jne.candidato_plan_gobierno (id_proceso_electoral, id_tipo_eleccion);

create table if not exists jne.planes_gobierno_dimensiones (
    id_plan_dimension text primary key,
    id_plan_gobierno bigint not null references jne.planes_gobierno(id_plan_gobierno),
    run_id uuid not null references jne.plan_gobierno_runs(id),
    id_plan_gob_dimension bigint null,
    dimension text not null check (dimension in ('social', 'economica', 'ambiental', 'institucional', 'propuesta')),
    item_index integer not null,
    problema text null,
    objetivo text null,
    indicador text null,
    meta text null,
    porcentaje double precision null,
    payload jsonb not null,
    updated_at timestamptz not null default now()
);

create index if not exists idx_planes_gob_dimensiones_plan
    on jne.planes_gobierno_dimensiones (id_plan_gobierno, dimension, item_index);

create table if not exists jne.planes_gobierno_pdf_texto (
    id_plan_gobierno bigint not null references jne.planes_gobierno(id_plan_gobierno),
    tipo_archivo text not null check (tipo_archivo in ('completo', 'resumen')),
    source_url text not null,
    http_status integer null,
    content_type text null,
    content_length_bytes integer null,
    text_content text null,
    text_length integer null,
    text_sha256 text null,
    extraction_ok boolean not null default false,
    extraction_error text null,
    extracted_at timestamptz not null default now(),
    primary key (id_plan_gobierno, tipo_archivo)
);

create index if not exists idx_planes_gobierno_pdf_texto_extraction_ok
    on jne.planes_gobierno_pdf_texto (extraction_ok);
