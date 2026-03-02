create table if not exists jne.catalog_procesos_electorales (
    id_proceso_electoral integer primary key,
    nombre_proceso_electoral text null,
    siglas_proceso_electoral text null,
    id_tipo_proceso integer null,
    payload jsonb not null,
    updated_at timestamptz not null default now()
);

create table if not exists jne.catalog_tipos_eleccion (
    id_proceso_electoral integer not null,
    id_tipo_eleccion integer not null,
    tipo_eleccion text null,
    payload jsonb not null,
    updated_at timestamptz not null default now(),
    primary key (id_proceso_electoral, id_tipo_eleccion)
);

create table if not exists jne.catalog_organizaciones_politicas (
    id_proceso_electoral integer not null,
    id_organizacion_politica integer not null,
    organizacion_politica text null,
    payload jsonb not null,
    updated_at timestamptz not null default now(),
    primary key (id_proceso_electoral, id_organizacion_politica)
);

create table if not exists jne.hoja_vida_secciones_raw (
    id_hoja_vida bigint not null references jne.candidatos(id_hoja_vida),
    section_name text not null,
    run_id uuid not null references jne.ingesta_runs(id),
    payload jsonb not null,
    fetched_at timestamptz not null default now(),
    primary key (id_hoja_vida, section_name)
);

create table if not exists jne.anotaciones_raw (
    id_hoja_vida bigint primary key references jne.candidatos(id_hoja_vida),
    run_id uuid not null references jne.ingesta_runs(id),
    payload jsonb not null,
    fetched_at timestamptz not null default now()
);

create table if not exists jne.expedientes_raw (
    id_hoja_vida bigint primary key references jne.candidatos(id_hoja_vida),
    run_id uuid not null references jne.ingesta_runs(id),
    payload jsonb not null,
    fetched_at timestamptz not null default now()
);
