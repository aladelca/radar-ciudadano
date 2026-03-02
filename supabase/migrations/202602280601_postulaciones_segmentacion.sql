create table if not exists jne.candidatos_postulaciones (
    id_hoja_vida bigint not null references jne.candidatos(id_hoja_vida),
    id_proceso_electoral integer not null,
    id_tipo_eleccion integer not null,
    tipo_eleccion text null,
    segmento_postulacion text not null check (segmento_postulacion in ('PRESIDENCIAL', 'SENADO', 'OTROS')),
    run_id uuid not null references jne.ingesta_runs(id),
    id_organizacion_politica integer null,
    organizacion_politica text null,
    numero_documento text null,
    nombre_completo text null,
    cargo text null,
    estado text null,
    numero_candidato integer null,
    updated_at timestamptz not null default now(),
    primary key (id_hoja_vida, id_proceso_electoral, id_tipo_eleccion)
);

create index if not exists idx_candidatos_postulaciones_proceso_tipo
    on jne.candidatos_postulaciones (id_proceso_electoral, id_tipo_eleccion);
create index if not exists idx_candidatos_postulaciones_segmento
    on jne.candidatos_postulaciones (segmento_postulacion);
create index if not exists idx_candidatos_postulaciones_doc
    on jne.candidatos_postulaciones (numero_documento);

insert into jne.candidatos_postulaciones (
    id_hoja_vida,
    id_proceso_electoral,
    id_tipo_eleccion,
    tipo_eleccion,
    segmento_postulacion,
    run_id,
    id_organizacion_politica,
    organizacion_politica,
    numero_documento,
    nombre_completo,
    cargo,
    estado,
    numero_candidato,
    updated_at
)
select
    c.id_hoja_vida,
    c.id_proceso_electoral,
    c.id_tipo_eleccion,
    cte.tipo_eleccion,
    case
        when c.id_tipo_eleccion = 1 or upper(coalesce(cte.tipo_eleccion, '')) like '%PRESIDENCIAL%' then 'PRESIDENCIAL'
        when c.id_tipo_eleccion in (20, 21) or upper(coalesce(cte.tipo_eleccion, '')) like '%SENADOR%' then 'SENADO'
        else 'OTROS'
    end as segmento_postulacion,
    c.run_id,
    c.id_organizacion_politica,
    c.organizacion_politica,
    c.numero_documento,
    c.nombre_completo,
    c.cargo,
    c.estado,
    c.numero_candidato,
    c.updated_at
from jne.candidatos c
left join jne.catalog_tipos_eleccion cte
    on cte.id_proceso_electoral = c.id_proceso_electoral
   and cte.id_tipo_eleccion = c.id_tipo_eleccion
on conflict (id_hoja_vida, id_proceso_electoral, id_tipo_eleccion) do update set
    tipo_eleccion = excluded.tipo_eleccion,
    segmento_postulacion = excluded.segmento_postulacion,
    run_id = excluded.run_id,
    id_organizacion_politica = excluded.id_organizacion_politica,
    organizacion_politica = excluded.organizacion_politica,
    numero_documento = excluded.numero_documento,
    nombre_completo = excluded.nombre_completo,
    cargo = excluded.cargo,
    estado = excluded.estado,
    numero_candidato = excluded.numero_candidato,
    updated_at = excluded.updated_at;

create or replace view jne.v_candidatos_segmento_postulacion as
select
    cp.*,
    (cp.segmento_postulacion = 'PRESIDENCIAL') as es_presidencial,
    (cp.segmento_postulacion = 'SENADO') as es_senado
from jne.candidatos_postulaciones cp;

create or replace view jne.v_postulaciones_resumen_persona as
with base as (
    select
        coalesce(nullif(trim(cp.numero_documento), ''), 'IDHV:' || cp.id_hoja_vida::text) as persona_key,
        cp.numero_documento,
        cp.nombre_completo,
        cp.id_hoja_vida,
        cp.segmento_postulacion,
        cp.id_proceso_electoral,
        cp.id_tipo_eleccion,
        cp.tipo_eleccion
    from jne.candidatos_postulaciones cp
)
select
    persona_key,
    max(numero_documento) as numero_documento,
    max(nombre_completo) as nombre_completo,
    bool_or(segmento_postulacion = 'PRESIDENCIAL') as postula_presidencial,
    bool_or(segmento_postulacion = 'SENADO') as postula_senado,
    array_agg(distinct id_hoja_vida) as hojas_vida,
    array_agg(distinct id_tipo_eleccion) as tipos_eleccion_ids,
    array_agg(distinct tipo_eleccion) filter (where tipo_eleccion is not null) as tipos_eleccion_nombres,
    count(*)::int as total_postulaciones
from base
group by persona_key;
