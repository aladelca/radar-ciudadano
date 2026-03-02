create or replace view jne.v_copilot_context as
select
    c.id_hoja_vida,
    c.nombre_completo,
    c.organizacion_politica,
    c.cargo,
    c.estado,
    coalesce(sp.cnt, 0)::integer as sentencias_penales_count,
    coalesce(so.cnt, 0)::integer as sentencias_obligaciones_count,
    coalesce(ex.cnt, 0)::integer as expedientes_count,
    coalesce(di.cnt, 0)::integer as ingresos_count,
    coalesce(bi.cnt, 0)::integer as bienes_inmuebles_count,
    coalesce(bm.cnt, 0)::integer as bienes_muebles_count,
    coalesce(obm.cnt, 0)::integer as otros_bienes_muebles_count,
    coalesce(ta.cnt, 0)::integer as titularidades_count,
    coalesce(am.cnt, 0)::integer as anotaciones_count,
    coalesce(hr.payload, '{}'::jsonb) as hoja_vida_payload,
    concat_ws(
        ' ',
        coalesce(c.nombre_completo, ''),
        coalesce(c.organizacion_politica, ''),
        coalesce(c.cargo, ''),
        coalesce(c.estado, ''),
        'sentencias_penales', coalesce(sp.cnt, 0)::text,
        'sentencias_obligaciones', coalesce(so.cnt, 0)::text,
        'expedientes', coalesce(ex.cnt, 0)::text,
        'ingresos', coalesce(di.cnt, 0)::text,
        'bienes_inmuebles', coalesce(bi.cnt, 0)::text,
        'bienes_muebles', coalesce(bm.cnt, 0)::text,
        'otros_bienes', coalesce(obm.cnt, 0)::text,
        'titularidades', coalesce(ta.cnt, 0)::text,
        'anotaciones', coalesce(am.cnt, 0)::text,
        coalesce(hr.payload::text, '')
    ) as context_text,
    c.updated_at
from jne.candidatos c
left join (
    select id_hoja_vida, count(*) as cnt
    from jne.sentencias_penales
    group by id_hoja_vida
) sp on sp.id_hoja_vida = c.id_hoja_vida
left join (
    select id_hoja_vida, count(*) as cnt
    from jne.sentencias_obligaciones
    group by id_hoja_vida
) so on so.id_hoja_vida = c.id_hoja_vida
left join (
    select id_hoja_vida, count(*) as cnt
    from jne.expedientes_candidato
    group by id_hoja_vida
) ex on ex.id_hoja_vida = c.id_hoja_vida
left join (
    select id_hoja_vida, count(*) as cnt
    from jne.declaracion_ingresos
    group by id_hoja_vida
) di on di.id_hoja_vida = c.id_hoja_vida
left join (
    select id_hoja_vida, count(*) as cnt
    from jne.bienes_inmuebles
    group by id_hoja_vida
) bi on bi.id_hoja_vida = c.id_hoja_vida
left join (
    select id_hoja_vida, count(*) as cnt
    from jne.bienes_muebles
    group by id_hoja_vida
) bm on bm.id_hoja_vida = c.id_hoja_vida
left join (
    select id_hoja_vida, count(*) as cnt
    from jne.otros_bienes_muebles
    group by id_hoja_vida
) obm on obm.id_hoja_vida = c.id_hoja_vida
left join (
    select id_hoja_vida, count(*) as cnt
    from jne.titularidad_acciones
    group by id_hoja_vida
) ta on ta.id_hoja_vida = c.id_hoja_vida
left join (
    select id_hoja_vida, count(*) as cnt
    from jne.anotaciones_marginales
    group by id_hoja_vida
) am on am.id_hoja_vida = c.id_hoja_vida
left join jne.hoja_vida_raw hr on hr.id_hoja_vida = c.id_hoja_vida;

create or replace function jne.search_candidatos_copilot(
    p_search_text text,
    p_max_rows integer default 20
)
returns table (
    id_hoja_vida bigint,
    nombre_completo text,
    organizacion_politica text,
    cargo text,
    estado text,
    sentencias_penales_count integer,
    sentencias_obligaciones_count integer,
    expedientes_count integer,
    ingresos_count integer,
    bienes_inmuebles_count integer,
    bienes_muebles_count integer,
    otros_bienes_muebles_count integer,
    titularidades_count integer,
    anotaciones_count integer,
    score integer
)
language sql
stable
as $$
with normalized as (
    select nullif(trim(coalesce(p_search_text, '')), '') as q
),
ranked as (
    select
        v.*,
        case
            when n.q is null then 1
            else
                (case when upper(coalesce(v.nombre_completo, '')) like upper(n.q) || '%' then 300 else 0 end) +
                (case when upper(coalesce(v.nombre_completo, '')) like '%' || upper(n.q) || '%' then 200 else 0 end) +
                (case when upper(coalesce(v.organizacion_politica, '')) like '%' || upper(n.q) || '%' then 120 else 0 end) +
                (case when upper(coalesce(v.cargo, '')) like '%' || upper(n.q) || '%' then 80 else 0 end) +
                (case when upper(v.context_text) like '%' || upper(n.q) || '%' then 20 else 0 end)
        end as rank_score,
        n.q
    from jne.v_copilot_context v
    cross join normalized n
)
select
    ranked.id_hoja_vida,
    ranked.nombre_completo,
    ranked.organizacion_politica,
    ranked.cargo,
    ranked.estado,
    ranked.sentencias_penales_count,
    ranked.sentencias_obligaciones_count,
    ranked.expedientes_count,
    ranked.ingresos_count,
    ranked.bienes_inmuebles_count,
    ranked.bienes_muebles_count,
    ranked.otros_bienes_muebles_count,
    ranked.titularidades_count,
    ranked.anotaciones_count,
    ranked.rank_score as score
from ranked
where ranked.q is null or ranked.rank_score > 0
order by ranked.rank_score desc, ranked.nombre_completo asc
limit greatest(1, least(coalesce(p_max_rows, 20), 100));
$$;
