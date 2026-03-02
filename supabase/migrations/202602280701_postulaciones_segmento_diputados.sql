alter table if exists jne.candidatos_postulaciones
    drop constraint if exists candidatos_postulaciones_segmento_postulacion_check;

alter table if exists jne.candidatos_postulaciones
    add constraint candidatos_postulaciones_segmento_postulacion_check
    check (segmento_postulacion in ('PRESIDENCIAL', 'SENADO', 'DIPUTADOS', 'OTROS'));

update jne.candidatos_postulaciones
set segmento_postulacion = case
    when id_tipo_eleccion = 1 or upper(coalesce(tipo_eleccion, '')) like '%PRESIDENCIAL%' then 'PRESIDENCIAL'
    when id_tipo_eleccion = 15 or upper(coalesce(tipo_eleccion, '')) like '%DIPUTAD%' then 'DIPUTADOS'
    when id_tipo_eleccion in (20, 21) or upper(coalesce(tipo_eleccion, '')) like '%SENADOR%' then 'SENADO'
    else 'OTROS'
end,
updated_at = now();

create or replace view jne.v_candidatos_segmento_postulacion as
select
    cp.*,
    (cp.segmento_postulacion = 'PRESIDENCIAL') as es_presidencial,
    (cp.segmento_postulacion = 'SENADO') as es_senado,
    (cp.segmento_postulacion = 'DIPUTADOS') as es_diputado
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
    count(*)::int as total_postulaciones,
    bool_or(segmento_postulacion = 'DIPUTADOS') as postula_diputado
from base
group by persona_key;
