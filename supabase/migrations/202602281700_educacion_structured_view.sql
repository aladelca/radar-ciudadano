create or replace view jne.v_candidato_educacion as
with formacion as (
    select
        h.id_hoja_vida,
        h.payload
    from jne.hoja_vida_secciones_raw h
    where h.section_name = 'formacionAcademica'
),
normalized as (
    select
        c.id_hoja_vida,
        coalesce((f.payload -> 'educacionBasica' ->> 'tengoEduBasica') = 'SI', false) as tiene_educacion_basica,
        coalesce((f.payload -> 'educacionBasica' ->> 'eduPrimaria') = 'SI', false) as tiene_primaria,
        coalesce((f.payload -> 'educacionBasica' ->> 'eduSecundaria') = 'SI', false) as tiene_secundaria,
        coalesce(jsonb_array_length(coalesce(f.payload -> 'educacionUniversitaria', '[]'::jsonb)), 0)::integer as educacion_universitaria_count,
        coalesce(jsonb_array_length(coalesce(f.payload -> 'educacionNoUniversitaria', '[]'::jsonb)), 0)::integer as educacion_no_universitaria_count,
        coalesce(jsonb_array_length(coalesce(f.payload -> 'educacionTecnico', '[]'::jsonb)), 0)::integer as educacion_tecnico_count,
        coalesce(jsonb_array_length(coalesce(f.payload -> 'educacionPosgrado', '[]'::jsonb)), 0)::integer as educacion_posgrado_count,
        coalesce(jsonb_array_length(coalesce(f.payload -> 'educacionPosgradoOtro', '[]'::jsonb)), 0)::integer as educacion_posgrado_otro_count
    from jne.candidatos c
    left join formacion f
      on f.id_hoja_vida = c.id_hoja_vida
)
select
    n.id_hoja_vida,
    n.tiene_educacion_basica,
    n.tiene_primaria,
    n.tiene_secundaria,
    n.educacion_universitaria_count,
    n.educacion_no_universitaria_count,
    n.educacion_tecnico_count,
    n.educacion_posgrado_count,
    n.educacion_posgrado_otro_count,
    (
        n.tiene_educacion_basica
        or n.tiene_primaria
        or n.tiene_secundaria
        or n.educacion_universitaria_count > 0
        or n.educacion_no_universitaria_count > 0
        or n.educacion_tecnico_count > 0
        or n.educacion_posgrado_count > 0
        or n.educacion_posgrado_otro_count > 0
    ) as has_any_studies
from normalized n;
