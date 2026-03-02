from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


JsonDict = Dict[str, Any]


@dataclass
class SearchMetrics:
    candidates_read: int = 0
    candidates_persisted: int = 0
    errors_count: int = 0
    pages_read: int = 0
    tipos_procesados: List[int] = field(default_factory=list)


@dataclass
class CandidateFilter:
    process_id: int
    tipo_eleccion_id: int
    organizacion_politica_id: int = 0
    estado_id: int = 0
    sentencia_declarada_id: int = 0
    grado_academico_id: int = 0
    expediente_dadiva_id: int = 0
    ubigeo: str = "0"
    anio_experiencia_id: int = 0
    cargo_ocupado: Optional[List[int]] = None

    def to_api_filter(self) -> JsonDict:
        return {
            "IdTipoEleccion": self.tipo_eleccion_id,
            "IdOrganizacionPolitica": self.organizacion_politica_id,
            "ubigeo": self.ubigeo,
            "IdAnioExperiencia": self.anio_experiencia_id,
            "cargoOcupado": self.cargo_ocupado or [0],
            "IdSentenciaDeclarada": self.sentencia_declarada_id,
            "IdGradoAcademico": self.grado_academico_id,
            "IdExpedienteDadiva": self.expediente_dadiva_id,
            "IdProcesoElectoral": self.process_id,
            "IdEstado": self.estado_id,
        }

