const API = {
  dashboard: "/api/v1/dashboard/insights",
  copilotAsk: "/api/v1/copilot/ask",
  copilotAskAI: "/api/v1/copilot/ask-ai",
};
const DASHBOARD_TOP_UNIVERSIDADES = 10;

const STORAGE = {
  sessionIdKey: "cv_copilot_session_id",
};
const COPILOT_LIMIT_DEFAULT = 20;

const charts = {};
const dashboardState = {
  tipoEleccionId: "",
};
const CHART_THEME = {
  text: "#1a1a1a",
  border: "rgba(18, 18, 18, 0.14)",
};

if (window.Chart) {
  Chart.defaults.color = CHART_THEME.text;
  Chart.defaults.borderColor = CHART_THEME.border;
  Chart.defaults.font.family = '"Manrope", sans-serif';
}

const numberFormatter = new Intl.NumberFormat("es-PE");

function formatNumber(value) {
  const numeric = Number(value || 0);
  return numberFormatter.format(Number.isFinite(numeric) ? numeric : 0);
}

function monoScale(total = 1) {
  const shades = ["#a71720", "#bf2f38", "#d54f58", "#e2767d", "#ed9ca1", "#f5c2c5"];
  return Array.from({ length: Math.max(1, total) }, (_, index) => shades[index % shades.length]);
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (!el) {
    return;
  }
  el.textContent = value;
}

function buildDashboardUrl(tipoEleccionId = "") {
  const params = new URLSearchParams({
    top_universidades: String(DASHBOARD_TOP_UNIVERSIDADES),
  });
  const normalized = String(tipoEleccionId || "").trim();
  if (normalized) {
    params.set("tipo_eleccion_id", normalized);
  }
  return `${API.dashboard}?${params.toString()}`;
}

function syncDashboardFilters(payload) {
  const select = document.getElementById("dashboardTipoEleccion");
  if (!select) {
    return;
  }

  const filters = payload?.filters || {};
  const options = Array.isArray(filters.tipo_eleccion_options) ? filters.tipo_eleccion_options : [];
  const selectedId =
    filters.selected_tipo_eleccion_id != null ? String(filters.selected_tipo_eleccion_id) : "";
  const selectedLabel = String(filters.selected_tipo_eleccion_label || "Todos");

  const optionElements = [];
  const allOption = document.createElement("option");
  allOption.value = "";
  allOption.textContent = "Todos";
  optionElements.push(allOption);

  for (const option of options) {
    const id = String(option?.id_tipo_eleccion ?? "").trim();
    if (!id) {
      continue;
    }
    const label = String(option?.tipo_eleccion || `Tipo ${id}`).trim();
    const count = Number(option?.candidates_count || 0);
    const item = document.createElement("option");
    item.value = id;
    item.textContent = `${label} (${formatNumber(count)})`;
    optionElements.push(item);
  }

  select.replaceChildren(...optionElements);
  select.value = optionElements.some((item) => item.value === selectedId) ? selectedId : "";
  dashboardState.tipoEleccionId = select.value;
  setText("dashboardFilterInfo", `Tipo de eleccion: ${selectedLabel}`);
}

function upsertChart(canvasId, type, labels, values, colors, options = {}) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) {
    return;
  }

  if (charts[canvasId]) {
    charts[canvasId].destroy();
  }

  charts[canvasId] = new Chart(canvas, {
    type,
    data: {
      labels,
      datasets: [
        {
          data: values,
          backgroundColor: colors,
          borderColor: "rgba(18, 18, 18, 0.12)",
          borderWidth: 1.5,
          borderRadius: type === "bar" ? 6 : 0,
          hoverOffset: type === "doughnut" ? 6 : 0,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: type !== "bar",
          position: "bottom",
          labels: {
            color: CHART_THEME.text,
            usePointStyle: true,
            boxWidth: 10,
          },
        },
        tooltip: {
          backgroundColor: "#111",
          titleColor: "#fff",
          bodyColor: "#f2f2f2",
          borderColor: CHART_THEME.border,
          borderWidth: 1,
        },
      },
      ...options,
    },
  });
}

function renderDashboard(payload) {
  const totals = payload.totals || {};
  const chartData = payload.charts || {};
  const notes = payload.notes || {};

  setText("generatedAt", `Actualizado (UTC): ${payload.generated_at || "-"}`);
  setText("statCandidates", formatNumber(totals.total_candidates));
  setText("statIncome", formatNumber(totals.candidates_with_income_amount));
  setText("statDenuncias", formatNumber(totals.candidates_with_denuncias));
  setText("statUniversidades", formatNumber(totals.candidates_with_university));

  setText("noteIngresos", notes.ingresos || "");
  setText("noteDenuncias", notes.denuncias || "");
  setText("noteUniversidades", notes.universidades || "");

  const ingresos = chartData.ingresos || [];
  upsertChart(
    "chartIngresos",
    "bar",
    ingresos.map((item) => item.label),
    ingresos.map((item) => item.count),
    monoScale(ingresos.length),
    {
      scales: {
        x: {
          grid: { display: false },
        },
        y: {
          beginAtZero: true,
          grid: { color: "rgba(18, 18, 18, 0.08)" },
          ticks: { precision: 0 },
        },
      },
    },
  );

  const denuncias = chartData.denuncias || [];
  upsertChart(
    "chartDenuncias",
    "doughnut",
    denuncias.map((item) => item.label),
    denuncias.map((item) => item.count),
    monoScale(denuncias.length),
  );

  const universidades = chartData.universidades || [];
  upsertChart(
    "chartUniversidades",
    "bar",
    universidades.map((item) => item.label),
    universidades.map((item) => item.count),
    monoScale(universidades.length),
    {
      indexAxis: "y",
      scales: {
        x: {
          beginAtZero: true,
          grid: { color: "rgba(18, 18, 18, 0.08)" },
          ticks: { precision: 0 },
        },
        y: {
          grid: { display: false },
        },
      },
    },
  );
}

function renderCopilotCandidates(response) {
  const container = document.getElementById("copilotCandidates");
  if (!container) {
    return;
  }
  container.replaceChildren();

  const candidates = Array.isArray(response.candidates) ? response.candidates : [];
  const evidence = Array.isArray(response.evidence) ? response.evidence : [];
  const evidenceById = new Map();
  const evidenceByRow = new Map();
  for (const item of evidence) {
    if (item && item.id_hoja_vida != null) {
      evidenceById.set(String(item.id_hoja_vida), item);
    }
    if (item && item.row_ref != null) {
      evidenceByRow.set(String(item.row_ref), item);
    }
  }

  if (candidates.length === 0) {
    const empty = document.createElement("p");
    empty.textContent = "No se encontraron resultados para esta consulta.";
    container.appendChild(empty);
    return;
  }

  for (const [index, candidate] of candidates.slice(0, 8).entries()) {
    const card = document.createElement("article");
    card.className = "result-card";

    const title = document.createElement("h3");
    const titleText =
      candidate.nombre_completo ||
      (candidate.organizacion_politica
        ? `Partido: ${candidate.organizacion_politica}`
        : candidate.segmento_postulacion
          ? `Segmento: ${candidate.segmento_postulacion}`
          : `Fila ${index + 1}`);
    title.textContent = titleText;
    card.appendChild(title);

    const meta = document.createElement("p");
    meta.className = "result-meta";
    if (candidate.nombre_completo) {
      meta.textContent =
        `${candidate.organizacion_politica || "Sin organizacion"} | ${candidate.cargo || "Sin cargo"} | estado ${candidate.estado || "-"} | score ${candidate.score ?? "-"}`;
    } else {
      const metricPairs = Object.entries(candidate)
        .filter(([key, value]) => typeof value === "number" && !["id_hoja_vida", "score"].includes(key))
        .slice(0, 4)
        .map(([key, value]) => `${key}: ${value}`);
      meta.textContent =
        metricPairs.length > 0
          ? metricPairs.join(" | ")
          : `${candidate.organizacion_politica || candidate.segmento_postulacion || "Resultado agregado"}`;
    }
    card.appendChild(meta);

    const evi =
      (candidate.id_hoja_vida != null ? evidenceById.get(String(candidate.id_hoja_vida)) : null) ||
      evidenceByRow.get(String(index + 1));
    if (evi && Array.isArray(evi.findings) && evi.findings.length > 0) {
      const list = document.createElement("ul");
      list.className = "findings";
      for (const finding of evi.findings.slice(0, 4)) {
        const item = document.createElement("li");
        item.textContent = String(finding);
        list.appendChild(item);
      }
      card.appendChild(list);
    }

    container.appendChild(card);
  }
}

async function loadDashboard() {
  const select = document.getElementById("dashboardTipoEleccion");
  if (select) {
    select.disabled = true;
  }
  try {
    const response = await fetch(buildDashboardUrl(dashboardState.tipoEleccionId), { method: "GET" });
    if (!response.ok) {
      throw new Error(`dashboard ${response.status}`);
    }
    const payload = await response.json();
    syncDashboardFilters(payload);
    renderDashboard(payload);
  } catch (error) {
    setText("generatedAt", "No se pudo cargar dashboard.");
    console.error(error);
  } finally {
    if (select) {
      select.disabled = false;
    }
  }
}

function bindDashboardFilters() {
  const select = document.getElementById("dashboardTipoEleccion");
  if (!select) {
    return;
  }

  select.addEventListener("change", () => {
    dashboardState.tipoEleccionId = String(select.value || "").trim();
    loadDashboard();
  });
}

function bindCopilot() {
  const form = document.getElementById("copilotForm");
  const queryInput = document.getElementById("copilotQuery");
  const useAiInput = document.getElementById("copilotUseAi");
  const modeInfo = document.getElementById("copilotModeInfo");
  const status = document.getElementById("copilotStatus");
  const summary = document.getElementById("copilotSummary");
  const btn = document.getElementById("copilotBtn");

  if (!form || !queryInput || !useAiInput || !modeInfo || !status || !summary || !btn) {
    return;
  }

  const syncModeLabel = () => {
    modeInfo.textContent = useAiInput.checked ? "Modo actual: IA" : "Modo actual: SQL";
  };
  syncModeLabel();
  useAiInput.addEventListener("change", syncModeLabel);

  const getSessionId = () => window.localStorage.getItem(STORAGE.sessionIdKey) || "";
  const setSessionId = (value) => {
    const normalized = String(value || "").trim();
    if (!normalized) {
      return;
    }
    window.localStorage.setItem(STORAGE.sessionIdKey, normalized);
  };

  form.addEventListener("submit", async (event) => {
    event.preventDefault();

    const query = String(queryInput.value || "").trim();
    if (!query) {
      status.textContent = "Escribe una pregunta para consultar.";
      return;
    }

    status.textContent = "Consultando copilot...";
    summary.textContent = "";
    btn.disabled = true;
    const endpoint = useAiInput.checked ? API.copilotAskAI : API.copilotAsk;
    const sessionId = getSessionId();
    const payloadBody = {
      query,
      limit: COPILOT_LIMIT_DEFAULT,
    };
    if (useAiInput.checked && sessionId) {
      payloadBody.session_id = sessionId;
    }

    try {
      const response = await fetch(endpoint, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payloadBody),
      });

      if (!response.ok) {
        const body = await response.text();
        throw new Error(`copilot ${response.status} ${body.slice(0, 180)}`);
      }

      const payload = await response.json();
      summary.textContent = payload.summary || "";
      const mode = payload.mode || (useAiInput.checked ? "ai" : "sql");
      const modeLabel = mode === "ai" ? "IA" : "fallback SQL";
      status.textContent = `Listo (${modeLabel}): ${payload.count || 0} candidato(s).`;
      if (payload.session_id) {
        setSessionId(payload.session_id);
      }
      if (payload.history_used !== undefined) {
        status.textContent += ` Historial: ${payload.history_used}.`;
      }
      if (payload.warning) {
        status.textContent += ` Aviso: ${payload.warning}`;
      }
      renderCopilotCandidates(payload);
    } catch (error) {
      status.textContent = "No se pudo procesar la consulta.";
      console.error(error);
    } finally {
      btn.disabled = false;
    }
  });
}

document.addEventListener("DOMContentLoaded", () => {
  bindDashboardFilters();
  loadDashboard();
  bindCopilot();
});
