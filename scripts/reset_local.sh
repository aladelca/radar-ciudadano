#!/usr/bin/env bash
set -euo pipefail

SUPABASE_URL="${SUPABASE_URL:-http://127.0.0.1:54321}"
STORAGE_HEALTH_URL="${SUPABASE_URL%/}/storage/v1/bucket"
WAIT_SECONDS="${RESET_WAIT_SECONDS:-30}"
SLEEP_SECONDS=2
LOG_FILE="$(mktemp "${TMPDIR:-/tmp}/supabase_db_reset.log.XXXXXX")"

log() {
  printf '[reset_local] %s\n' "$*"
}

storage_is_healthy() {
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' "$STORAGE_HEALTH_URL" || true)"
  case "$code" in
    200|400|401|403)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

find_kong_container() {
  local project_container fallback
  project_container="supabase_kong_$(basename "$PWD")"
  if docker ps --format '{{.Names}}' | grep -Fx "$project_container" >/dev/null 2>&1; then
    printf '%s\n' "$project_container"
    return 0
  fi

  fallback="$(docker ps --format '{{.Names}}' | grep '^supabase_kong_' | head -n 1 || true)"
  if [[ -n "$fallback" ]]; then
    printf '%s\n' "$fallback"
    return 0
  fi

  return 1
}

restart_kong_and_wait() {
  local kong_container
  if ! kong_container="$(find_kong_container)"; then
    log "No se encontro contenedor de Kong para reiniciar."
    return 1
  fi

  log "Reiniciando Kong: $kong_container"
  docker restart "$kong_container" >/dev/null

  local waited=0
  while (( waited < WAIT_SECONDS )); do
    if storage_is_healthy; then
      log "Storage API recuperada tras reiniciar Kong."
      return 0
    fi
    sleep "$SLEEP_SECONDS"
    ((waited += SLEEP_SECONDS))
  done

  log "Storage API sigue no disponible luego de ${WAIT_SECONDS}s."
  return 1
}

run_reset() {
  log "Ejecutando: supabase db reset --debug"
  set +e
  supabase db reset --debug 2>&1 | tee "$LOG_FILE"
  local exit_code="${PIPESTATUS[0]}"
  set -e
  return "$exit_code"
}

main() {
  log "Log de ejecucion: $LOG_FILE"

  if run_reset; then
    log "Reset finalizo con codigo 0."
    if storage_is_healthy; then
      log "Storage API saludable."
      return 0
    fi
    log "Storage API no saludable tras reset; aplicando autocorreccion."
    restart_kong_and_wait
    return 0
  fi

  if grep -Eq 'storage/v1/bucket|Error status 502' "$LOG_FILE"; then
    log "Detectado fallo conocido 502 en healthcheck de Storage."
    if restart_kong_and_wait; then
      log "Recuperado. Las migraciones SQL ya se aplicaron antes del fallo de healthcheck."
      return 0
    fi
  fi

  log "Reset no recuperable automaticamente. Revisa: $LOG_FILE"
  return 1
}

main "$@"
