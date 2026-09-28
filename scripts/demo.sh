#!/usr/bin/env bash
# Demo driver: runs the full golden path + failure path + load-spike test
# against the running local stack (see deploy/local/docker-compose.yml),
# printing what each step does and its real result, pausing between steps
# so a presenter can talk over each one while recording.
#
# Usage: scripts/demo.sh [--auto] [SPIKE_N] [UPLOAD_BASE_URL] [SAGA_BASE_URL]
#   --auto           don't wait for Enter between steps; sleep briefly instead
#   SPIKE_N          uploads for the load-spike step (default 30)
#   UPLOAD_BASE_URL  default http://localhost:8081
#   SAGA_BASE_URL    default http://localhost:8084

set -uo pipefail

AUTO=0
if [ "${1:-}" = "--auto" ]; then
  AUTO=1
  shift
fi

SPIKE_N="${1:-30}"
UPLOAD_URL="${2:-http://localhost:8081}"
SAGA_URL="${3:-http://localhost:8084}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURE="${FIXTURE:-$(cd "$SCRIPT_DIR/../../fiapx-video-processing-service" && pwd)/tests/fixtures/sample.mp4}"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

BOLD=$'\033[1m'; DIM=$'\033[2m'; RESET=$'\033[0m'
GREEN=$'\033[32m'; YELLOW=$'\033[33m'; RED=$'\033[31m'; CYAN=$'\033[36m'

STEP_N=0

hr() { printf '%s\n' "----------------------------------------------------------------------"; }

step() {
  STEP_N=$((STEP_N + 1))
  hr
  printf '%s%s Passo %d: %s%s\n' "$BOLD" "$CYAN" "$STEP_N" "$1" "$RESET"
}

explain() { printf '%s%s%s\n' "$DIM" "$1" "$RESET"; }

ok()   { printf '%s%s%s\n' "$GREEN" "$1" "$RESET"; }
warn() { printf '%s%s%s\n' "$YELLOW" "$1" "$RESET"; }
fail() { printf '%s%s%s\n' "$RED" "$1" "$RESET"; }

show_json() { jq -C . 2>/dev/null || cat; }

pause() {
  if [ "$AUTO" -eq 1 ]; then
    sleep 2
    return
  fi
  printf '%s[ENTER para continuar]%s ' "$DIM" "$RESET"
  if [ -r /dev/tty ]; then
    read -r _ < /dev/tty
  else
    read -r _ || true
  fi
}

open_url() {
  if command -v open >/dev/null 2>&1; then
    open "$1" >/dev/null 2>&1 || true
  fi
}

for cmd in curl jq unzip; do
  command -v "$cmd" >/dev/null 2>&1 || { fail "comando obrigatório não encontrado: $cmd"; exit 1; }
done
if [ ! -f "$FIXTURE" ]; then
  fail "fixture não encontrada: $FIXTURE"
  exit 1
fi

# --- Passo 0: pre-flight ---------------------------------------------------
hr
printf '%s%sVerificando se a stack local está no ar...%s\n' "$BOLD" "$CYAN" "$RESET"
PREFLIGHT_OK=1
for pair in "upload-service:8081" "processing-service:8082" "notification-service:8083" "saga-orchestrator:8084"; do
  name="${pair%%:*}"
  port="${pair##*:}"
  code="$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:$port/healthz" || echo "000")"
  if [ "$code" = "200" ]; then
    ok "  $name (porta $port): ok"
  else
    fail "  $name (porta $port): não respondeu (HTTP $code)"
    PREFLIGHT_OK=0
  fi
done
if [ "$PREFLIGHT_OK" -ne 1 ]; then
  fail "Stack não está pronta. Rode primeiro:"
  fail "  cd deploy/local && COMPOSE_PARALLEL_LIMIT=1 docker compose up -d --build"
  exit 1
fi
pause

# --- Passo 1: registro ------------------------------------------------------
step "registro de usuário"
explain "Criando um usuário novo para esta demonstração (e-mail único, evita conflito em re-execuções)."
EMAIL="demo-$(date +%s)@fiapx.local"
PASSWORD="senha123"
resp="$(curl -sS -w '\n%{http_code}' -X POST "$UPLOAD_URL/api/v1/auth/register" \
  -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}")"
code="$(tail -n1 <<<"$resp")"; body="$(sed '$d' <<<"$resp")"
echo "$body" | show_json
if [ "$code" != "201" ]; then fail "esperava 201, veio $code"; exit 1; fi
ok "usuário $EMAIL criado (HTTP $code)"
pause

# --- Passo 2: login ----------------------------------------------------------
step "login"
explain "Autenticando para obter o token JWT usado em toda chamada autenticada a seguir."
resp="$(curl -sS -w '\n%{http_code}' -X POST "$UPLOAD_URL/api/v1/auth/login" \
  -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}")"
code="$(tail -n1 <<<"$resp")"; body="$(sed '$d' <<<"$resp")"
echo "$body" | show_json
TOKEN="$(jq -r .token <<<"$body")"
if [ "$code" != "200" ] || [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then fail "login falhou (HTTP $code)"; exit 1; fi
ok "token JWT obtido (HTTP $code)"
pause

# --- Passo 3: upload (golden path) ------------------------------------------
step "upload de vídeo (caminho feliz)"
explain "Enviando um vídeo real. A resposta volta na hora (202) — o ffmpeg roda assíncrono, não trava a requisição."
resp="$(curl -sS -w '\n%{http_code}' -X POST "$UPLOAD_URL/api/v1/videos" \
  -H "Authorization: Bearer $TOKEN" \
  -F "video=@$FIXTURE;filename=demo-golden.mp4")"
code="$(tail -n1 <<<"$resp")"; body="$(sed '$d' <<<"$resp")"
echo "$body" | show_json
VIDEO_ID="$(jq -r .video_id <<<"$body")"
if [ "$code" != "202" ] || [ -z "$VIDEO_ID" ] || [ "$VIDEO_ID" = "null" ]; then fail "upload falhou (HTTP $code)"; exit 1; fi
ok "vídeo $VIDEO_ID aceito (HTTP $code)"
pause

# --- Passo 4: poll de status -------------------------------------------------
step "acompanhando o status até COMPLETED"
explain "Consultando GET /videos até o vídeo terminar. Do upload ao COMPLETED leva menos de 2 segundos."
start_ts=$(date +%s)
STATUS=""
for _ in $(seq 1 60); do
  listing="$(curl -sS "$UPLOAD_URL/api/v1/videos?limit=50" -H "Authorization: Bearer $TOKEN")"
  STATUS="$(jq -r --arg id "$VIDEO_ID" '.videos[] | select(.video_id == $id) | .status' <<<"$listing")"
  elapsed=$(( $(date +%s) - start_ts ))
  printf '  [%ss] status: %s\n' "$elapsed" "$STATUS"
  if [ "$STATUS" = "COMPLETED" ] || [ "$STATUS" = "FAILED" ]; then break; fi
  sleep 1
done
if [ "$STATUS" != "COMPLETED" ]; then fail "vídeo não completou (status final: $STATUS)"; exit 1; fi
ok "vídeo completou em ${elapsed}s"
pause

# --- Passo 5: histórico da saga ----------------------------------------------
step "histórico da saga"
explain "Consultando o saga-orchestrator para ver a transição de estado completa desse vídeo."
curl -sS "$SAGA_URL/api/v1/sagas/$VIDEO_ID" | show_json
pause

# --- Passo 6: download --------------------------------------------------------
step "download do resultado"
explain "Pedindo a URL de download pré-assinada e baixando o .zip com os frames extraídos."
dl_resp="$(curl -sS "$UPLOAD_URL/api/v1/videos/$VIDEO_ID/download" -H "Authorization: Bearer $TOKEN")"
echo "$dl_resp" | show_json
DL_URL="$(jq -r .download_url <<<"$dl_resp")"
ZIP_PATH="$WORKDIR/resultado.zip"
curl -sS -o "$ZIP_PATH" "$DL_URL"
ok "conteúdo do zip:"
unzip -l "$ZIP_PATH"
pause

# --- Passo 7: e-mail de conclusão --------------------------------------------
step "e-mail de conclusão"
explain "Confira o Mailhog (http://localhost:8025) — o e-mail de conclusão chegou nesse momento."
open_url "http://localhost:8025"
pause

# --- Passo 8: caminho de falha (upload inválido) -----------------------------
step "caminho de falha: upload de arquivo inválido"
explain "Enviando um arquivo que não é um vídeo válido. O upload é aceito (202); a falha aparece no processamento."
FAKE="$WORKDIR/invalido.mp4"
echo "isto nao e um video valido" > "$FAKE"
resp="$(curl -sS -w '\n%{http_code}' -X POST "$UPLOAD_URL/api/v1/videos" \
  -H "Authorization: Bearer $TOKEN" \
  -F "video=@$FAKE;filename=demo-invalido.mp4")"
code="$(tail -n1 <<<"$resp")"; body="$(sed '$d' <<<"$resp")"
echo "$body" | show_json
FAIL_VIDEO_ID="$(jq -r .video_id <<<"$body")"
if [ "$code" != "202" ] || [ -z "$FAIL_VIDEO_ID" ] || [ "$FAIL_VIDEO_ID" = "null" ]; then fail "upload falhou (HTTP $code)"; exit 1; fi
ok "vídeo $FAIL_VIDEO_ID aceito (HTTP $code)"
pause

# --- Passo 9: poll até FAILED -------------------------------------------------
step "acompanhando o status até FAILED"
explain "O ffmpeg vai rejeitar o arquivo; o status deve virar FAILED com uma mensagem de erro real."
start_ts=$(date +%s)
STATUS=""
for _ in $(seq 1 60); do
  listing="$(curl -sS "$UPLOAD_URL/api/v1/videos?limit=50" -H "Authorization: Bearer $TOKEN")"
  STATUS="$(jq -r --arg id "$FAIL_VIDEO_ID" '.videos[] | select(.video_id == $id) | .status' <<<"$listing")"
  elapsed=$(( $(date +%s) - start_ts ))
  printf '  [%ss] status: %s\n' "$elapsed" "$STATUS"
  if [ "$STATUS" = "COMPLETED" ] || [ "$STATUS" = "FAILED" ]; then break; fi
  sleep 1
done
if [ "$STATUS" != "FAILED" ]; then fail "esperava FAILED, veio $STATUS"; exit 1; fi
jq -r --arg id "$FAIL_VIDEO_ID" '.videos[] | select(.video_id == $id)' <<<"$listing" | show_json
ok "vídeo terminou como FAILED, como esperado"
pause

# --- Passo 10: e-mail de falha -------------------------------------------------
step "e-mail de falha"
explain "Confira o Mailhog novamente — o e-mail de falha, com o motivo, chegou agora."
open_url "http://localhost:8025"
pause

# --- Passo 11: teste de carga --------------------------------------------------
step "teste de carga: $SPIKE_N uploads concorrentes"
explain "Disparando $SPIKE_N uploads ao mesmo tempo para provar que o sistema não perde requisição sob pico."
"$SCRIPT_DIR/load_spike_test.sh" "$SPIKE_N" "$UPLOAD_URL"
pause

# --- Encerramento ---------------------------------------------------------
hr
ok "Demonstração concluída: caminho feliz, caminho de falha e teste de carga, todos ao vivo."
