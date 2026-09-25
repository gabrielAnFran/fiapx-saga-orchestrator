# Rodando a stack completa do FIAP X localmente

Este arquivo compose sobe os 4 microsservicos, seus proprios bancos de
dados, RabbitMQ, MinIO (armazenamento S3-compatible para os videos
originais e os zips gerados) e Mailhog (caixa de entrada SMTP fake para
os e-mails do notification-service), para uma demonstracao local ponta a
ponta do pipeline de upload -> extracao de frames -> notificacao.

## Pre-requisitos

Clone os 4 repositorios como irmaos no disco:

```
pos/
├── fiapx-video-upload-service
├── fiapx-video-processing-service
├── fiapx-notification-service
└── fiapx-saga-orchestrator   <- este repositorio, rode o compose a partir daqui
```

## Subindo a stack

```bash
docker compose -f deploy/local/docker-compose.yml up --build
```

- UI de gerenciamento do RabbitMQ: http://localhost:15672 (guest/guest)
- Console do MinIO: http://localhost:9001 (minioadmin/minioadmin)
- UI do Mailhog (onde os e-mails de notificacao aparecem): http://localhost:8025

Portas dos servicos: upload-service 8081, processing-service 8082,
notification-service 8083, saga-orchestrator 8084.

## Passo a passo do caminho feliz (curl)

```bash
# 1. Registra um usuario
curl -X POST http://localhost:8081/api/v1/auth/register -H 'Content-Type: application/json' \
  -d '{"email":"demo@fiapx.local","password":"senha123"}'

# 2. Faz login e guarda o token
TOKEN=$(curl -s -X POST http://localhost:8081/api/v1/auth/login -H 'Content-Type: application/json' \
  -d '{"email":"demo@fiapx.local","password":"senha123"}' | jq -r '.token')

# 3. Faz upload de um video (multipart)
curl -X POST http://localhost:8081/api/v1/videos -H "Authorization: Bearer $TOKEN" \
  -F "video=@sample.mp4"
# resposta inclui o video_id gerado

# 4. Acompanha a saga assumindo o pipeline (worker do orquestrador consome
#    VideoUploaded de forma assincrona, aguarde alguns segundos)
curl http://localhost:8084/api/v1/sagas/<video_id>
# espera-se PROCESSING logo apos o upload, depois COMPLETED (ou FAILED)

# 5. Poll na lista de videos do usuario ate o status virar COMPLETED
curl http://localhost:8081/api/v1/videos -H "Authorization: Bearer $TOKEN"

# 6. Baixa o zip de frames gerado
curl -L http://localhost:8081/api/v1/videos/<video_id>/download -H "Authorization: Bearer $TOKEN" -o frames.zip

# 7. Confirma que o e-mail de conclusao chegou no Mailhog
open http://localhost:8025
```

## Acompanhando a saga

`GET http://localhost:8084/api/v1/sagas/<video_id>` retorna a instancia
da saga (campo `saga`, com `State` em `UPLOADED` -> na pratica a saga ja
nasce em `PROCESSING`, ver `docs/adr/0001-orchestrated-saga.md`) e a
trilha completa de transicoes em `history`, util para depurar onde um
video travou caso `processing-service` ou `notification-service` nao
estejam consumindo suas filas.

## Nota

Os Dockerfiles dos repositorios irmaos podem ainda nao existir no momento
em que este arquivo compose foi escrito (cada agente constroi seu proprio
servico em paralelo) — `docker compose build` so precisa que os caminhos
relativos resolvam quando a stack for efetivamente executada, nao no
momento em que este arquivo foi escrito. Se a convencao real de
`Dockerfile`/`TARGET` de um repositorio irmao acabar sendo diferente do
que se assume aqui, atualize o bloco `build:` daquele servico de acordo.
