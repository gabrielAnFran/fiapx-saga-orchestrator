# fiapx-saga-orchestrator

O microsservico Orquestrador de Saga do sistema FIAP X de processamento
de video (upload -> extracao assincrona de frames -> download de um zip),
dividido em 4 servicos implantados de forma independente. Este servico
coordena o processo de negocio que atravessa todos eles, e e o dono do
arquivo docker-compose local que sobe a stack inteira junto para
demonstracoes/testes.

## Arquitetura

```
                    RabbitMQ (video.events topic exchange)
                                    │
        ┌───────────────┬──────────┼──────────┬───────────────┐
        │               │          │           │               │
  ┌───────────┐   ┌────────────┐   │   ┌──────────────┐  ┌─────────────┐
  │  Upload   │   │ Processing │   │   │ Notification │  │    Saga     │
  │  Service  │   │  Service   │   │   │   Service    │  │ Orchestrator│
  │  (8081)   │   │  (8082)    │   │   │   (8083)     │  │   (8084)    │
  │ Postgres  │   │ Postgres   │   │   │  Postgres    │  │  Postgres   │
  │  + MinIO  │   │ + MinIO    │   │   │  + Mailhog   │  │             │
  └───────────┘   └────────────┘   │   └──────────────┘  └─────────────┘
                                    │
                          cada servico: banco proprio,
                          outbox proprio, processo
                          outbox-dispatcher proprio
```

Cada servico e dono do seu proprio banco de dados (sem schema
compartilhado, sem joins entre servicos) e fala com os outros apenas
atraves de eventos no `video.events`. Este repositorio, o orquestrador, e
o unico servico com um papel explicito de coordenacao: ele escuta os 3
eventos de fato que upload-service e processing-service produzem e e o
**unico** componente autorizado a publicar os 4 comandos que movem o
pipeline adiante — upload-service, processing-service e
notification-service nunca se chamam diretamente entre si.

Repositorios irmaos (clonados ao lado deste para demonstracoes locais,
veja `deploy/local/`): `fiapx-video-upload-service`,
`fiapx-video-processing-service`, `fiapx-notification-service`. Para a
visao completa do sistema (requisitos do desafio, decisoes de produto),
veja `docs/architecture.md` no repositorio irmao `project-5-hacka`.

## Por que uma saga orquestrada mesmo sem compensacao

Veja `docs/adr/0001-orchestrated-saga.md` para o registro de decisao
completo. Resumo: diferente de uma saga com transacoes distribuidas de
verdade (como a do sistema de PDV que inspirou esta arquitetura), este e
um pipeline linear sem acoes de compensacao — se a extracao de frames
falhar, nao ha nada para desfazer, apenas reportar a falha. Ainda assim,
mantemos o padrao orquestrado por consistencia com a convencao dos
servicos irmaos, pela trilha de auditoria durável em `saga_history`, e
porque cada etapa nova do pipeline (scan de virus, thumbnails, moderacao)
vira apenas uma linha nova na tabela de transicao, em vez de acoplamento
direto entre servicos.

## Fluxo de eventos

```
VideoUploaded
  -> [orchestrator] PROCESSING, emite ProcessVideoCommand
VideoProcessingCompleted
  -> COMPLETED (terminal), emite VideoStatusCompletedCommand + VideoNotifyRequestedCommand
VideoProcessingFailed
  -> FAILED (terminal), emite VideoStatusFailedCommand + VideoNotifyRequestedCommand
```

A tabela de transicao completa (cada linha `(state, event) ->
(next_state, commands)`) vive em `internal/domain/saga/state_machine.go` e
e coberta exaustivamente por `internal/domain/saga/state_machine_test.go`.

## Estrutura do repositorio

- `internal/domain/saga` — a maquina de estados pura (sem I/O).
- `internal/application/usecases` — conecta a maquina de estados a
  persistencia/mensageria (`handle_event.go`), o padrao outbox para
  mudanca de estado + publicacao de comando atomicas.
- `internal/infrastructure/{db,messaging,config}` — Postgres/GORM,
  RabbitMQ, configuracao de ambiente.
- `internal/presentation/handlers` — endpoints REST Gin somente leitura.
- `cmd/server` — API REST (`GET /api/v1/sagas/:video_id`, health checks).
- `cmd/worker` — consome eventos de dominio e conduz a saga.
- `cmd/outbox-dispatcher` — faz polling da tabela outbox e publica no
  RabbitMQ.
- `docs/adr` — registros de decisao de arquitetura.
- `docs/events` — JSON Schema de cada evento/comando trafegado.
- `docs/postman/collection.json` — colecao Postman com o endpoint de
  debug deste servico.
- `deploy/local` — docker-compose para a stack local completa dos 4
  servicos + infraestrutura (Postgres x4, RabbitMQ, MinIO, Mailhog).
- `charts/saga-orchestrator` — Helm chart (3 Deployments + Service +
  ConfigMap + Secret + HPA).
- `tests/integration` — placeholder (build tag `integration`) para uma
  suite futura contra Postgres/RabbitMQ reais; adiado como stretch goal.

## API REST

- `GET /api/v1/sagas/:video_id` — instancia da saga do video + trilha de
  historico completa (`saga_history`). Util para depurar/demonstrar onde
  um video esta no pipeline.
- `GET /healthz`, `GET /readyz`

Sem endpoints de comando: este servico e guiado apenas por eventos.

## Variaveis de ambiente

| Variavel | Padrao | Descricao |
|---|---|---|
| `SAGA_PORT` | `8084` | Porta HTTP do `cmd/server` |
| `SAGA_DB_DSN` | `host=localhost user=postgres password=postgres dbname=saga_orchestrator port=5432 sslmode=disable` | DSN do Postgres |
| `SAGA_AMQP_URL` | `amqp://guest:guest@localhost:5672/` | URL do RabbitMQ |
| `SAGA_DISPATCH_INTERVAL_MS` | `500` | Intervalo de polling do outbox dispatcher |

## Testes

```bash
make test              # unit + use-case tests
make test-integration   # placeholder, build tag `integration` (stretch goal, nao implementado)
make coverage
```

`go build ./...`, `go vet ./...` e `go test ./...` estao todos verdes.
`internal/domain/saga` tem cobertura exaustiva de cada linha da tabela de
transicao (3 transicoes validas + 5 casos invalidos); `handle_event_test.go`
cobre o caminho feliz completo (upload -> processing -> completed/failed),
eventos duplicados (idempotencia) e eventos para video_id desconhecido
(logados e ignorados, nao tratados como erro).

## Rodando localmente

Veja `deploy/local/README.md` para subir a stack completa dos 4 servicos
com docker-compose e um passo a passo em curl do caminho feliz (registro
-> login -> upload -> acompanhamento da saga -> download do zip).
