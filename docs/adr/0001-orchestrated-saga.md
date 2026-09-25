# ADR 0001: Saga Orquestrada para o Pipeline de Processamento de Video

## Status

Aceito.

## Contexto

O sistema FIAP X e dividido em quatro microsservicos implantados de forma
independente — upload-service (recebe o video, guarda no MinIO), o
processing-service (extrai frames e gera um zip), o notification-service
(avisa o usuario por e-mail) e este Saga Orchestrator — comunicando-se
exclusivamente atraves de um broker assincrono (RabbitMQ, topic exchange
`video.events`). Diferente do sistema de PDV que inspirou esta arquitetura
(uma transacao distribuida de verdade entre orcamento/pagamento/execucao,
com acoes de compensacao reais — estornar pagamento, cancelar orcamento,
cancelar pedido), este e um **pipeline linear**: upload -> processamento
-> notificacao. Se a extracao de frames falhar, nao ha nada para
desfazer — nenhum pagamento foi cobrado, nenhum recurso de outro servico
foi comprometido. A unica acao possivel diante de uma falha e reportá-la.

## Decisao

Mesmo sem compensacoes reais, manter o **padrao de saga orquestrada**:
este servico continua sendo o unico dono da maquina de estados
(`UPLOADED -> PROCESSING -> COMPLETED|FAILED`) e o unico componente
autorizado a publicar os 4 eventos de comando que movem o pipeline;
upload-service, processing-service e notification-service nunca se
chamam diretamente.

## Justificativa

- **Consistencia com a convencao da equipe**: os outros 3 servicos irmaos
  ja assumem esse papel do orquestrador (nunca publicam comandos,
  apenas fatos), entao manter o padrao aqui evita uma excecao arquitetural
  so porque este pipeline nao tem compensacao.
- **Trilha de auditoria durável**: `saga_history` registra cada transicao
  (from_state, to_state, evento) de forma imutavel — o unico lugar para
  reconstituir o que aconteceu com o video de um usuario, util tanto para
  debugar quanto para demonstrar o sistema.
- **Um lugar para crescer**: adicionar uma etapa nova ao pipeline (scan de
  virus, geracao de thumbnail, moderacao de conteudo) vira apenas novas
  linhas na tabela de transicao deste servico, em vez de acoplamento
  O(n²) servico-a-servico onde cada novo passo exigiria que servicos
  existentes passassem a conhecer uns aos outros.

## Consequencias

- O orquestrador e um ponto unico de coordenacao: se estiver fora do ar,
  videos em andamento nao avancam, mas nada e perdido — RabbitMQ enfileira
  os eventos ate o worker voltar, e todo o estado durável vive em
  `saga_instances`/`saga_history`/`outbox` no Postgres, nao em memoria.
- Sem logica de compensacao para implementar/testar, a maquina de estados
  fica deliberadamente pequena (3 eventos consumidos, 4 comandos
  emitidos, 2 estados terminais) — ver `internal/domain/saga/state_machine.go`
  e sua cobertura exaustiva em `state_machine_test.go`.

## Implementacao

Veja `internal/domain/saga/state_machine.go` para a tabela de transicao e
`internal/domain/saga/instance.go` para a funcao `Apply` pura;
`internal/application/usecases/handle_event.go` a conecta a persistencia
(padrao outbox para mudanca de estado + publicacao de comando atomicas) e
mensageria. `docs/events/*.schema.json` documenta o contrato de cada
evento/comando que o orquestrador consome ou produz.
