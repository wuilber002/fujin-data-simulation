# Runbook — payload físico local do Fujin

## Objetivo e isolamento

Este runbook prepara amostras físicas controladas para o **Fujin Provider**. O
modo `SIMULATION` nunca lê, gera, monta ou administra arquivos físicos: usa
somente bytes determinísticos. O volume é montado em leitura no provedor S3
`s3-oci-fujin-provider` e em leitura/escrita exclusivamente no
`s3-oci-fujin-provider-materializer`; API e workers Raijin não recebem esse mount.
O destino continua consumindo, validando e descartando os bytes.

O catálogo em `/var/lib/s3-oci-migration/fujin-provider` é compartilhado entre
esses dois containers, portanto usa SELinux `:z`; `:Z` é exclusivo e faria um
container relabelar o catálogo, impedindo o outro de abrir SQLite.

O operador nunca fornece um path. Pela console **Fujin Provider**, cria um dataset
com nome, quota, seed e perfil; o materializer gera arquivos de alta entropia,
calcula SHA-256 e só marca o dataset `READY` após gravar o manifesto durável.

## Volume dedicado provisionado

O Block Volume OCI dedicado de **15 TB** foi criado e anexado à VM como `/dev/oracleoci/oraclevdb`. Está formatado em XFS e montado persistentemente em `/var/lib/s3-oci-migration/fujin-payloads`, com permissões `0700`, separado de banco, logs e releases.

1. Confirmar espaço livre, IOPS e throughput antes de cada ensaio físico.
2. Iniciar `scripts/start-fujin-provider-runtime.sh`. Somente Fujin Provider recebe
   o bind: o provedor S3 com `:ro` e o materializer com `:rw`.
3. Confirmar que `scripts/start-runtime.sh SIMULATION` não contém esse mount,
   rota, configuração nem reader de payload físico.

Antes do ensaio de 5–10 TB, confirmar novamente que o mount corresponde ao volume de bloco independente e que a telemetria do ambiente está disponível.

## Smoke de 20–100 GB

1. Crie um dataset pequeno pela console Fujin Provider e aguarde `READY`.
2. Associe-o a um bucket S3 LOCAL, escolhendo o multiplicador lógico que
   representa as réplicas do catálogo sem copiar os bytes físicos.
3. Execute discovery, BULK ou STANDARD, range e multipart; pause/resume e
   retry ao menos uma vez. Os tiers cobrem o mesmo reader; diferem apenas na
   janela prevista de restore.
4. Abra a auditoria Fujin e confirme: dataset, bytes físicos únicos,
   referências lógicas, manifesto SHA-256 e nenhuma divergência.
5. Modifique ou remova deliberadamente um arquivo somente em ambiente descartável e confirme a falha de snapshot. Restaure o dataset por seed/manifest, não por cópia manual.

## Operação, cache e limpeza

- Rotule cada execução como `cold start` ou `warm cache` no campo de configuração do cenário. O Fujin nunca limpa o cache do host.
- Injete erros no stream/operação, não no arquivo persistido.
- Purge de cenário remove somente catálogo/evidência. Dataset só pode ser retirado após nenhuma referência de `VirtualObject`; a API apenas enfileira a limpeza e o materializer remove o diretório próprio de modo durável, nunca por `rm` manual do operador.
- Para recuperação, recrie o dataset com o perfil/seed e compare o manifesto SHA-256. Backup de volume é complementar.

## Go / no-go para 5–10 TB

Prosseguir somente com snapshot íntegro, mount isolado, perfil/histograma aprovados, telemetria de host+disco+link, volume livre suficiente e a suíte de regressão verde. Qualquer divergência de snapshot, erro de isolamento ou telemetria ausente bloqueia o ensaio.
