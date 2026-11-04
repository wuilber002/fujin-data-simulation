# Fujin Data Simulation

Fujin is an independent private-cloud laboratory. It owns deterministic
datasets, simulated S3/Glacier and OCI Object Storage behavior, restore
policies, fault profiles, payload materialization and its administrative API.

It has its own catalog and lifecycle. A consumer such as Raijin reaches Fujin
only through the normal AWS and OCI protocol contracts; Fujin never imports a
consumer's queue, workers, credentials or database.

## Local development

Requirements: Docker Compose or Podman Compose, with the service bound to
loopback unless it is protected by an equivalent private network boundary.

```bash
docker compose up --build -d
curl -fsS http://127.0.0.1:8090/healthz
curl -fsS http://127.0.0.1:8091/healthz
curl -fsS http://127.0.0.1:8090/readyz
curl -fsS http://127.0.0.1:8091/readyz
```

The Fujin administration console is available at
`http://127.0.0.1:8090/console`. Persistent catalogues and generated payloads
are stored only in the `fujin-data` volume.

For validation without containers:

```bash
PYTHONPATH=. pytest -q -p no:cacheprovider
```

## Instalação manual em uma VM

O instalador usa Podman, mantém os dados em `/var/lib/fujin-data-simulation`
e publica a console e o simulador somente em loopback. Não requer Terraform
nem compartilha banco, volume ou processo com o consumidor.

```bash
curl -fsSL https://raw.githubusercontent.com/wuilber002/fujin-data-simulation/main/scripts/install-fujin.sh \
  | sudo bash

sudo systemctl status fujin-data-simulation.service
curl -fsS http://127.0.0.1:8090/healthz
curl -fsS http://127.0.0.1:8091/healthz
curl -fsS http://127.0.0.1:8090/readyz
curl -fsS http://127.0.0.1:8091/readyz
```

Para expor os serviços com TLS, coloque um proxy reverso à frente dos dois
endpoints ou altere `/etc/fujin-data-simulation/fujin.env` e reinicie o
serviço. As credenciais de qualquer consumidor devem ser configuradas nele;
Fujin não recebe nem armazena credenciais AWS ou OCI reais.

## Integration boundary

Before connecting a consumer, create and validate a dataset, create the
simulated source and destination buckets, then enable the provider endpoints.
Use the generated CA and private endpoint profile exposed by the Fujin console.
The consumer must be deployed separately and must not share Fujin's storage
volume or database.
