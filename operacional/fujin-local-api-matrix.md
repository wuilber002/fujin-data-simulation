# Matriz de contrato — Fujin LOCAL

**Estado:** inventário inicial, produzido a partir dos consumidores atuais em
`app/real_worker.py` e `app/main.py`. A implementação de cada rota somente
começa após teste do cliente SDK correspondente.

## AWS

| Consumidor Raijin | Serviço | Operação | Evidência de compatibilidade |
|---|---|---|---|
| Pre-check, readiness, workers | STS | `AssumeRole`, `GetCallerIdentity` | Credenciais temporárias e account ID retornados por Boto3. |
| Discovery | S3 | `ListObjectsV2` | `Contents`, `NextContinuationToken`, storage class, ETag e `LastModified`. |
| Restore polling/transfer/audit | S3 | `HeadObject`, `GetObject` | `x-amz-restore`, metadata, tags, stream e `Range`. |
| Restore | S3 | `RestoreObject` | Idempotência, estados e expiração persistida. |
| Controle de batches | S3 | `PutObject`, `GetObject`, `ListObjectsV2` no control bucket | Manifests e relatórios Raikou. |
| Restore em lote | S3 Control | `CreateJob`, `DescribeJob` | Estado, relatório e falhas por objeto usados por Raikou. |

## OCI Object Storage

| Consumidor Raijin | Operação | Evidência de compatibilidade |
|---|---|---|
| Readiness/reconciliação | namespace, `ListObjects`, `HeadObject` | namespace, paginação, headers e metadados. |
| Transferência direta | `PutObject` | tamanho, `opc-content-sha256`, metadata e ETag. |
| Multipart/resume | `CreateMultipartUpload`, `UploadPart`, `ListMultipartUploadParts`, `CommitMultipartUpload`, `AbortMultipartUpload` | upload ID, partes, checksums e commit idempotente. |
| Deep Audit | `GetObject` | bytes, tamanho e evidência referencial. |

## Dependências deliberadamente reais

| Dependência | Motivo |
|---|---|
| OCI Vault | Raijin continua obtendo a Secret AWS pelo fluxo já existente. |
| OCI Instance Principal/Identity | Raijin continua usando o signer normal; Fujin aplica allowlist privada na primeira versão. |

## Fora do contrato

Qualquer rota AWS/OCI não listada acima deve retornar erro explícito e possuir
request ID pesquisável na auditoria Fujin. Ela somente entra na matriz após um
consumidor Raijin ser identificado.
