# Matriz de contrato — Fujin Provider

**Estado:** contrato independente do Provider. A implementação de cada rota
só evolui com teste do cliente SDK correspondente.

## Descoberta do Provider

| Rota | Contrato | Garantia |
|---|---|---|
| `GET /healthz` | processo acessível | não garante catálogo migrado. |
| `GET /readyz` | prontidão operacional | valida catálogo e diretórios gerenciados. |
| `GET /v1/provider/handshake` | `ProviderHandshake` v1 | publica identidade, revisão, capacidades e endpoints sem credenciais. |

## AWS

| Consumidor | Serviço | Operação | Evidência de compatibilidade |
|---|---|---|---|
| Pre-check, readiness, workers | STS | `AssumeRole`, `GetCallerIdentity` | Credenciais temporárias e account ID retornados por Boto3. |
| Discovery | S3 | `ListObjectsV2` | `Contents`, `NextContinuationToken`, storage class, ETag e `LastModified`. |
| Restore polling/transfer/audit | S3 | `HeadObject`, `GetObject` | `x-amz-restore`, metadata, tags, stream e `Range`. |
| Restore | S3 | `RestoreObject` | Idempotência, estados e expiração persistida. |
| Controle de batches | S3 | `PutObject`, `GetObject`, `ListObjectsV2` no control bucket | Manifests e relatórios Raikou. |
| Restore em lote | S3 Control | `CreateJob`, `DescribeJob` | Estado, relatório e falhas por objeto usados por Raikou. |

## OCI Object Storage

| Consumidor | Operação | Evidência de compatibilidade |
|---|---|---|
| Readiness/reconciliação | namespace, `ListObjects`, `HeadObject` | namespace, paginação, headers e metadados. |
| Transferência direta | `PutObject` | tamanho, `opc-content-sha256`, metadata e ETag. |
| Multipart/resume | `CreateMultipartUpload`, `UploadPart`, `ListMultipartUploadParts`, `CommitMultipartUpload`, `AbortMultipartUpload` | upload ID, partes, checksums e commit idempotente. |
| Deep Audit | `GetObject` | bytes, tamanho e evidência referencial. |

## Dependências deliberadamente reais

| Dependência | Motivo |
|---|---|
| Cofre de segredos do consumidor | Credenciais AWS pertencem ao consumidor e não são armazenadas pelo Fujin. |
| Identidade OCI do consumidor | O consumidor usa o seu próprio signer; Fujin pode aplicar allowlist privada. |

## Fora do contrato

Qualquer rota AWS/OCI não listada acima deve retornar erro explícito e possuir
request ID pesquisável na auditoria Fujin. Ela só entra na matriz após um caso
de uso de consumidor ter sido identificado e testado.
