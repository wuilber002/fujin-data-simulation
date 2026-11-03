# Aceite operacional — Fujin LOCAL

Em uma VM Oracle Linux provisionada, execute após criar o perfil OCI LOCAL e
chamar `sudo /usr/local/sbin/s3-oci-start-fujin-local-runtime`. Em ambiente
de desenvolvimento, o equivalente é `docker compose --profile local up -d`.
Use sempre o túnel SSH local. O Raijin permanece em `REAL`; não há seletor
LOCAL na UI Raijin.

> Para criar uma VM de aceite via `terraform/orm`, forneça um arquivo de
> variáveis aprovado com `compartment_ocid`, `availability_domain`,
> `subnet_ocid`, `image_ocid`, `ssh_public_key` e
> `secrets_compartment_ocid`. Este repositório não contém esses valores nem
> estado de uma VM existente.
> Use `terraform/orm/fujin-local-acceptance.tfvars.example` como contrato
> inicial e mantenha a cópia preenchida fora do Git.

## Pré-condições

1. Confirme que o host provisionou o Secret local
   `/etc/s3-oci-migration/secrets/fujin_local_admin_token` (modo `0600`) e
   abra `http://127.0.0.1:8080/fujin/`. Informe o token somente na console e
   confirme saúde, DNS/TLS e auditoria.
2. Confirme o volume dedicado em
   `/var/lib/s3-oci-migration/fujin-payloads`. Pela console **Fujin LOCAL**,
   solicite a geração de um dataset com nome, seed, perfil, quantidade e quota.
   Não informe nem importe paths do host: o `fujin-local-materializer` cria os
   arquivos, o manifesto e os SHA-256 de modo durável; só um dataset `READY`
   pode ser associado ao bucket S3 LOCAL. O provedor S3 o monta somente para
   leitura. Crie também um bucket OCI LOCAL independente.

   O catálogo SQLite é compartilhado por provedor e materializer com rótulo
   SELinux `:z` (compartilhado), nunca `:Z` (privado); os dois processos
   precisam abrir o mesmo catálogo sem relabel concorrente.
3. Gere o perfil OCI LOCAL isolado, preservando o perfil REAL:

   ```bash
   sudo /usr/local/sbin/configure-fujin-local-oci-runtime \
     --source /etc/s3-oci-migration/oci-runtime.json \
     --output /etc/s3-oci-migration/oci-runtime-local.json \
     --namespace local
   ```

4. Copie o pacote sintético para o Vault sem alterar seu schema. Na conexão
   AWS do Raijin configure STS/S3/S3 Control privados, `virtual` e a CA
   `/etc/fujin-local-ca/fujin-local.crt`; configure o endpoint OCI privado no
   runtime como `https://oci.us-east-1.fujin.internal`, usando a mesma CA.

## Aceite E2E

1. Crie a source no Raijin, execute pré-check e discovery. Confirme na
   auditoria Fujin `STS_*`, `S3_LIST_OBJECTS_V2` e os request IDs.
2. Crie waves, solicite Batch Restore e confirme `S3CONTROL_CREATE_JOB`,
   restores persistentes, `x-amz-restore` e expiração por relógio real.
3. Aguarde a lane, valide transferência direta e multipart, pause/resume,
   retry e reprocessamento. Confirme `OCI_*` e os objetos referenciais na
   console Fujin.
4. Execute relatório e Deep Audit no Raijin; confirme leitura dos bytes
   físicos referenciados e request IDs correlacionáveis no Fujin.
5. Com multipart/restore ativo, reinicie somente `fujin-local`; confirme
   recuperação por catálogo persistente, retomada segura e ausência de objeto
   OCI permanente para upload abortado. Em ensaio separado, interrompa e
   reinicie `fujin-local-materializer` durante uma geração: o job deve retomar
   sem publicar arquivos parciais e só concluir como `READY` após o manifesto.
6. Tente excluir dataset/bucket com referências/operacões ativas e confirme
   bloqueio. Após revogar buckets e finalizar operações, confirme que a API
   enfileira a limpeza e que o materializer remove somente o diretório
   `datasets/<id>` permitido, com a liberação auditada.
7. Execute os perfis `real` e `simulation` separadamente e rode a regressão.

Nenhum item pode ser marcado aceito apenas pela UI: registre request ID,
evento de auditoria e resultado de cada etapa.
