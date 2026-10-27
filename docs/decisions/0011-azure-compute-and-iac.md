# 0011. Azure Container Apps, GHCR and Terraform

Status: Accepted (2026-10-28)

## Context
The Azure deployment has to fit an Azure for Students credit of $100, valid for 12 months, and come up and tear
down with single commands. The workload has two long-running consumers, scheduled batch jobs, an HTTP API and a
dashboard.

## Decision
- **Compute: Azure Container Apps (consumption)**
  - ingest: 0.25 vCPU / 0.5 GiB, min replicas 1
  - stream-processor: 0.5 vCPU / 1 GiB, min replicas 1
  - API and Grafana: min replicas 0 with HTTP scaling
  - batch: Container Apps Jobs on cron schedules
- **Images:** GitHub Container Registry (GHCR).
- **Secrets:** Key Vault (HMAC key, Grafana admin password), read through managed identity. Postgres and Event
  Hubs also use Entra ID auth through managed identity, with no passwords.
- **IaC:** Terraform with azurerm 5.11.0. State lives in a storage account created once by `make
  azure-bootstrap`. `make azure-up` and `make azure-down` apply and destroy everything else.
- **CI/CD:** GitHub Actions with OIDC (`azure/login@v3`), with no stored cloud credentials.

## Alternatives considered
- **AKS.** The control plane is free, but node VMs cost more than the whole budget and need cluster
  operations.
- **Azure Container Registry Basic ($5.07/month).** GHCR is free for public images, which removes one service.
- **Bicep.** It is Azure-only. Terraform skills and structure carry over to the GCP mapping in the architecture
  doc.
- **Azure Functions.** It is a poor fit for long-lived SSE and Kafka consumers.

## Consequences
- **Estimated cost if left running 24/7:** about $100/month (about $3.3/day):

  | Item | Monthly |
  |---|---|
  | Event Hubs | ~$23 |
  | Postgres B1ms + 32 GB | ~$16 |
  | always-on Container Apps | ~$54 |
  | Jobs | ~$5–8 |
  | storage and logs | ~$1–2 |

  The project runs in sessions: `make azure-up`, work, then `make azure-down`.
- Destroying the environment deletes the lake and the database. Raw source files are re-downloadable, and
  bronze stream data from a torn-down period is lost. That is acceptable for a demo environment, and
  architecture.md states it.
- An always-active consumer gets no benefit from the Container Apps idle rate. Merging ingest and processing into
  one app would save about $15/month, but it would couple their failure domains, so it is rejected.
