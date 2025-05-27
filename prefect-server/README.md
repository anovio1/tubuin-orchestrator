# 🛰️ Prefect Server (Self-Hosted)

This folder contains a minimal self-hosted Prefect 2.x server using Docker Compose. It runs the Prefect API and UI locally, allowing you to monitor and orchestrate flows without using Prefect Cloud.
## NOTE: When launching prefect server
read the output, may have to set config
`prefect config set PREFECT_API_URL=http://0.0.0.0:4200/api`

---

## 🚀 Quick Start (for a new project)

### 1. Clone or copy this folder

```bash
cd prefect-server
```

### 2. Start the Prefect server + UI

Run Docker Desktop

```bash
docker compose up
```

This launches the Prefect server and UI at:

> 🌐 http://localhost:4200

---

## ⚙️ Configure Your Local Environment

Point your Prefect CLI to the local server:

```bash
prefect config set PREFECT_API_URL=http://localhost:4200/api
```

Verify the connection:

```bash
prefect status
```

You should see that you're connected to `http://localhost:4200/api`.

---

## 🧱 Start a Prefect Agent

In a new terminal (with your virtual environment activated):

```bash
prefect agent start default
```

This allows your flow runs to be picked up by the agent.

---

## 🗂 Folder Structure

```
prefect-server/
├── docker-compose.yml   # Prefect server and UI config
└── README.md            # This file
```

---

## 🧪 Deploy a Flow (Example)

In your project folder:

```bash
prefect deployment build flow.py:my_flow -n "My Flow"
prefect deployment apply my_flow-deployment.yaml
prefect deployment run "My Flow"
```

---

## 🧼 Stop & Clean Up

To stop the server:

```bash
docker compose down
```

To remove the persisted volume:

```bash
docker volume rm prefect-server_prefect-db
```

---

## 📌 Notes

- This uses SQLite for local testing — not recommended for production
- Port `4200` is used for the UI
- Based on `prefecthq/prefect:2-latest`

---

## 🔗 More

- Docs: https://docs.prefect.io
- CLI Reference: https://docs.prefect.io/latest/concepts/cli/

---
