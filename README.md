# Tubuin Orchestrator

The data pipeline orchestration service for the Tubuin project, powered by Prefect.

Its role is to source match replays and metadata, extract raw data through parsing, generate artifacts for consumers, and populate the data backend for the Tubuin analytical platform.

## Core Responsibilities

- **Ingestion:** Processes `.mpk` (Messagepack) files from completed matches
- **Transformation:** Runs `message_pack_processor` to create artifacts
- **Deployment:** Uploads processed artifacts to a cloud object store (Cloudflare R2).
- **Cataloging:** Notifies the `tubuin-api` of new, available match data.

## Architecture

This system is built as a series of observable dataflows using the [Prefect](https://www.prefect.io/) orchestration framework.

## Getting Started

This project is designed to be run as a service. For developers looking to run the orchestration layer locally, please see the **[Local Setup Guide](./docs/LOCAL_SETUP.md)**.

---
*This repository is part of the larger Tubuin Analytics ecosystem.*