# Healthcare AI Platform

An end-to-end healthcare data engineering and AI platform built with synthetic FHIR data, Apache Spark, Delta Lake, Apache Airflow, PostgreSQL, pgvector, FastAPI, and OpenAI.

The platform demonstrates how healthcare data can move from standards-based FHIR ingestion through a medallion data architecture and ultimately support analytics, APIs, and grounded AI-assisted patient questions.

> **Synthetic Data Only**
>
> All patient data used by this project is synthetically generated with Synthea. The repository contains no real patient data or protected health information (PHI).

## Overview

The Healthcare AI Platform is a portfolio project designed to demonstrate a production-oriented healthcare data architecture rather than an isolated ETL pipeline or AI prototype.

The platform combines healthcare interoperability, distributed data processing, incremental pipelines, data quality, analytics, vector search, and retrieval-augmented generation in a single end-to-end system.

Key capabilities include:

* FHIR-based healthcare data ingestion using HAPI FHIR
* Incremental extraction using FHIR `_lastUpdated` checkpoints
* Bronze, Silver, and Gold Delta Lake processing
* PySpark transformation, deduplication, and aggregation
* Delta Lake merge/upsert processing
* Apache Airflow pipeline orchestration
* Automated Silver and Gold data-quality validation
* Structured patient analytics
* FHIR DocumentReference clinical-note ingestion
* Clinical-note sectioning and chunking
* OpenAI embedding generation
* Patient-filtered semantic retrieval with PostgreSQL and pgvector
* Hybrid RAG combining structured patient data with clinical documentation
* Grounded patient-level AI questions with source provenance
* FastAPI patient and AI service endpoints
* Streamlit patient and population exploration
* Spark and Delta Lake performance profiling and optimization

## Architecture

The platform combines a traditional healthcare data engineering pipeline with a hybrid retrieval architecture for patient-level AI questions.

```text
                         HAPI FHIR
                             |
                      FHIR Extraction
                             |
                         Bronze Delta
                             |
                +------------+-------------+
                |                          |
                v                          v
          Structured FHIR            DocumentReference
                |                          |
                v                          v
           Silver Delta              Silver Documents
                |                          |
                v                          v
            Gold Delta              Clinical Note Chunks
                |                          |
                |                          v
                |                     Embeddings
                |                          |
                |                          v
                |                       pgvector
                |                          |
                +------------+-------------+
                             |
                             v
                    Hybrid Patient Context
                             |
                             v
                      AI Assistant
                             |
                    +--------+--------+
                    |                 |
                    v                 v
                 FastAPI          Streamlit
```

Structured patient facts are retrieved deterministically from curated Gold Delta datasets, while relevant clinical documentation is retrieved semantically from pgvector. The two sources are combined into grounded patient context before being supplied to the AI assistant.

## Technology Stack

| Area                 | Technology                    |
| -------------------- | ----------------------------- |
| Healthcare Data      | HL7 FHIR, HAPI FHIR, Synthea  |
| Processing           | Python, PySpark, Apache Spark |
| Data Lake            | Delta Lake                    |
| Orchestration        | Apache Airflow                |
| Operational Database | PostgreSQL                    |
| Vector Search        | pgvector                      |
| Embeddings           | OpenAI                        |
| AI Assistant         | OpenAI                        |
| API                  | FastAPI                       |
| Dashboard            | Streamlit                     |
| Containers           | Docker, Docker Compose        |
| Testing              | pytest, FastAPI TestClient    |


## Data Pipeline

The structured-data pipeline follows a medallion architecture using Delta Lake.

### Bronze

FHIR resources are incrementally extracted from HAPI FHIR using `_lastUpdated` checkpoints. Source records are first preserved as immutable Bronze snapshots and then converted into Bronze Delta tables.

Currently supported FHIR resources:

* Patient
* Encounter
* Condition
* Observation
* MedicationRequest
* DocumentReference

This layer preserves source data while providing a reliable foundation for incremental downstream processing.

### Silver

Silver transformations normalize nested FHIR resources into analytics-friendly Delta tables.

Processing includes:

* Incremental watermark processing
* FHIR structure flattening
* Resource deduplication
* Schema normalization
* Derived fields
* Delta Lake merge/upsert processing
* Automated data-quality validation

Each resource is processed incrementally so that previously processed Bronze records do not need to be transformed again.

### Gold

Gold datasets provide curated patient-level analytical views consumed by the dashboard, API, and AI assistant.

Current Gold datasets include:

* Patient latest vitals
* Patient conditions
* Patient medications
* Patient utilization

These datasets provide deterministic structured patient context for analytics and AI-assisted questions.

## Hybrid RAG

The Patient AI Assistant uses a hybrid retrieval architecture that combines deterministic structured data with semantic retrieval from clinical documentation.

### Structured Retrieval

Patient facts are retrieved directly from curated Gold Delta datasets using the patient identifier.

Structured context includes:

* Latest vital signs
* Active and historical conditions
* Medication history
* Encounter and utilization information

This provides deterministic retrieval for questions that can be answered from normalized patient data.

### Semantic Clinical-Note Retrieval

Unstructured clinical documentation originates from FHIR `DocumentReference` resources.

Clinical notes are:

1. Extracted and decoded from FHIR
2. Split into clinical sections
3. Chunked into retrievable passages
4. Converted into OpenAI embeddings
5. Stored in PostgreSQL using pgvector
6. Retrieved using patient-filtered vector similarity search

Filtering vector retrieval by patient ID prevents semantically similar documentation belonging to other patients from entering the retrieval context.

### Combined Patient Context

For hybrid questions, structured Gold data and relevant clinical-note excerpts are combined before being supplied to the AI assistant.

```text id="f2ayjc"
Patient Question
       |
 +-----+------+
 |            |
 v            v
Gold Delta   pgvector
 |            |
 v            v
Structured   Relevant
Patient      Clinical
Facts        Notes
 |            |
 +-----+------+
       |
       v
Combined Patient Context
       |
       v
AI Assistant
       |
       v
Grounded Answer + Provenance
```

The assistant is instructed to answer from the supplied patient evidence, distinguish historical clinical documentation from current structured state, and identify the sources used to construct the response.

## FastAPI Service

The platform exposes patient analytics and the hybrid AI assistant through a REST API built with FastAPI.

Current endpoints include:

| Method | Endpoint                             | Purpose                               |
| ------ | ------------------------------------ | ------------------------------------- |
| `GET`  | `/health`                            | Service health check                  |
| `GET`  | `/patients/{patient_id}`             | Patient summary                       |
| `GET`  | `/patients/{patient_id}/vitals`      | Latest patient vital signs            |
| `GET`  | `/patients/{patient_id}/conditions`  | Patient conditions                    |
| `GET`  | `/patients/{patient_id}/medications` | Patient medications                   |
| `GET`  | `/patients/{patient_id}/utilization` | Patient utilization                   |
| `POST` | `/patients/{patient_id}/ask`         | Ask a grounded patient-level question |

### Run the API

From the project root:

```bash id="8og2h4"
PYTHONPATH=src uvicorn api.main:app --reload
```

The API is available at:

```text id="k6szq0"
http://localhost:8000
```

Interactive Swagger documentation is available at:

```text id="h2mr48"
http://localhost:8000/docs
```

The `/ask` endpoint uses the same hybrid retrieval architecture as the Patient AI Assistant, combining structured Gold data with relevant clinical documentation before generating a grounded response.

## Streamlit Dashboard

The platform includes a Streamlit dashboard for exploring both patient-level and population-level analytics.

The dashboard provides views for:

* Patient demographics and summary information
* Latest vital signs
* Conditions
* Medications
* Healthcare utilization
* Population-level analytics
* Patient-level AI-assisted questions using the hybrid retrieval pipeline

Run the dashboard from the project root:

```bash
PYTHONPATH=src streamlit run src/dashboard/app.py
```

Streamlit will display the local dashboard URL when the application starts.

## Getting Started

### Prerequisites

The platform is designed to run locally and requires:

* Python 3.11
* Docker and Docker Compose
* Git
* Java 17
* An OpenAI API key for embedding generation and AI-assisted patient questions

Synthea is downloaded separately during setup and is not stored in this repository.

### 1. Clone the Repository

```bash
git clone https://github.com/nathanbeach51/healthcare-ai-platform
cd healthcare-ai-platform
```

### 2. Create the Python Environment

Create and activate a Python 3.11 virtual environment:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

Install the project dependencies:

```bash
pip install -r requirements.txt
```

### 3. Configure the Environment

Copy the example environment file:

```bash
cp .env.example .env
```

Add your OpenAI API key to `.env`:

```text
OPENAI_API_KEY=your-api-key
```

The remaining values in `.env.example` are local-development defaults and can normally be left unchanged.

The OpenAI API key is required for clinical-note embedding generation and AI-assisted patient questions.

### 4. Set Up Synthea

Download the Synthea synthetic patient generator:

```bash
./scripts/setup-synthea.sh
```

Generate synthetic healthcare data:

```bash
./scripts/generate-hospital.sh
```

The generation script currently creates 100 synthetic patients. Generated Synthea data is stored locally and is excluded from source control.

### 5. Start the Platform Services

Start HAPI FHIR, PostgreSQL/pgvector, and Apache Airflow:

```bash
docker compose up -d
```

Docker Compose starts the following infrastructure:

* HAPI FHIR server
* PostgreSQL/pgvector database
* Airflow metadata database
* Airflow API server
* Airflow scheduler
* Airflow DAG processor

HAPI FHIR is available at:

```text
http://localhost:8080/fhir
```

The Airflow interface is available at:

```text
http://localhost:8081
```

### 6. Load Synthetic FHIR Data

Load the generated Synthea FHIR bundles into HAPI FHIR:

```bash
PYTHONPATH=src python src/ingestion/load_bundles.py
```

The loader imports hospital and practitioner reference bundles first, followed by the first 25 generated patient bundles.

## Running the Pipeline

The primary data pipeline is orchestrated by Apache Airflow.

Open the Airflow interface at:

```text
http://localhost:8081
```

Locate the following DAG:

```text
healthcare_pipeline
```

Trigger the DAG manually to run the end-to-end pipeline.

The DAG performs:

```text
FHIR Extraction
      |
      v
Bronze Delta
      |
      v
Silver Delta
      |
      +--------------------------+
      |                          |
      v                          v
Silver Data Quality       Clinical Documents
      |                          |
      v                          v
Gold Delta                 Note Chunking
      |                          |
      v                          v
Gold Data Quality           Embeddings
                                 |
                                 v
                              pgvector
```

The structured branch processes Patient, Encounter, Condition, Observation, and MedicationRequest resources into Silver and Gold datasets with automated quality validation.

The clinical-document branch processes DocumentReference resources into section-aware clinical-note chunks, generates embeddings, and loads the resulting vectors into PostgreSQL/pgvector.

The pipeline uses incremental FHIR checkpoints and Delta processing so subsequent executions process newly available data rather than rebuilding the entire platform from scratch.

## Data Quality

Automated data-quality validation is integrated directly into the Airflow pipeline.

Reusable validation checks support requirements including:

* Non-empty datasets
* Required identifiers and fields
* Identifier uniqueness
* Composite-key uniqueness
* Valid date relationships
* Plausible numeric ranges

Silver validation runs after the structured Silver transformations complete. Gold processing begins only after Silver quality checks pass.

Gold datasets are validated again after aggregation, creating quality gates between major layers of the data pipeline.

A failed validation raises an error and causes the corresponding Airflow task to fail rather than allowing invalid data to silently continue through the pipeline.

## Testing

The project includes automated API tests and a dedicated evaluation suite for the Patient AI Assistant.

### API Tests

FastAPI endpoints are tested using `pytest` and FastAPI's `TestClient`.

External AI calls are mocked during API testing so tests remain deterministic and do not require unnecessary external API requests.

Run the API test suite from the project root:

```bash id="hz3jj5"
PYTHONPATH=src python -m pytest tests/api/test_api.py -v
```

### Patient AI Assistant Evaluation

The AI evaluation suite exercises structured retrieval, semantic retrieval, hybrid retrieval, grounding, provenance, and patient-safety behaviors.

Evaluation scenarios include:

* Structured patient questions
* Clinical-note retrieval
* Questions requiring both structured and unstructured evidence
* Source provenance
* Historical versus current-state interpretation
* Contradictory evidence handling
* Insufficient-evidence behavior

Run the evaluation suite with:

```bash id="2w6j6f"
PYTHONPATH=src python src/ai/evaluate_patient_assistant.py
```

The evaluation harness is intentionally separate from the API tests because it evaluates retrieval and AI behavior rather than deterministic HTTP endpoint behavior.

## Performance Engineering

The project includes a dedicated Spark and Delta Lake performance-engineering exercise focused on measuring and understanding distributed execution rather than applying optimizations without evidence.

The Observation pipeline was selected as the primary workload:

```text id="kl9d7m"
Bronze Observation
       |
       v
Silver Observation
       |
       v
Patient Latest Vitals
```

Synthetic scaled datasets were generated to exercise the pipeline at approximately 1 million and 5 million Bronze Observation rows.

Performance analysis included:

* Spark physical-plan inspection
* Partition and task analysis
* Shuffle analysis
* Data-skew investigation
* Adaptive Query Execution behavior
* Repartitioning experiments
* Persistence/caching experiments
* Delta file-layout experiments
* Predicate and partition-filter inspection
* Gold aggregation and join analysis

### Measurement-Driven Optimization

Optimization decisions were based on measured behavior rather than assumptions.

For example, explicitly repartitioning the approximately 5-million-row Observation workload increased execution time by roughly 35%, so the change was rejected.

Persisting an intermediate dataset that was reused across multiple actions reduced measured runtime by approximately 22%, making persistence beneficial for that workload.

Experiments with intentionally fragmented Delta files and patient-based partitioning also demonstrated that an optimization technique is not automatically beneficial simply because Spark or Delta Lake supports it.

The Gold latest-vitals pipeline was inspected separately and already demonstrated efficient behavior, including column pruning, predicate pushdown, Adaptive Query Execution, window pre-reduction, and a broadcast join. No additional manual tuning was applied because the measurements did not justify it.

Detailed experiments, execution-plan observations, measurements, and conclusions are documented in:

`docs/performanceEngineering.md`

## Project Structure

```text
healthcare-ai-platform/
├── dags/                   # Airflow pipeline orchestration
├── data/
│   └── reference/          # Version-controlled reference data
├── docs/                   # Milestones and performance documentation
├── scripts/                # Synthea setup and generation scripts
├── src/
│   ├── ai/                 # Hybrid RAG, embeddings, retrieval, and evaluation
│   ├── api/                # FastAPI service and schemas
│   ├── config/             # Application configuration
│   ├── dashboard/          # Streamlit application
│   ├── ingestion/          # FHIR client, checkpoints, and bundle loading
│   ├── performance/        # Spark and Delta performance experiments
│   ├── processing/
│   │   ├── gold/           # Curated patient analytics
│   │   └── silver/         # FHIR normalization and transformation
│   ├── quality/            # Silver and Gold data-quality validation
│   └── storage/            # Bronze persistence
├── tests/                  # Automated tests
├── docker-compose.yml      # Local platform infrastructure
├── Dockerfile.airflow      # Airflow runtime image
├── requirements.txt        # Local Python dependencies
└── requirements-airflow.txt
```

Generated healthcare data, Delta tables, Airflow logs, local environments, and the downloaded Synthea source repository are excluded from source control.


## Project Status

The platform currently includes a complete local end-to-end workflow from synthetic FHIR generation through analytics and hybrid AI-assisted patient questions.

Completed major capabilities include:

* Persistent HAPI FHIR and PostgreSQL infrastructure
* Incremental FHIR extraction and checkpointing
* Bronze, Silver, and Gold Delta Lake architecture
* Incremental Delta processing and merge/upsert patterns
* Apache Airflow orchestration
* Automated Silver and Gold data-quality gates
* Streamlit patient and population analytics
* Structured patient AI retrieval
* Clinical-document ingestion and chunking
* OpenAI embeddings and pgvector semantic retrieval
* Hybrid structured/unstructured RAG
* Patient AI evaluation suite
* FastAPI service layer
* Spark and Delta Lake performance engineering

Detailed implementation history and milestone validation are available in `docs/Milestones.md`.

## Future Work

Potential next phases focus on extending the platform toward cloud and production-oriented data engineering patterns, including:

* Pipeline observability and operational monitoring
* Cloud deployment on AWS
* Databricks-based Spark and Delta Lake execution
* Snowflake and dbt integration
* Expanded automated testing and data-quality coverage
* Additional FHIR resources and analytical datasets
* API and application deployment
* CI/CD and automated environment validation

The project is intentionally developed incrementally so new technologies are introduced where they demonstrate a specific architectural or engineering capability rather than being added solely to increase the size of the technology stack.
