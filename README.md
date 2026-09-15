# Healthcare AI Platform

An end-to-end healthcare data engineering and AI platform built using
synthetic FHIR data, Apache Spark, Delta Lake, Apache Airflow, PostgreSQL,
pgvector, FastAPI, and OpenAI.

The project demonstrates how healthcare data can move from FHIR ingestion
through a medallion-style data architecture and ultimately support both
analytics and grounded AI-assisted patient questions.

> **Synthetic Data Only**
>
> All patient data in this project is synthetically generated using Synthea.
> The repository does not contain real patient data or protected health
> information (PHI).

---

## Overview

The Healthcare AI Platform is a portfolio project designed to demonstrate
an end-to-end healthcare data architecture rather than an isolated ETL
pipeline.

The platform currently supports:

- FHIR-based healthcare data ingestion
- Incremental extraction using FHIR `_lastUpdated` checkpoints
- Bronze, Silver, and Gold Delta Lake processing
- Spark-based transformation and aggregation
- Airflow pipeline orchestration
- Automated Silver and Gold data-quality validation
- Structured patient analytics
- Clinical document ingestion from FHIR DocumentReference resources
- Clinical-note sectioning and chunking
- OpenAI embedding generation
- Semantic clinical-note retrieval using PostgreSQL and pgvector
- Hybrid RAG combining structured patient data with clinical documentation
- Grounded AI-assisted patient questions with source provenance
- Streamlit patient exploration
- FastAPI patient and AI service endpoints

---

## Architecture

The platform combines traditional healthcare data engineering with
retrieval-augmented generation.

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
                FastAPI           Streamlit

## Key Capabilities

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
| Containers           | Docker / Docker Compose       |


## Data Pipeline

The primary structured-data pipeline follows a medallion-style
architecture.

Bronze

FHIR resources are incrementally extracted from HAPI FHIR and preserved
as immutable source records before being converted into Bronze Delta
tables.

Current resources include:

Patient
Encounter
Condition
Observation
MedicationRequest
DocumentReference
Silver

Silver transformations normalize FHIR structures into analytics-friendly
tables.

Processing includes:

Incremental watermark processing
FHIR structure flattening
Resource deduplication
Schema normalization
Derived fields
Delta merge/upsert processing
Automated data-quality validation
Gold

Gold datasets provide patient-level analytical views used by the
dashboard and AI assistant.

Current Gold datasets include:

Patient latest vitals
Patient conditions
Patient medications
Patient utilization

## Hybrid RAG

Hybrid RAG

The patient AI assistant uses two complementary retrieval strategies.

Structured Retrieval

Patient facts are retrieved deterministically from curated Gold Delta
datasets.

This is appropriate for questions involving structured information such
as current vitals, medications, condition history, and utilization.

Semantic Retrieval

FHIR DocumentReference clinical notes are:

Extracted and decoded
Split into clinical sections
Chunked into retrievable passages
Converted into embeddings
Stored in PostgreSQL using pgvector
Retrieved using patient-filtered vector similarity search

Relevant clinical-note excerpts are combined with structured Gold data
to create the context supplied to the AI assistant.

The assistant is instructed to remain grounded in the supplied evidence,
distinguish historical documentation from current structured state, and
provide source provenance.

## FastAPI Service

The platform includes a REST API for accessing patient data and the
hybrid AI assistant.

Current endpoints include:

GET  /health
GET  /patients/{patient_id}
POST /patients/{patient_id}/ask

Interactive API documentation is available through FastAPI's Swagger UI
when the service is running:

http://localhost:8000/docs

Run the API locally with:

PYTHONPATH=src uvicorn api.main:app --reload

## Data Quality

The project includes automated validation at multiple layers.

Silver and Gold data-quality checks validate requirements including:

Non-empty datasets
Required identifiers
Identifier uniqueness
Composite-key uniqueness
Valid date relationships
Plausible numeric ranges

The patient AI assistant also includes an evaluation suite covering
structured retrieval, semantic retrieval, hybrid retrieval, provenance,
historical/current-state handling, contradiction handling, and safety
behavior.

FastAPI endpoints are tested using FastAPI's TestClient. External AI
calls are mocked during API tests to keep tests deterministic and avoid
unnecessary external API requests.

Run API tests with:

PYTHONPATH=src python -m pytest tests/api/test_api.py -v
Project Status

The platform is under active development.

Completed major capabilities include:

FHIR ingestion and persistence
Bronze / Silver / Gold Delta architecture
Incremental processing
Airflow orchestration
Automated data quality
Patient analytics dashboard
Structured patient AI assistant
Hybrid clinical-document RAG
Incremental embedding and pgvector pipeline
FastAPI service layer (in progress)

## Project Structure

See docs/Milestones.md for detailed development history and milestone
validation.

## Local Setup

## Running the Pipeline

## Running the API

## Testing

## Synthetic Data / PHI Disclaimer

## Milestones

## Future Work