# Healthcare AI Platform Milestones

## 2026-07-30 - Milestone 0: Project Initialization

**Status:** ✅ Complete

### Completed
- Created GitHub repository
- Configured Git and SSH
- Initialized project structure
- Created Docker Compose file
- Started local HAPI FHIR server

### Outcome
A local FHIR server is running and ready for development.

---

## 2026-07-30 - Milestone 1: Local FHIR Server

**Status:** ✅ Complete

### Completed
- HAPI FHIR running in Docker
- Verified landing page
- Verified API endpoints
- Explored FHIR resources

### Outcome
Established the simulated Electronic Health Record (EHR) that will serve as the source system for the data platform.

---

## ✅ Milestone 3 - FHIR Extraction Pipeline (2026-08-03)

- Built reusable `FhirClient`
- Added FHIR health check
- Implemented resource search
- Implemented pagination support
- Added BronzeWriter for raw landing zone
- Extracted Patient, Encounter, Condition, Observation, and MedicationRequest
- Stored raw FHIR Bundles for downstream processing

## ✅ Milestone 4 - Bronze Delta (2026-08-04)

- Installed PySpark and Delta Lake
- Created reusable Spark session
- Read Bronze FHIR bundles
- Flattened Bundle entries into Patient resources
- Added ingestion metadata
- Persisted Patient resources as Delta tables
- Verified Delta read/write operations

## ✅ Milestone 5 - Generic Bronze Processing (2026-08-04)

- Refactored Patient processing into reusable functions
- Added generic Bronze→Delta pipeline
- Processed Patient, Encounter, Condition, Observation, and MedicationRequest
- Added ingestion metadata
- Verified Delta write/read for all resources

## ✅ Milestone 6 - Generic Bronze Processing (2026-08-04)

- Created Silver Layer for patients
- Created Silver Layer for encounters

## ✅ Milestone 7 - Patient Utilization Gold Table (2026-08-05)

- Built Silver Encounter transformation
- Flattened and deduplicated Encounter resources
- Joined Silver Patient and Encounter data
- Calculated encounter count per patient
- Calculated first and latest encounter dates
- Calculated average encounter duration
- Wrote the first Gold Delta table

## ✅ Milestone 8 - Patient/Encounter Gold Table (2026-08-05)

- Created second Gold table, patient / Encounter relations

## ✅ Milestone 9 - Observations / Vitals (2026-08-08)

- Created Bronze Level Observation table
- Created Silver Level Observation table
- Created Gold level Observation table based on Patients

## ✅ Milestone 10 - Patient Medications (2026-08-08)

- Silver medication list
- Gold medication list

## ✅ Milestone 11 - Streamlit Map (2026-08-14)

- Streamlit app 
    - with Population Analytics
    - with Patient Explorer 

## ✅ Milestone 12 - Patient Trends (2026-08-14)

- Added to streamlit app
    - Weight Trends
    - BMI Trends
    - BP Trends 

## Milestone 13 — Incremental Pipeline Processing & Hardening

### Incremental FHIR Extraction
- Add checkpoint-based incremental extraction from HAPI FHIR
- Track the last successful extraction timestamp by resource type
- Use FHIR `_lastUpdated` to retrieve only new or updated resources
- Validate incremental extraction across Patient, Encounter, Condition,
      Observation, and MedicationRequest

### Incremental Bronze Processing
- Process only new Bronze JSON files into Bronze Delta
- Track previously processed source files using `_source_file`
- Append new FHIR resources to existing Bronze Delta tables
- Support FHIR schema evolution when new resource fields appear

### Incremental Silver Processing
- Process only newly ingested Bronze records
- Add reusable Delta incremental-processing utilities
- Deduplicate resources using FHIR resource IDs and source timestamps
- Merge Patient updates into Silver Delta
- Merge Encounter updates into Silver Delta
- Merge Condition updates into Silver Delta
- Merge Observation updates into Silver Delta
- Merge MedicationRequest updates into Silver Delta
- Preserve latest resource versions during incremental processing

### Pipeline Validation
- Add validation for duplicate resource IDs
- Add validation for required patient/resource identifiers
- Verify incremental Patient processing
- Verify incremental Encounter processing
- Verify incremental Condition processing
- Verify incremental Observation processing
- Verify incremental MedicationRequest processing
- Verify Gold tables update correctly after incremental Silver processing
- Verify Streamlit dashboard displays newly loaded patients

### Condition Classification
- Review previously unclassified Synthea conditions
- Expand condition classification rules
- Separate clinical conditions, social factors, history, and behavioral factors
- Add reporting for remaining unclassified conditions

### Logging & Debugging
- Add configurable debug logging
- Standardize Silver pipeline logging
- Move verbose Spark `.show()` output behind debug logging
- Add concise row-count and success messages for Silver transforms

### HAPI FHIR Persistence
- Replace HAPI's temporary in-memory database with PostgreSQL
- Add PostgreSQL service to Docker Compose
- Add persistent Docker volume for PostgreSQL data
- Verify FHIR data survives container shutdown and restart

### Synthea Loading
- Load hospital bundles before patient bundles
- Load practitioner/provider bundles before patient bundles
- Add configurable patient bundle limit
- Improve bundle loader logging
- Validate dependency-aware Synthea loading

### End-to-End Validation
- Generate/load a new batch of Synthea patients
- Extract incremental FHIR resources
- Process incremental Bronze data
- Process incremental Silver data
- Rebuild/update Gold analytics
- Validate new patients in the Streamlit dashboard
- Verify a second run does not unnecessarily reprocess data
- Verify HAPI patient data persists across Docker restarts

## Milestone 14 — Airflow Orchestration ✅

- Added Apache Airflow 3.3.1 to the Docker environment
- Added persistent PostgreSQL Airflow metadata database
- Created healthcare pipeline DAG
- Orchestrated FHIR extraction and Bronze Delta processing
- Added parallel Silver transformations
  - Patient
  - Encounter
  - Condition
  - Observation
  - MedicationRequest
- Added Silver completion/validation checkpoint
- Added parallel Gold transformations
  - Patient conditions
  - Latest patient vitals
  - Patient medications
  - Patient utilization
- Added extraction retries
- Configured Docker networking between Airflow and HAPI FHIR
- Configured Spark/Delta execution inside Airflow
- Verified task-level logging
- Successfully tested complete FHIR → Bronze → Silver → Gold pipeline

## Milestone 15 — Data Quality & Observability ✅

- Added reusable PySpark data-quality checks
  - Non-empty datasets
  - Non-null columns
  - Unique keys
  - Composite uniqueness
  - Column ordering/comparison checks
  - Numeric range validation
- Added Silver quality validation
  - Patient
  - Encounter
  - Condition
  - Observation
  - MedicationRequest
- Added Gold quality validation
  - Patient latest vitals
  - Patient conditions
  - Patient medications
  - Patient utilization
- Added healthcare-specific sanity checks
  - Encounter start <= encounter end
  - Plausible vital-sign ranges
  - Non-negative aggregate counts
- Integrated Silver and Gold quality gates into Airflow
- Verified validation failures prevent downstream execution
- Verified successful end-to-end pipeline execution

## Milestone 16 — Patient AI Assistant ✅

- Added patient-level context retrieval from Gold Delta datasets
- Created LLM-friendly structured patient context
- Integrated OpenAI Responses API using GPT-5.6 Luna
- Added grounded healthcare system prompt
- Added guardrails against unsupported diagnosis and treatment recommendations
- Added source provenance to AI responses
- Added repeatable AI evaluation suite
  - Factual retrieval
  - Missing-data handling
  - Clinical guardrails
  - Source attribution
- Achieved 7/7 evaluation checks
- Integrated patient Q&A into Streamlit Patient Explorer

## Milestone 17 — Hybrid RAG with Clinical Documents (9-6-2026)✅

**Status:** Complete

### Goal

Extend the patient AI assistant beyond structured Gold data by adding
semantic retrieval over unstructured FHIR clinical documents.

The assistant now combines:

- Structured patient context from Gold Delta tables
- Semantically relevant clinical-note excerpts from pgvector
- Source provenance for both structured and unstructured evidence

### Architecture

FHIR DocumentReference
→ Bronze Delta
→ Silver DocumentReference
→ Clinical Note Chunks
→ OpenAI Embeddings
→ pgvector
→ Semantic Retrieval
→ Hybrid Patient Context
→ AI Assistant

Structured Gold datasets continue to provide deterministic retrieval for
patient facts such as vitals, conditions, medications, and utilization.

Clinical notes provide semantic retrieval for information contained in
unstructured documentation.

### Completed

- Added `DocumentReference` to incremental FHIR extraction
- Added generic Bronze Delta processing for DocumentReference
- Built incremental Silver DocumentReference transformation
- Decoded base64 FHIR text attachments into clinical-note text
- Implemented section-aware clinical-note chunking
- Added deterministic chunk IDs for incremental processing
- Added incremental chunk metadata and Delta merge behavior
- Generated clinical-note embeddings using `text-embedding-3-small`
- Implemented batched embedding generation to avoid driver memory issues
- Added incremental embedding processing
- Enabled pgvector in the HAPI PostgreSQL instance
- Created pgvector storage for clinical-note embeddings
- Implemented incremental Delta-to-pgvector loading with checkpoints
- Built reusable patient-scoped semantic clinical-note retrieval
- Combined semantic note retrieval with structured Gold patient context
- Added clinical-note provenance to AI assistant responses
- Updated prompting to distinguish historical note evidence from current
  structured patient state
- Integrated hybrid retrieval into the Streamlit Patient Explorer
- Integrated DocumentReference → chunk → embedding → pgvector processing
  into the Airflow DAG
- Added Docker environment configuration for OpenAI and pgvector access
- Expanded patient-assistant evaluation suite to structured, semantic,
  hybrid, provenance, contradiction, and safety cases
- Achieved 11/11 passing patient-assistant evaluation tests

### Incremental Pipeline Validation

Validated the complete incremental RAG workflow under Airflow.

A no-change DAG run correctly produced no new downstream RAG work.

A new FHIR DocumentReference was then created for an existing patient and
successfully flowed through:

1. Incremental FHIR extraction
2. Bronze Delta
3. Silver DocumentReference
4. Clinical-note chunking
5. Embedding generation
6. pgvector loading
7. Semantic retrieval
8. Hybrid AI assistant response

The newly added clinical note was successfully retrieved by the patient
assistant and used to answer a question with the correct DocumentReference
provenance.

### Issues Resolved

- Refactored embedding generation from `.collect()` to batched
  `toLocalIterator()` processing to avoid Spark driver memory pressure
- Migrated existing embeddings to incremental metadata without regenerating
  all embeddings
- Moved OpenAI client initialization to task execution time so Airflow DAG
  parsing does not require API credentials
- Configured Docker networking for OpenAI and pgvector Airflow tasks
- Added pgvector checkpoint bootstrap to avoid unnecessary full reloads
- Identified and corrected a Condition Silver transform that was
  accidentally writing DocumentReference data, causing concurrent Delta
  merge conflicts
- Restored the Condition Silver transform and correct `condition_id` merge
  behavior

### Result

The platform now supports two complementary retrieval strategies:

**Structured retrieval**
- Exact patient-level facts
- Gold Delta datasets
- Deterministic lookup by patient ID

**Semantic retrieval**
- Unstructured clinical documentation
- Vector similarity search
- Patient-filtered pgvector retrieval

These sources are combined into a grounded patient context before being
sent to the AI assistant, allowing questions to be answered using both
structured healthcare data and relevant clinical documentation while
preserving source provenance.

## Milestone 18 — FastAPI Patient Service ✅

**Status:** Complete

### Goal

Add a REST API service layer to the Healthcare AI Platform so structured
patient data and the hybrid AI assistant can be accessed independently
of the Streamlit user interface.

The API provides a reusable application boundary between client
applications and the platform's underlying data and AI services.

### Architecture

Client
→ FastAPI
→ Patient / AI Services
→ Gold Delta + pgvector
→ Structured JSON Response

FastAPI reuses the existing patient context and hybrid RAG components
rather than duplicating data retrieval or AI logic inside the API layer.

### Completed

- Added FastAPI application structure
- Added health-check endpoint
- Added patient-level structured data endpoint
- Added focused patient endpoints for:
  - Latest vitals
  - Conditions
  - Medications
  - Utilization
- Exposed the hybrid patient AI assistant through a REST endpoint
- Added Pydantic request and response models
- Added structured clinical-note retrieval provenance to AI responses
- Added patient-not-found handling with HTTP 404 responses
- Added automatic OpenAPI / Swagger documentation
- Added module documentation for the API application, routes, and schemas
- Updated the project README with API architecture and usage
- Added automated API tests using FastAPI TestClient
- Mocked external AI calls during automated testing to keep tests
  deterministic and avoid unnecessary API requests
- Achieved 8/8 passing API tests

### API Endpoints

```text
GET  /health
GET  /patients/{patient_id}
GET  /patients/{patient_id}/vitals
GET  /patients/{patient_id}/conditions
GET  /patients/{patient_id}/medications
GET  /patients/{patient_id}/utilization
POST /patients/{patient_id}/ask