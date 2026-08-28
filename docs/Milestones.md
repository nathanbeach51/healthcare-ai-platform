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

## Milestone 14 — Airflow Orchestration
 - Airflow + Docker set up and config to run Airflow locally
 - DAG which would load Bronze, Bronze Delta, Silver Delta, and Gold Delta
 - DAG processes the pipelines in order and uses the previous level for dependency management