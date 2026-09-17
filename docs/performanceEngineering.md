# Spark & Delta Performance Engineering

Milestone 19 evaluates Spark and Delta Lake performance using the
Healthcare AI Platform's existing Observation processing pipeline.

The goal was not to apply generic Spark tuning recommendations, but
to establish a baseline, inspect actual execution behavior, identify
bottlenecks, test targeted changes, and measure the results.

## Test Environment

Performance testing was performed locally using PySpark and Delta Lake.

The production pipeline was not modified during benchmarking.
Performance-specific Delta datasets and profiling scripts were created
under `data/performance` and `src/performance`.

## Observation Pipeline

The primary pipeline examined was:

FHIR Observation
→ Bronze Delta
→ Silver Observation
→ Gold Patient Latest Vitals

Observation was selected because it is one of the highest-volume FHIR
resources and includes filtering, exploding nested components, window
operations, aggregation, pivoting, and joins.

## Baseline Dataset

Original Bronze Observation resources:

- 78,462 rows
- 7 input partitions

After transforming simple and component observations:

- 62,928 simple observation rows
- 41,830 component rows
- 104,758 combined rows
- 42,341 deduplicated Silver rows

The primary shuffle occurred during Silver deduplication:

    Window.partitionBy(
        "observation_id",
        "observation_code",
    )

Spark planned 200 shuffle partitions. Adaptive Query Execution
coalesced these partitions at runtime.

The baseline shuffle wrote approximately 5.1 MiB.

No significant partition skew was observed.

## Scaled Performance Testing

A separate performance dataset was created by replicating existing
synthetic FHIR Observation resources while generating unique resource
IDs.

This avoided modifying production Bronze data or ingestion
checkpoints.

### 1 Million Row Test

Bronze rows:

- 1,020,006

Results:

- Transformation time: 7.3 seconds
- Shuffle write: approximately 41 MiB

### 5 Million Row Test

Bronze rows:

- 5,021,568

Results:

- Transformation time: 14.84 seconds
- Largest observed shuffle: approximately 143 MiB
- Shuffle stage tasks: 12

Task durations did not indicate severe data skew.

## Repartition Experiment

The 5-million-row dataset initially had seven input partitions.

A test was performed using:

    bronze.repartition(14)

Hypothesis:

Increasing input parallelism might improve processing performance.

Result:

- Baseline: 14.84 seconds
- Repartitioned: approximately 20 seconds

Performance degraded by approximately 35%.

The additional shuffle introduced by repartitioning cost more than the
additional parallelism saved.

Conclusion:

Low input partition count alone is not sufficient justification for
calling repartition(). Partition changes should be based on measured
execution behavior.

## Persistence Experiment

The production Silver pipeline performs several actions against the
same transformed DataFrame, including validation counts and the final
write/merge workflow.

Without persistence:

- 20.73 seconds

With the transformed Silver DataFrame persisted:

- 16.17 seconds

Improvement:

- 4.56 seconds
- approximately 22%

Persistence prevented Spark from repeatedly recomputing the expensive
Bronze → transform → window lineage.

Individual validation checks against the materialized DataFrame were:

- Null patient check: 0.26 seconds
- Null observation-code check: 0.23 seconds
- Duplicate-key check: 1.33 seconds

The duplicate check was more expensive because its groupBy operation
requires redistribution.

## Delta File Layout Experiment

Two versions of the approximately 5-million-row Bronze dataset were
tested.

Normal layout:

- 7 physical Parquet files
- 6 Spark scan partitions
- Full count: 4.87 seconds

Small-file test:

- 200 physical Parquet files
- 8 Spark scan partitions
- Full count: 4.96 seconds

Spark combined multiple small files into scan partitions rather than
creating one Spark partition per physical file.

At this local dataset size, 200 files did not create a meaningful
performance penalty.

This experiment demonstrated that these are separate concepts:

- Delta/Parquet physical files
- Spark input partitions
- Spark shuffle partitions

The small-file problem is expected to become more significant with
large numbers of files and object-storage metadata/open costs.

## Predicate Pushdown and Partition Pruning

Patient 200009 represented 574,592 rows in the scaled dataset.

Query against the normal layout:

- 574,592 rows
- 4.68 seconds

Spark pushed the nested patient predicate into the Parquet scan.

A separate experimental Delta table was physically partitioned by
patient ID.

The physical plan then showed:

    PartitionFilters:
    [isnotnull(patient_id), (patient_id = 200009)]

This confirmed partition pruning.

Partitioned query:

- 574,592 rows
- 4.76 seconds

Despite successful pruning, runtime did not materially improve in the
local test environment.

Patient ID was used to demonstrate partition pruning and is not
recommended here as a production partitioning strategy because its
high cardinality could create excessive directories and small files.

## Gold Latest Vitals Pipeline

A scaled Silver Observation table containing 2,709,824 rows was used
to profile the Gold patient-latest-vitals transformation.

The physical plan showed:

1. Column pruning
2. Predicate pushdown for the seven vital codes
3. Window shuffle by patient_id and observation_code
4. Partial and final WindowGroupLimit operations
5. Hash aggregation for pivot processing
6. Additional aggregation exchanges
7. BroadcastExchange
8. BroadcastHashJoin

The final Gold dataset contained 75 patient rows.

Spark chose to broadcast the pivoted vital dataset even though the
upstream Observation table contained approximately 2.7 million rows.

This demonstrates that join strategy depends on the size of the
datasets at the join boundary rather than simply the size of the
original source.

The Gold Delta write completed in approximately:

- 3.2 seconds

The largest observed shuffle write was only:

- 29.3 KiB
- 525 records

With 75 patients and seven vital types, there are at most:

    75 × 7 = 525

patient/vital combinations.

The pipeline therefore reduces the large Observation dataset before
later aggregation and join operations.

## Key Findings

Performance profiling produced several practical conclusions:

1. Wide transformations should be identified from the physical plan
   rather than assumed from DataFrame size.

2. Exchange nodes clearly identify shuffle boundaries.

3. Adaptive Query Execution can reduce an excessive configured shuffle
   partition count at runtime.

4. Repartitioning is not automatically an optimization. An unnecessary
   repartition made the tested pipeline approximately 35% slower.

5. Persistence can provide substantial benefits when multiple actions
   repeatedly evaluate the same expensive lineage.

6. Predicate pushdown and partition pruning are different optimization
   mechanisms.

7. Physical Parquet files, Spark input partitions, and shuffle
   partitions are separate concepts.

8. Early data reduction can dramatically reduce downstream shuffle
   volume.

9. Broadcast decisions depend on the size of data at the join boundary,
   not the size of the upstream source table.

10. An optimization visible in a physical plan does not necessarily
    produce a measurable runtime improvement at small local scale.

## Conclusion

The most effective optimization identified during this milestone was
persisting a reused transformed DataFrame, which improved the measured
multi-action Silver workload by approximately 22%.

Other experiments were intentionally rejected when measurements did
not support them. In particular, manually increasing partitions added
an unnecessary shuffle and degraded performance.

The milestone demonstrates a measurement-driven Spark performance
workflow:

    baseline
       ↓
    inspect
       ↓
    hypothesize
       ↓
    benchmark
       ↓
    accept or reject
       ↓
    document