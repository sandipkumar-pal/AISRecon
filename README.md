# AIS–RF Data Fusion: Technical Requirements & Data Pipeline (for BRD)

## 1) Scope & Objectives

- **Objective A (Gap Fill):** Reconstruct vessel trajectories during AIS-silent periods using RF detections.
- **Objective B (Spoof Correction):** Detect and correct AIS location anomalies using RF-derived geolocations.
- **Delivery:** Read from **S3 or local**, produce **N6-ready** fused dataset with confidence labels, audit trail, and metadata.

## 2) Data Inputs

### 2.1 AIS Input (raw)

- **Source:** S3 (`s3://<bucket>/ais/raw/YYYY/MM/DD/*.parquet`) or local (`/data/ais/raw/`).
- **Minimum fields:** `{mmsi, imo?, call_sign?, ts_utc, lat, lon, sog, cog, nav_status?, position_accuracy?, source_system}`
- **Assumptions:** Timestamps are UTC ISO 8601 or epoch ms; lat/lon WGS-84.

### 2.2 RF Input (raw)

- **Source:** S3 (`s3://<bucket>/rf/raw/YYYY/MM/DD/*.parquet`) or local (`/data/rf/raw/`).
- **Minimum fields:** `{rf_id, ts_utc, est_lat, est_lon, freq_band, rssi?, toa?, tdoa_cluster_id?, geoloc_method, geo_uncertainty_m}`
- **Assumptions:** RF geolocation includes uncertainty radius; one or more detections per time slice.

## 3) Preprocessing & Normalization

### 3.1 Common

- Time normalization to UTC; enforce `ts_utc` as integer epoch ms.
- Coordinate validation: drop/flag lat ∉ [−90, 90], lon ∉ [−180, 180].
- De-duplication: within vessel+timestamp buckets (AIS) and rf_id+timestamp (RF).

### 3.2 AIS Cleaning

- Interpolate minor gaps ≤ 5 min if adjacent points are spatially consistent.
- Compute **derived fields:** `delta_t`, `delta_dist_km`, `speed_calc_kn`, `track_id`.

### 3.3 RF Cleaning

- For clustered TDOA/MLAT solutions, select **centroid** and propagate **geo_uncertainty_m**.
- Drop RF points with `geo_uncertainty_m > U_MAX` (default 5,000 m).

## 4) Entity Resolution & Time–Space Indexing

- **Join key:** `mmsi` primarily; fallback to `{imo, call_sign}` if provided in RF metadata or via prior mapping table.
- **Temporal windowing:** index per vessel using 1–5 min buckets (configurable).
- **Spatial indexing:** Geohash(precision=7) or H3(res 8) to accelerate proximity joins.

## 5) Core Algorithms

### 5.1 Algorithm A — AIS Gap Reconciliation via RF

**Goal:** Fill AIS-silent intervals with RF positions and produce a continuous track.

**Steps:**

1. Detect AIS gaps where `delta_t > GAP_THR` (default 10 min).
2. Fetch RF detections within ±6 min of gap window.
3. Score RF candidates using time, spatial, and quality scores.
4. Use Viterbi path selection for smooth trajectory reconstruction.
5. Synthesize new points with interpolated timestamps.
6. Label filled points (`fill_method='RF'`, confidence 0–1).
7. Validate by ensuring speed continuity.

### 5.2 Algorithm B — AIS Spoof Detection & Correction via RF

**Goal:** Identify and correct AIS spoofed positions.

**Signals:**

- Spatial residual between AIS and RF positions.
- Temporal consistency.
- Kinematic plausibility (implied speed/heading).

**Steps:**

1. Compute residual distance `ε`.
2. Flag if `ε > 20 km` and exceeds RF uncertainty.
3. Recalculate speed/heading consistency.
4. Compute composite `spoof_score`.
5. Replace with RF position if score ≥ 0.7.
6. Retain audit fields (`orig_lat`, `orig_lon`, `spoof_score`).

## 6) Data Pipeline Flow

```
[Ingest AIS]   [Ingest RF]
      \          /
       \        /
      [Standardize & Clean]
              |
      [Entity Resolution]
              |
  +-----------+-----------+
  |                       |
[Gap Fill (Alg A)]   [Spoof Detect & Correct (Alg B)]
  |                       |
  +-----------+-----------+
              |
        [Merge + QC]
              |
     [Schema Map to N6]
              |
   [Write parquet + manifest]
              |
         [Publish to N6]
```

## 7) Output Schema (N6-ready)

| Field                    | Type   | Description                       |
| ------------------------ | ------ | --------------------------------- |
| mmsi                     | string | Vessel MMSI                       |
| ts_utc                  | long   | Epoch ms                          |
| lat                      | double | Final latitude                    |
| lon                      | double | Final longitude                   |
| source_tag              | string | `AIS`, `RF_FILL`, `AIS_CORRECTED` |
| fill_confidence         | float  | RF fill confidence                |
| spoof_score             | float  | Spoof likelihood                  |
| rf_ids_used            | array  | RF references                     |
| geo_uncertainty_m      | int    | RF uncertainty                    |
| sog                      | float  | Speed                             |
| cog                      | float  | Course                            |
| correction_distance_km | float  | Distance corrected                |
| position_label          | string | Label of position source          |
| ingest_partition        | date   | Partition key                     |
| lineage_run_id         | string | Run identifier                    |

## 8) Configuration Example

```yaml
time:
  gap_thr_min: 10
  rf_window_min: 6
spatial:
  max_res_km: 20
rf_quality:
  max_geo_uncertainty_m: 5000
scoring:
  spoof_thr: 0.7
io:
  input_ais_uri: s3://bucket/ais/raw/
  input_rf_uri: s3://bucket/rf/raw/
  output_uri: s3://bucket/n6/ais_rf_fused/
```

## 9) KPIs & Acceptance Criteria

- **Gap Coverage Improvement (%):** `(gap_minutes_filled / total_gap_minutes)`
- **Spoof Detection Precision:** ≥70% verified by analyst samples.
- **Schema Compliance:** 100% with N6 ingestion schema.
- **Idempotency:** identical outputs on rerun.

## 10) Risks & Mitigations

- **Sparse RF coverage:** expand time window; lower confidence.
- **Entity mislink:** maintain vessel-ID mapping table.
- **False spoof positives:** conservative thresholds + review queue.

## 11) Notebook/Implementation Steps

1. Config & Imports.
2. Loaders (AIS, RF).
3. Preprocessing & Cleaning.
4. Gap Fill Algorithm.
5. Spoof Detection Algorithm.
6. Merge & QC.
7. Schema Mapping.
8. Write Outputs.
9. KPIs & Audit.

## 12) Expected Deliverables

- **Algorithmic Module 1:** AIS Gap Fill using RF.
- **Algorithmic Module 2:** AIS Spoof Detection & Correction.
- **Output:** Enriched dataset ready for N6 ingestion with metadata and lineage tracking.
