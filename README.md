# Ride Sharing Analytics Using Spark Streaming and Spark SQL.
---
## **Prerequisites**
Before starting the assignment, ensure you have the following software installed and properly configured on your machine:
1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Faker**:
   - Install using `pip`:
     ```bash
     pip install faker
     ```

---

## **Setup Instructions**

### **1. Project Structure**

The project directory should have the following structure follows:

```
ride-sharing-analytics/
├── outputs/
│ ├── task1/ → Parsed ride data (CSV)
│ ├── task2/ → Driver-level aggregations
│ └── task3/ → Windowed time analytics
│
├── checkpoints/
│ ├── task1/ → Spark streaming checkpoints
│ ├── task2/
│ └── task3/
│
├── task1.py # Ingestion and parsing
├── task2.py # Driver-level aggregations
├── task3.py # Time-based window analytics
├── data_generator.py # JSON data stream generator
└── README.md

```

- **data_generator.py/**: generates a constant stream of input data of the schema (trip_id, driver_id, distance_km, fare_amount, timestamp)  
- **outputs/**: CSV files of processed data of each task stored in respective folders.
- **README.md**: Assignment instructions and guidelines.
  
---

## ▶️ Running the Analysis Tasks

Open two terminals. The **data generator** must be running in one terminal while the tasks execute in another.

### 1️⃣ Start the Data Generator (Terminal 1)
```bash
python data_generator.py
```

## 2️⃣ Run the Tasks (Terminal 2)

Execute one task at a time in a **new terminal window** while keeping the data generator running.


# Task 1: Ingestion & Parsing → CSV (real-time rows)
```bash
python task1.py
```

# Task 2: Driver-Level Aggregations → CSV (per micro-batch)
```bash
python task2.py
```

# Task 3: Time-Based Windowed Trends → CSV 
```bash
python task3.py
```
## **Verify the Outputs**:
   Check the `outputs/` directory for the resulting files:
   ```bash
   ls outputs/
   ```

---

## 🧠 Overview

This project demonstrates how to build a **real-time analytics pipeline** for a ride-sharing platform using **Apache Spark Structured Streaming**.  
It continuously processes live ride data, performs real-time aggregations, and analyzes fare trends over time.

---

## 🎯 Objectives

By completing this assignment, you will be able to:

- **Task 1:** Ingest and parse real-time ride data.  
- **Task 2:** Perform real-time aggregations on driver earnings and trip distances.  
- **Task 3:** Analyze trends over time using a sliding time window.

---
## 🧩 Task 1 — Basic Streaming Ingestion and Parsing

**Goal:**  
Ingest streaming data from `localhost:9999`, parse incoming JSON messages, and store structured results in CSV format.

**Implementation Notes:**
- Create a Spark session.  
- Use:
  ```python
  spark.readStream.format("socket").option("host", "localhost").option("port", 9999)
### 🧩 Parsing and Output — Task 1

**Parse the JSON payload into columns using:**
```python
from_json(col("value"), schema)
```
### Write the parsed data to CSV files:**
```bash
outputs/task1/
```
### Checkpoint location:
```bash
checkpoints/task1/
```
### 📄 Sample Output:
# Sample Output (Task 1)
```bash
trip_id,driver_id,distance_km,fare_amount,timestamp
ac6a3544-be6b-4eeb-b06f-b8c79a9e3460,97,29.37,104.72,2025-10-14 21:29:51
```
---
## 🧩 Task 2 — Real-Time Aggregations (Driver-Level)

**Goal:**  
Aggregate ride data in real time to compute each driver’s total fare and average trip distance.

**Implementation Notes:**
- Reuse the parsed DataFrame from Task 1.  
- Group by `driver_id` and compute:
  ```python
  SUM(fare_amount).alias("total_fare")
  AVG(distance_km).alias("avg_distance")

### 🧮 Aggregation Logic

Use the following aggregation logic inside your streaming query:

```python
data.groupBy("driver_id") \
    .agg(
        sum("fare_amount").alias("total_fare"),
        avg("distance_km").alias("avg_distance")
    )
```
### 🧩 Output — Task 2

**Write aggregated results to CSV files:**
```bash
outputs/task2/
```

**Checkpoint location:**
```bash
checkpoints/task2/
```

### 📄 Sample Output

# Sample Output (Task 2)
```bash
driver_id,total_fare,avg_distance
51,14.23,33.17
42,129.56,5.91
73,103.67,38.90
98,51.53,47.05
17,91.90,39.75
```
---
## 🧩 Task 3 — Windowed Time-Based Analytics

**Goal:**  
Perform a **5-minute windowed aggregation** on `fare_amount`, sliding by **1 minute** and watermarking by **1 minute**, to analyze real-time fare trends.

---

### 🛠 Implementation Notes

- Convert the timestamp column to a proper `TimestampType`:
  ```python
  data.withColumn("event_time", col("timestamp").cast(TimestampType()))
  ```

  ### Apply Watermark and Perform Windowed Aggregation

**Apply a watermark to handle late data:**
```python
data_with_watermark = data.withWatermark("event_time", "1 minute")
```

### 🧮 Perform the Windowed Aggregation

**Perform the windowed aggregation:**
```python
data_with_watermark.groupBy(
    window(col("event_time"), "5 minutes", "1 minute")
).agg(
    sum("fare_amount").alias("total_fare")
)
```
### 🧩 Output — Task 3

**Write windowed aggregation results to CSV files:**
```bash
outputs/task3/
```

**Checkpoint location:**
```bash
checkpoints/task3/
```
### 📄 Sample Output

# Sample Output (Task 3)
```bash
window_start,window_end,total_fare
2025-10-14T22:24:00.000Z,2025-10-14T22:29:00.000Z,2787.4
2025-10-14T22:25:00.000Z,2025-10-14T22:30:00.000Z,2841.6
2025-10-14T22:26:00.000Z,2025-10-14T22:31:00.000Z,2920.3
```

## 📬 Submission Checklist

- [ ] Python scripts 
- [ ] Output files in the `outputs/` directory  
- [ ] Completed `README.md`  
- [ ] Commit everything to GitHub Classroom  
- [ ] Submit your GitHub repo link on canvas

---

