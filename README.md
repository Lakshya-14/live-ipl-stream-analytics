A high-performance, real-time sports analytics pipeline built with **Python**, **Apache Kafka**, and **Streamlit**. This engine simulates a ball-by-ball live stream from historical IPL data, processes cumulative stats downstream, and serves an interactive dashboard.

## 🏗️ Architecture
* **`match_producer.py`**: Simulates live ball deliveries, dynamically handles schema variances, and publishes telemetry packets to Kafka.
* **`analytics_consumer.py`**: Consumes raw match streams, manages state aggregation for batsman partnerships and team scorecards, and writes state atomically.
* **`dashboard.py`**: A low-latency Streamlit visualization layer that polls the state cache to display live scorecards and striking charts.

## 🚀 How to Run Locally

1. **Start Kafka**: Ensure your local Kafka broker is running on `localhost:9092`.
2. **Setup Data**: Download your IPL dataset and save it in the root folder as `match_data.csv`.
3. **Install Dependencies**:
```bash
   pip install -r requirements.txt

