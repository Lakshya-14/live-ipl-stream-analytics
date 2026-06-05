"""
IPL Live Event Stream Producer
Framework: Native Kafka Producer Engine
Architecture: High-fidelity historical CSV event stream simulator
Parses, sanitizes, and streams real-time ball-by-ball match metrics to Kafka brokers.
"""

import csv
import time
import json
from kafka import KafkaProducer

# --- INGESTION CONFIGURATION SINK ---
KAFKA_TOPIC = 'ipl-match-stream'
BOOTSTRAP_SERVERS = ['localhost:9092']
DEFAULT_DATA_PATH = 'match_data.csv'
STREAM_DELAY_SECONDS = 5.0

def stream_match(file_path = DEFAULT_DATA_PATH):
  """
  Parses historical match data line-by-line, tracks real-time over
  state transitions, and publishes telemetry packets to the messaging cluster.
  """
  print(f"🔌 Initializing Kafka Producer target broker: {BOOTSTRAP_SERVERS}...")
  producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
  )

  try:
    with open(file_path, mode='r') as f:
      reader = csv.DictReader(f)   

      # --- ROBUST COMPLIANT SCHEMA MAPPING ---
      H_OVER = next((col for col in reader.fieldnames if col.endswith('overs__over')), 'innings__overs__over')
      H_BATSMAN = next((col for col in reader.fieldnames if col.endswith('deliveries__batter')), 'innings__overs_deliveries_batter')
      H_RUNS = next((col for col in reader.fieldnames if col.endswith('runs__batter')), 'innings__overs_deliveries_runs_batter')
      H_WIDES = next((col for col in reader.fieldnames if col.endswith('extras__wides')), 'innings__overs__deliveries__extras__wides')
      H_TEAM = next((col for col in reader.fieldnames if 'innings__team' in col or col.endswith('innings__team')), 'innings__team')
      H_TOTAL_RUNS = next((col for col in reader.fieldnames if col.endswith('runs__total')), 'innings__overs__deliveries__runs__total')
      H_WICKET_PLAYER = next((col for col in reader.fieldnames if 'wicket' in col and 'player_out' in col), None)

      # --- OPERATIONAL STATE TRACKERS ---
      current_over = "0"
      balls_completed_in_over = 0
      new_over = "0"
      current_batting_team = None

      print(f"🎬 Ingestion Pipeline Active. Simulating stream from '{file_path}'...\n")

      for row in reader:
        # --- (i) Skip row if batsman tracking cell is empty ---
        batsman = row.get(H_BATSMAN, "").strip()
        if not batsman:
          print("⚠️ Metadata Packet Dropped: Missing explicit batsman identity identifier.")
          continue
          
        # --- (ii) Detect Innings Transitions & Reset Metrics ---
        row_team = row.get(H_TEAM, "").strip()
        if row_team and row_team != current_batting_team:
          current_batting_team = row_team
          current_over = "0"
          new_over = "0"
          balls_completed_in_over = 0
          print(f"\n🌟 INNINGS BREAK SWITCH: '{current_batting_team}' taking strike. Resetting over counters to 0.1\n")
           
        # Evaluate extra metrics
        is_wide = row.get(H_WIDES, "") != "" and int(row.get(H_WIDES, 0)) > 0

        # Synchornize local over transitions
        if new_over != current_over:
          current_over = new_over
          balls_completed_in_over = 0

        display_ball_number = balls_completed_in_over + 1

        if not is_wide:
          balls_completed_in_over+=1

        # Extract batsman performance scoring
        runs_off_bat = int(row.get(H_RUNS, 0) or 0)

        # Differentiate run metrics from extra overheads
        if H_TOTAL_RUNS in row and row.get(H_TOTAL_RUNS):
          total_ball_runs = int(row[H_TOTAL_RUNS])
        else :
          wide_extra = int(row.get(H_WIDES, 0) or 0) if is_wide else 0
          total_ball_runs = runs_off_bat + wide_extra
           
        # Extract out-of-bounds wicket changes
        is_wicket = False
        if H_WICKET_PLAYER and row.get(H_WICKET_PLAYER, "").strip():
          is_wicket = True
           
        count_as_balls_faced = not is_wide

        if(balls_completed_in_over == 6 and not is_wide):
          new_over = int(current_over) + 1

        # Construct stream message structure
        output_payload = {
          "over": current_over,
          "ball": display_ball_number,
          "batsman": batsman,
          "runs": runs_off_bat,
          "total_runs": total_ball_runs,
          "is_wicket": is_wicket,
          "count_as_ball": count_as_balls_faced,
          "batting_team": current_batting_team or "Unknown Team"
         }
           
        print(
          f"PRODUCER EVENT >> Ball: {output_payload['over']}.{output_payload['ball']} | "
               f"Team: {output_payload['batting_team']:<22} | "
               f"{output_payload['batsman']:<15} scored {output_payload['runs']} run(s)"
        )
           
        # Emit structured record downstream
        producer.send(KAFKA_TOPIC, value=output_payload)
        time.sleep(STREAM_DELAY_SECONDS)

  except KeyboardInterrupt:
    print("\n🛑 Graceful execution termination intercepted via operator command.")
  finally:
    # Guarantee memory buffers are flushed to brokers before tearing down connections
    print("🧼 Flushing stream buffers and closing connection sockets...")
    producer.flush()
    producer.close()
    print("🏁 Producer engine offline.")

if __name__ == "__main__":
    stream_match()


