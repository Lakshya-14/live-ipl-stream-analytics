"""
IPL Stream Processing Engine & Analytics Consumer
Framework: Native Kafka Consumer Engine
Architecture: State-tracking aggregator for live batsman and team statistics.
Compiles event streams into an atomic JSON state cache for dashboard integration
"""

import os
import json
from kafka import KafkaConsumer
from collections import defaultdict

# --- ENGINE CONFIGURATION SINK ---
KAFKA_TOPIC = 'ipl-match-stream'
BOOTSTRAP_SERVERS = ['localhost:9092']
CONSUMER_GROUP_ID = 'ipl-local-v1'
STATE_OUTPUT_FILE = 'live_stats.json'

def run():
    """
    Initializes the Kafka consumption loop, tracks real-time batsman partnerships,
    aggregates team scorecards, and dumps the state into an atomic file cache.
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',   
        group_id=CONSUMER_GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # --- IN-MEMORY REAL-TIME STATE ENGINES ---
    # Tracks granular stats per player
    batsman_metrics = defaultdict(lambda: {"runs": 0, "balls": 0, "team": "Unknown"})

    # Tracks cumulative team scores and overs 
    team_metrics = defaultdict(lambda: {"runs": 0, "wickets": 0, "overs": "0.0"})

    # Recency slider to isolate the 2 active on-crease batsmen
    active_partnership = []

    print(f"🚀 Streaming Engine Online. Listening to topic '{KAFKA_TOPIC}'...\n")

    for msg in consumer:
        try:
            data = msg.value

            # Payload Variable Extraction
            batsman = data['batsman']
            runs_off_bat = int(data.get('runs', 0) or 0)
            is_valid_ball = data.get('count_as_ball', True)

            total_ball_runs = int(data.get('total_runs', 0) or 0)
            is_wicket = data.get('is_wicket', False)
            team_name = data.get('batting_team', 'Unknown Team')
            over_num = str(data.get('over', '0'))
            ball_num = int(data.get('ball', 1))

            # 1. METRIC AGGREGATION: PLAYER PROFILE
            batsman_metrics[batsman]["runs"] += runs_off_bat
            batsman_metrics[batsman]["team"] = team_name
            if is_valid_ball:
                batsman_metrics[batsman]["balls"] += 1
        
            # 2. METRIC AGGREGATION: PARTNERSHIP TRACKING
            if batsman in active_partnership:
                active_partnership.remove(batsman)
            active_partnership.insert(0, batsman)
            active_partnership = active_partnership[:2]

            # 3. METRIC AGGREGATION: TEAM SCORECARD
            team_metrics[team_name]["runs"] += total_ball_runs
            if is_wicket:
                team_metrics[team_name]["wickets"] += 1
            team_metrics[team_name]["overs"] = f"{over_num}.{ball_num}" 

            # 4. STREAM STATUS TELEMETRY LOG
            balls_faced = batsman_metrics[batsman]["balls"]
            cumulative_runs = batsman_metrics[batsman]["runs"]
            strike_rate = (cumulative_runs / balls_faced) * 100 if balls_faced > 0 else 0

            print(
                f"KAFKA LIVE >> {batsman:<15} ({team_name}) | "
                f"Runs: {cumulative_runs:>3} | Balls: {balls_faced:>3} | SR: {strike_rate:.2f} | "
                f"Score: {team_metrics[team_name]['runs']}/{team_metrics[team_name]['wickets']} ({team_metrics[team_name]['overs']} ov)"
            )

            # 5. STRUCTURED STATE COMPILATION
            dashboard_state = {
                "batsmen": dict(batsman_metrics),
                "active_batsmen": active_partnership,
                "teams": dict(team_metrics),
                "current_batting_team": team_name
            }

            # 6. ATOMIC WRITE PATTERN: Prevents reading race conditions in UI thread
            temp_state_file = f"{STATE_OUTPUT_FILE}.tmp"
            with open(temp_state_file, "w") as f:
                json.dump(dashboard_state, f)

            # Atomic swap completely replaces the old file in a single OS clock cycle
            os.replace(temp_state_file, STATE_OUTPUT_FILE)
    
        except KeyError as ke:
            print(f"⚠️ Warning: Poison-pill packet skipped. Missing essential payload key: {str(ke)}")
        except Exception as e:
            print(f"⚠️ Warning: Error processing stream packet, skipping block. Details: {str(e)}")
    

if __name__ == "__main__":
    run()
