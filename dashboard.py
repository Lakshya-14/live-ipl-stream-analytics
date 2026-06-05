"""
IPL Live Match Center Dashboard
Framework: Streamlit
Architecture: Real-time file-polling consumer interface.
Pulls structured match states from Kafka-processed JSON sinks.
"""

import streamlit as st
import pandas as pd
import json
import time

# --- DASHBOARD LAYOUT CONFIGURATION --- #
st.set_page_config(
    page_title="Live IPL Analytics | Match Center", 
    layout="wide",
    initial_sidebar_state="collapsed"
    )
st.title("🏏 Live IPL Match Center")

#  Create an empty container to handle full-frame UI repaints dynamically
placeholder = st.empty()

# --- REAL-TIME VISUALIZATION POLLING LOOP ---
while True:
    try:
        # Load the latest state snapshot compiled by the Kafka consumer pipeline
        with open("live_stats.json", "r") as f:
            state = json.load(f)
        
        # Extract individual state components from the structured payload
        batsmen_data = state.get("batsmen", {})
        active_batsmen = state.get("active_batsmen", [])
        team_data = state.get("teams", {})
        currrent_batting_team = state.get("current_batting_team", "")

        with placeholder.container():
            # 1. LIVE SCORECARD BANNER
            st.subheader("🏟️ Match Scorecard")
            cols = st.columns(len(team_data) if team_data else 1)

            for i, (team_name, team_metrics) in enumerate(team_data.items()):
                with cols[i]:
                    # Visual indicator for the team currently taking strike
                    batting_marker = "🏏 (Batting)" if team_name == currrent_batting_team else ""
                    st.metric(
                        label = f"{team_name} {batting_marker}",
                        value=f"{team_metrics['runs']}/{team_metrics['wickets']}",
                        delta=f"{team_metrics['overs']} Overs",
                        delta_color="off"
                    )

            st.markdown("---")

            # 2. BATTING PERFORMANCE METRICS
            if batsmen_data:
                # Process raw JSON mapping into a structured Pandas DataFrame
                df_all = pd.DataFrame.from_dict(batsmen_data, orient='index')
                df_all = df_all.reset_index().rename(columns={"index": "Batsman"})

                # Compute Strike Rate (SR)
                df_all['SR'] = (df_all['runs'] / df_all['balls'] * 100).fillna(0).round(2)

                # Prune and normalize tabular presentation layers
                df_all = df_all[['Batsman', 'team', 'runs', 'balls', 'SR']]
                df_all.columns = ['Batsman', 'Team', 'Runs', 'Balls Faced', 'Strike Rate']

                # Create split-screen container columns
                col_table, col_chart = st.columns([1.2, 1])

                with col_table:
                    st.subheader("📋 Innings Leaderboard")
                    st.dataframe(
                        df_all.style.highlight_max(axis = 0, subset=['Strike Rate']), 
                        width='stretch'
                    )

                with col_chart:
                    st.subheader("🔥 Live On-Crease Striking")
                    # Filter chart to isolate only the current batting partnership
                    df_active = df_all[df_all['Batsman'].isin(active_batsmen)]
                    
                    if not df_active.empty:
                        chart_data = df_active.set_index('Batsman')[['Strike Rate']]
                        st.bar_chart(chart_data, width='stretch')
                    else:
                        st.info("🔄 Re-calculating live striking metrics for new partnership...")
            else:
                st.info("⏳ Awaiting toss and opening delivery to populate scorecard data.")
    
    except FileNotFoundError:
        st.warning("⚠️ Telemetry Offline: Waiting for Kafka Stream Pipeline to establish connection sink...")

    except json.JSONDecodeError:
        # Handles the exact millisecond a file write overlaps with a file read operation
        pass

    except Exception as e:
        st.error(f"🚨 Critical System Error loading visualization components: {str(e)}")

    # Polling frequency control to optimize CPU utilisation
    time.sleep(1)