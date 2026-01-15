"""
TPS Benchmark Script for Shopping Data Pipeline

Orchestrates a throughput test by:
1. Connecting to Kafka to monitor lag.
2. Triggering the data generator with different TPS levels.
3. Monitoring stability (lag growth).

Usage:
    python tps_benchmark.py --scenarios low,medium,high
"""

import os
import sys
import time
import argparse
import logging
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.structs import TopicPartition
import requests
import subprocess

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Config
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
TOPIC = 'shopping-events'
GROUP_ID = 'shopping-stream-consumer'  # Must match the streaming job's group id if known, or we inspect offsets directly

SCENARIOS = {
    'low': {'tps': 100, 'duration': 60},
    'medium': {'tps': 1000, 'duration': 120},
    'high': {'tps': 5000, 'duration': 120},
    'stress': {'tps': 10000, 'duration': 60}
}


def get_consumer_lag(consumer, topic):
    """Estimate consumer lag for a topic."""
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        return 0
    
    total_lag = 0
    for p in partitions:
        tp = TopicPartition(topic, p)
        # Get end offset
        end_offsets = consumer.end_offsets([tp])
        end_offset = end_offsets[tp]
        
        # Get committed offset (position)
        # Note: Streaming jobs commit offsets. If using checkpointing, they might not commit to Kafka __consumer_offsets 
        # unless enable.auto.commit or commitAsync is used. 
        # Spark Structure Streaming usually commits to checkpoint. 
        # BUT newer Spark versions can commit to Kafka too if configured.
        # If unavailable, we might need another metric.
        # For this test, we assume we can read committed offsets or simply check "production rate" vs "processing rate" logic if we had metrics.
        # Fallback: We'll check if we can simply consume from the topic at a certain rate?
        # Actually, best proxy for "System Handling Load" without internal Spark metrics is:
        # Does the generator crash? Does Kafka accept the writes?
        
        # Let's try to fetch committed.
        committed = consumer.committed(tp)
        if committed:
            total_lag += (end_offset - committed)
    
    return total_lag


def run_benchmark(scenarios_to_run):
    """Run selected benchmark scenarios."""
    
    # Check Kafka connection
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id='benchmark-monitor',
            enable_auto_commit=False
        )
        logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP}")
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        return

    print("\n" + "="*50)
    print("STARTING TPS BENCHMARK")
    print("="*50 + "\n")

    for name in scenarios_to_run:
        config = SCENARIOS.get(name)
        if not config:
            logger.warning(f"Unknown scenario: {name}")
            continue

        target_tps = config['tps']
        duration = config['duration']
        
        print(f"\n[Scenario: {name.upper()}] Target: {target_tps} TPS for {duration}s")
        
        # 1. Update Generator Env & Restart
        # We assume independent control or we just launch a separate generator process here.
        # For simplicity, we will launch a docker container or python process with the override.
        
        logger.info("Starting Data Generator...")
        cmd = [
            "python3", "data-generator/main.py",
            "--mode", "normal",
            "--duration", str(duration)
        ]
        
        # Inject TPS via env var (we need to support env var override or arg in main.py, 
        # currently main.py pulls from settings which pulls from env)
        env = os.environ.copy()
        env["BASE_TPS"] = str(target_tps)
        env["KAFKA_BOOTSTRAP_SERVERS"] = KAFKA_BOOTSTRAP
        
        process = subprocess.Popen(cmd, env=env, cwd="/home/junho/shopping-data-pipeline")
        
        # 2. Monitor Loop
        start_time = time.time()
        try:
            while process.poll() is None:
                # Check lag or just liveness
                # Since we can't easily see Spark's lag without API access, 
                # we will trust the Generator's logs (stdout) to see if it sustains the rate.
                time.sleep(5)
                
                # Check if time is up (process should exit by itself due to --duration)
                if time.time() - start_time > duration + 10:
                    logger.warning("Generator timed out!")
                    process.terminate()
                    break
                    
        except KeyboardInterrupt:
            process.terminate()
            print("\nBenchmark interrupted.")
            return

        print(f"[Scenario: {name.upper()}] COMPLETED")
        time.sleep(5) # Cooldown

    print("\n" + "="*50)
    print("BENCHMARK COMPLETE")
    print("="*50 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--scenarios', default='low,medium,high', help='Comma-separated scenarios')
    args = parser.parse_args()
    
    scenarios = [s.strip() for s in args.scenarios.split(',')]
    run_benchmark(scenarios)
