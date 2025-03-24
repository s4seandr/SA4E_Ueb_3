#!/usr/bin/env python3
import argparse
import time
import random
from rediscluster import RedisCluster

def process_segment(segment_id, next_segments, redis_host="redis", redis_port=6379, max_rounds=3):
    # Erstelle einen cluster-fähigen Redis-Client.
    startup_nodes = [{"host": redis_host, "port": redis_port}]
    client = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    
    stream_name = f"stream-{segment_id}"
    rounds_hash = "token_rounds"
    start_times_hash = "token_start_times"
    
    print(f"Segment {segment_id} gestartet (Redis Cluster: {redis_host}:{redis_port})...")
    
    while True:
        # Lese neue Nachrichten aus dem eigenen Stream.
        messages = client.xread({stream_name: "$"}, block=0)
        for _, entries in messages:
            for entry_id, entry_data in entries:
                token = entry_data.get("token")
                print(f"[{segment_id}] Token {token} empfangen (ID: {entry_id}).")
                
                # Setze den aktuellen Standort im Hash "token_locations".
                client.hset("token_locations", token, segment_id)
                print(f"[{segment_id}] Token {token} Standort gesetzt auf {segment_id}.")
                
                # Startzeit und Rundenzähler: Für Tokens im Startsegment.
                if segment_id.startswith("start-and-goal"):
                    if client.hget(start_times_hash, token) is None:
                        client.hset(start_times_hash, token, time.time())
                    current = client.hget(rounds_hash, token)
                    if current is None:
                        current = 1
                    else:
                        current = int(current) + 1
                    client.hset(rounds_hash, token, current)
                    print(f"[{segment_id}] Token {token} Runde: {current}")
                    if current > max_rounds:
                        finish_time = time.time()
                        start_time = client.hget(start_times_hash, token)
                        runtime = finish_time - float(start_time) if start_time else 0
                        print(f"[{segment_id}] Token {token} hat das Rennen beendet. Gesamtzeit: {runtime:.2f} Sekunden")
                        # Speichere das Gesamtlaufzeit-Ergebnis in einem separaten Hash (optional).
                        client.hset("race_results", token, runtime)
                        client.incr("finished_tokens")
                        # Lösche die Nachricht, damit sie nicht erneut verarbeitet wird.
                        client.xdel(stream_name, entry_id)
                        continue
                
                # Simuliere die Bearbeitungszeit im Segment (zufälliges Delay).
                delay = random.uniform(0.5, 2.0)
                print(f"[{segment_id}] Bearbeitung für {delay:.2f} Sekunden...")
                seg_start = time.time()
                time.sleep(delay)
                seg_duration = time.time() - seg_start
                # Pro Segment wird die verstrichene Zeit in einer Liste protokolliert.
                client.rpush(f"race_results:{token}", f"{segment_id}:{seg_duration}")
                print(f"[{segment_id}] Token {token} verbrachte {seg_duration:.2f} Sekunden in diesem Segment.")
                
                # Leite das Token an alle folgenden Segmente weiter.
                for nxt in next_segments:
                    next_lock = f"lock:{nxt}"
                    while client.get(next_lock) is not None:
                        time.sleep(0.1)
                    client.xadd(f"stream-{nxt}", {"token": token})
                    print(f"[{segment_id}] Token {token} weitergeleitet an {nxt}.")
                
                # Lösche diese Nachricht aus dem Stream, damit sie nicht erneut verarbeitet wird.
                client.xdel(stream_name, entry_id)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Segment-Prozess, der Tokens weiterleitet, den aktuellen Standort speichert und die Bearbeitungszeit aufzeichnet."
    )
    parser.add_argument("--segment-id", required=True, help="Die ID dieses Segments (z.B. 'start-and-goal-1').")
    parser.add_argument("--next", required=True, help="Kommagetrennte Liste der nächsten Segmente (z.B. 'segment-1-1').")
    parser.add_argument("--redis-host", default="redis", help="Hostname des Redis-Clusters (Standard: 'redis').")
    parser.add_argument("--redis-port", type=int, default=6379, help="Port des Redis-Clusters (Standard: 6379).")
    parser.add_argument("--max-rounds", type=int, default=3, help="Maximale Runden, bevor ein Token als fertig gilt (Standard: 3).")
    args = parser.parse_args()
    
    next_segments = [s.strip() for s in args.next.split(",") if s.strip()]
    process_segment(args.segment_id, next_segments, args.redis_host, args.redis_port, args.max_rounds)

