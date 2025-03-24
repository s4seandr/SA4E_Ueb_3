#!/usr/bin/env python3
import subprocess
import time
import json
from rediscluster import RedisCluster

# --- Statische Parameter / Konstanten ---
BASE_PATH = "/home/sebi/SA4E_Ueb_3/CuCuCo"  # Basis-Verzeichnis für Konfigurationsdateien
REDIS_CONFIG_PATH = BASE_PATH  # Falls Konfigurationsdateien hier liegen
REDIS_NODE_1_CONFIG = f"{REDIS_CONFIG_PATH}/redis-node-1.conf"
REDIS_NODE_2_CONFIG = f"{REDIS_CONFIG_PATH}/redis-node-2.conf"
REDIS_NODE_3_CONFIG = f"{REDIS_CONFIG_PATH}/redis-node-3.conf"
NETWORK_NAME = "redis-cluster"  # Name des Docker-Netzwerks

# --- Globale Konfiguration ---
TOKENS_PER_TRACK = 1   # Pro Track wird genau 1 Token gestartet.
MAX_ROUNDS = 3
MONITOR_DURATION = 30  # Dauer der Überwachung in Sekunden.

# --- Funktionen zur Rennverwaltung ---

def load_tracks(file_path):
    """Lädt die Streckenbeschreibung aus einer JSON-Datei."""
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

def get_container_ip(container_name):
    try:
        cmd = f"docker inspect -f '{{{{range .NetworkSettings.Networks}}}}{{{{.IPAddress}}}}{{{{end}}}}' {container_name}"
        ip = subprocess.check_output(cmd, shell=True).decode().strip()
        return ip
    except Exception as e:
        print(f"Fehler beim Abrufen der IP von {container_name}: {e}")
        return None

def reset_redis_cluster():
    """Stoppt und entfernt alle Redis-Container, um das Cluster neu zu erstellen."""
    redis_containers = ["redis-node-1", "redis-node-2", "redis-node-3"]
    for name in redis_containers:
        try:
            subprocess.run(f"docker stop {name}", shell=True, check=True)
            subprocess.run(f"docker rm {name}", shell=True, check=True)
            print(f"Redis-Container {name} wurde gestoppt und entfernt (Reset).")
        except subprocess.CalledProcessError as e:
            print(f"Fehler beim Zurücksetzen von {name}: {e}")
    time.sleep(2)

def start_redis_containers():
    commands = [
        f'docker run --name redis-node-1 --net {NETWORK_NAME} -v {REDIS_NODE_1_CONFIG}:/usr/local/etc/redis/redis.conf -p 7001:7001 -d redis redis-server /usr/local/etc/redis/redis.conf',
        f'docker run --name redis-node-2 --net {NETWORK_NAME} -v {REDIS_NODE_2_CONFIG}:/usr/local/etc/redis/redis.conf -p 7002:7002 -d redis redis-server /usr/local/etc/redis/redis.conf',
        f'docker run --name redis-node-3 --net {NETWORK_NAME} -v {REDIS_NODE_3_CONFIG}:/usr/local/etc/redis/redis.conf -p 7003:7003 -d redis redis-server /usr/local/etc/redis/redis.conf'
    ]
    for cmd in commands:
        try:
            subprocess.run(cmd, shell=True, check=True)
            print(f"Redis-Container gestartet: {cmd}")
        except subprocess.CalledProcessError as e:
            print(f"Befehl schlug fehl (evtl. existieren diese bereits): {cmd}")
    time.sleep(5)

def is_container_running(container_name):
    try:
        cmd = f"docker inspect -f '{{{{.State.Running}}}}' {container_name}"
        output = subprocess.check_output(cmd, shell=True).decode().strip()
        return output.lower() == "true"
    except Exception:
        return False

def check_redis_cluster():
    try:
        cmd = 'docker exec redis-node-1 redis-cli -p 7001 cluster info'
        output = subprocess.check_output(cmd, shell=True).decode().strip()
        if "cluster_state:ok" in output:
            print("Redis-Cluster Status: cluster_state:ok")
            return True
        else:
            print("Cluster Info:", output)
            return False
    except Exception as e:
        print(f"Fehler bei der Cluster-Überprüfung: {e}")
        return False

def create_redis_cluster():
    ip1 = get_container_ip("redis-node-1")
    ip2 = get_container_ip("redis-node-2")
    ip3 = get_container_ip("redis-node-3")
    if not (ip1 and ip2 and ip3):
        print("Nicht alle Container-IP-Adressen abrufbar. Cluster-Erstellung abgebrochen.")
        return False
    create_cmd = f'echo "yes" | docker exec -i redis-node-1 redis-cli -p 7001 --cluster create {ip1}:7001 {ip2}:7002 {ip3}:7003 --cluster-replicas 0'
    try:
        print("Initialisiere Redis-Cluster mit folgendem Befehl:")
        print(create_cmd)
        output = subprocess.check_output(create_cmd, shell=True).decode()
        print(output)
        return True
    except Exception as e:
        print(f"Fehler bei der Cluster-Erstellung: {e}")
        return False

def start_segment_containers(tracks):
    """
    Startet für jedes Segment in allen Tracks einen Docker-Container,
    der das Segment-Programm (Image 'segment') ausführt.
    Vor dem Start werden vorhandene Container mit demselben Namen entfernt.
    """
    container_names = []
    for track in tracks:
        for seg in track.get("segments", []):
            seg_id = seg["segmentId"]
            next_segs = seg["nextSegments"]
            next_arg = ",".join(next_segs)
            container_name = f"seg-{seg_id}"
            try:
                subprocess.run(f"docker rm -f {container_name}", shell=True, check=False)
            except Exception:
                pass
            cmd = f"docker run --name {container_name} --net {NETWORK_NAME} -d segment --segment-id {seg_id} --next {next_arg} --redis-host redis-node-1 --redis-port 7001 --max-rounds {MAX_ROUNDS}"
            try:
                subprocess.run(cmd, shell=True, check=True)
                print(f"Segment-Container gestartet: {container_name}")
                container_names.append(container_name)
            except subprocess.CalledProcessError as e:
                print(f"Fehler beim Starten von {container_name}: {e}")
    return container_names

def stop_containers(container_names):
    for name in container_names:
        try:
            subprocess.run(f"docker stop {name}", shell=True, check=True)
            subprocess.run(f"docker rm {name}", shell=True, check=True)
            print(f"Container {name} wurde gestoppt und entfernt.")
        except subprocess.CalledProcessError as e:
            print(f"Fehler beim Beenden von {name}: {e}")

def start_race(start_segment_id, num_tokens, client):
    for token_id in range(1, num_tokens + 1):
        token = f"token-{start_segment_id.split('-')[-1]}-{token_id}"
        client.xadd(f"stream-{start_segment_id}", {"token": token})
        print(f"Token {token} gestartet in {start_segment_id}.")

def monitor_token_locations(client, duration):
    """Gibt für die angegebene Dauer (in Sekunden) wiederholt den aktuellen Redis-Hash 'token_locations' aus."""
    start_time = time.time()
    print("Überwache die aktuellen Token-Standorte ...")
    while time.time() - start_time < duration:
        try:
            locations = client.hgetall("token_locations")
        except Exception as e:
            locations = {"Error": str(e)}
        print("Aktuelle Token-Standorte:", locations)
        time.sleep(1)

def race_finished(client, total_tokens):
    """
    Prüft, ob der Redis-Key "finished_tokens" mindestens den Wert total_tokens erreicht hat.
    """
    try:
        val = client.get("finished_tokens")
        if val is None:
            return False
        return int(val) >= total_tokens
    except Exception as e:
        print(f"Fehler bei der Überprüfung des Rennstatus: {e}")
        return False

def save_results(client, tracks):
    """
    Liest für jedes Token (basierend auf dem Startsegment) die Redis-Liste 'race_results:<token>',
    summiert die einzelnen Segmentzeiten und schreibt die Ergebnisse (Segmentzeiten und Gesamtzeit)
    in 'race_results.txt'.
    """
    try:
        with open("race_results.txt", "w") as f:
            for track in tracks:
                start_segment = None
                for s in track.get("segments", []):
                    if s.get("type") == "start-goal":
                        start_segment = s.get("segmentId")
                        break
                if start_segment:
                    token_id = start_segment.split('-')[-1]
                    token = f"token-{token_id}-1"
                    results_list = client.lrange(f"race_results:{token}", 0, -1)
                    f.write(f"Token {token}:\n")
                    total_time = 0.0
                    for item in results_list:
                        try:
                            segment, duration = item.split(":")
                            duration = float(duration)
                            total_time += duration
                            f.write(f"  {segment}: {duration:.6f} seconds\n")
                        except Exception as ex:
                            f.write(f"  Fehler beim Parsen von: {item}\n")
                    f.write(f"  Gesamtzeit: {total_time:.6f} seconds\n\n")
        print("Rennergebnisse gespeichert.")
    except Exception as e:
        print(f"Fehler beim Speichern der Ergebnisse: {e}")

def main():
    # Reset: Cluster neu erstellen
    print("Setze bestehenden Redis-Cluster zurück...")
    reset_redis_cluster()
    print("Starte neuen Redis-Cluster...")
    start_redis_containers()
    
    if not create_redis_cluster():
        print("Cluster-Erstellung fehlgeschlagen. Programm wird beendet.")
        return
    time.sleep(5)
    if not check_redis_cluster():
        print("Cluster funktioniert nach Neuerstellung nicht. Abbruch.")
        return

    # Ermittele IP-Adressen und initialisiere den Redis-Cluster-Client.
    ip1 = get_container_ip("redis-node-1")
    ip2 = get_container_ip("redis-node-2")
    ip3 = get_container_ip("redis-node-3")
    startup_nodes = [{"host": ip1, "port": 7001}, {"host": ip2, "port": 7002}, {"host": ip3, "port": 7003}]
    print("Startup-Nodes:", startup_nodes)
    client = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    
    # Setze finished_tokens vor Beginn auf 0.
    client.set("finished_tokens", 0)
    
    # Lade die Streckenbeschreibung aus der JSON-Datei.
    tracks_data = load_tracks("tracks.json")
    tracks = tracks_data.get("tracks", [])
    print("Geladene Streckendaten:", tracks)
    
    # Starte für jedes Segment aller Tracks einen eigenen Segment-Container.
    segment_container_names = start_segment_containers(tracks)
    
    # Pro Track: Bestimme das Startsegment (Typ "start-goal") und starte dort ein Token.
    total_tokens = len(tracks) * TOKENS_PER_TRACK
    for track in tracks:
        start_segment = None
        for s in track.get("segments", []):
            if s.get("type") == "start-goal":
                start_segment = s.get("segmentId")
                break
        if start_segment:
            start_race(start_segment, TOKENS_PER_TRACK, client)
    
    # Überwache für MONITOR_DURATION Sekunden die aktuellen Token-Standorte.
    monitor_token_locations(client, MONITOR_DURATION)
    
    # Nach der Überwachung: Gib den finalen Wert von finished_tokens aus.
    finished = client.get("finished_tokens")
    print(f"Rennstatus final: finished_tokens = {finished} (Erwartet: {total_tokens})")
    
    # Speichere die Rennergebnisse.
    save_results(client, tracks)
    
    # Beende und entferne alle Segment-Container.
    stop_containers(segment_container_names)
    
    # Beende den Redis-Cluster (Reset).
    print("Beende den Redis-Cluster...")
    reset_redis_cluster()

if __name__ == "__main__":
    main()

