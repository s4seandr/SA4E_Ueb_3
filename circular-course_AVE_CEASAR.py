#!/usr/bin/env python3
import sys
import json
import random

def generate_tracks_with_global_caesar_and_bottleneck(num_tracks: int, base_segments: int):
    """
    Erzeugt Rundkurse mit einem **globalen Caesar-Segment** und einem **globalen Bottleneck-Segment**.

    - Ein gemeinsames Caesar-Segment ("segment-global-caesar"), auf das alle Fahrer zielen.
    - Ein gemeinsames Bottleneck-Segment ("segment-global-bottleneck"), auf das alle Fahrer zielen.
    - Rückführungs-Segmente ("segment-t-caesar-ret" und "segment-t-bottleneck-ret") für jeden Track.
    """
    if base_segments < 4:
        sys.exit("Die Basis-Segmentanzahl muss mindestens 4 betragen.")

    CAESAR_INDEX = random.randint(1, base_segments - 2)
    BOTTLENECK_INDEX = random.randint(1, base_segments - 2)
    while BOTTLENECK_INDEX == CAESAR_INDEX:
        BOTTLENECK_INDEX = random.randint(1, base_segments - 2)

    total_segments = base_segments + 2  # Caesar-Rückführung + Bottleneck-Rückführung
    all_tracks = []

    for t in range(1, num_tracks + 1):
        segments = []
        track_id = str(t)
        start_segment_id = f"start-and-goal-{t}"

        # Index 0: Startsegment
        segments.append({
            "segmentId": start_segment_id,
            "type": "start-goal",
            "nextSegments": []  # wird später gesetzt
        })

        for c in range(1, base_segments):
            if c == CAESAR_INDEX:
                seg_id = f"segment-{t}-caesar-link"
                seg_type = "caesar-link"
                next_segments = ["segment-global-caesar"]
            elif c == BOTTLENECK_INDEX:
                seg_id = f"segment-{t}-bottleneck"
                seg_type = "bottleneck"
                # Alle Bottleneck-Segmente verweisen auf das globale Bottleneck-Feld
                next_segments = ["segment-global-bottleneck"]
            else:
                seg_id = f"segment-{t}-{c}"
                seg_type = "normal"
                if c < base_segments - 1:
                    if c + 1 == CAESAR_INDEX:
                        next_segment = f"segment-{t}-caesar-link"
                        next_segments = [next_segment, f"segment-{t}-caesar-ret"]
                    elif c + 1 == BOTTLENECK_INDEX:
                        next_segment = f"segment-{t}-bottleneck"
                        next_segments = [next_segment, "segment-global-bottleneck"]
                    else:
                        next_segment = f"segment-{t}-{c+1}"
                        next_segments = [next_segment]
                else:
                    next_segments = [start_segment_id]
            segments.append({
                "segmentId": seg_id,
                "type": seg_type,
                "nextSegments": next_segments
            })

        # Caesar-Rückführung
        caesar_ret_id = f"segment-{t}-caesar-ret"
        caesar_ret_next = f"segment-{t}-{CAESAR_INDEX+1}" if CAESAR_INDEX < base_segments - 1 else start_segment_id
        caesar_ret_segment = {
            "segmentId": caesar_ret_id,
            "type": "caesar-ret",
            "nextSegments": [caesar_ret_next]
        }

        # Bottleneck-Rückführung
        bottleneck_ret_id = f"segment-{t}-bottleneck-ret"
        bottleneck_ret_next = f"segment-{t}-{BOTTLENECK_INDEX+1}" if BOTTLENECK_INDEX < base_segments - 1 else start_segment_id
        bottleneck_ret_segment = {
            "segmentId": bottleneck_ret_id,
            "type": "bottleneck-ret",
            "nextSegments": [bottleneck_ret_next]
        }

        # Ergänze Rückführungssegmente
        final_segments = []
        for seg in segments:
            final_segments.append(seg)
            if seg["segmentId"] == f"segment-{t}-caesar-link":
                final_segments.append(caesar_ret_segment)
            if seg["segmentId"] == f"segment-{t}-bottleneck":
                final_segments.append(bottleneck_ret_segment)

        # Setze im Startsegment das nextSegment auf das erste normale Segment
        if len(final_segments) > 1:
            final_segments[0]["nextSegments"] = [final_segments[1]["segmentId"]]

        track = {
            "trackId": track_id,
            "segments": final_segments
        }
        all_tracks.append(track)

    # Gemeinsame globale Segmente für Caesar und Bottleneck
    global_caesar = {
        "segmentId": "segment-global-caesar",
        "type": "global-caesar",
        "nextSegments": [f"segment-{t}-caesar-ret" for t in range(1, num_tracks + 1)]
    }

    global_bottleneck = {
        "segmentId": "segment-global-bottleneck",
        "type": "global-bottleneck",
        "nextSegments": [f"segment-{t}-bottleneck-ret" for t in range(1, num_tracks + 1)]
    }

    return {"tracks": all_tracks, "globalSegments": [global_caesar, global_bottleneck]}

def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <num_tracks> <base_segments> <output_file>")
        sys.exit(1)

    num_tracks = int(sys.argv[1])
    base_segments = int(sys.argv[2])
    output_file = sys.argv[3]

    tracks_data = generate_tracks_with_global_caesar_and_bottleneck(num_tracks, base_segments)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(tracks_data, f, indent=2)
        f.write("\n")
    print(f"Successfully generated {num_tracks} track(s) with global Caesar and Bottleneck into '{output_file}'")

if __name__ == "__main__":
    main()

