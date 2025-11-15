#!/bin/bash

set -euo pipefail

echo "Starting container with ROLE=${ROLE:-unknown}"

export SYNC_INPUT_ROOT="${SYNC_INPUT_ROOT:-/input}"
export SYNC_LABELED_ROOT="${SYNC_LABELED_ROOT:-/labeled}"

INPUT_ROOT="$SYNC_INPUT_ROOT"
LABELED_ROOT="$SYNC_LABELED_ROOT"

mkdir -p "$INPUT_ROOT" "$LABELED_ROOT"

create_small_input() {
    mkdir -p "$INPUT_ROOT"

    local sample_file="${INPUT_ROOT}/partition.0"
    if [ ! -f "$sample_file" ]; then
        echo "[entrypoint] Creating sample input file at ${sample_file}"
        dd if=/dev/urandom of="$sample_file" bs=100 count=200 status=none
    else
        echo "[entrypoint] Reusing existing sample input file at ${sample_file}"
    fi
}

create_virtual_labeled_files() {
    mkdir -p "$LABELED_ROOT"

    local self_ip
    self_ip=$(hostname -I | awk '{print $1}')
    local peers_csv="${SYNC_WORKER_IPS:-2.2.2.101,2.2.2.102,2.2.2.103}"
    local counter=1
    local created=0
    local files_per_peer=4

    IFS=',' read -ra peers <<< "$peers_csv"
    for peer in "${peers[@]}"; do
        if [ "$peer" = "$self_ip" ] || [ -z "$peer" ]; then
            continue
        fi

        for seq in $(seq 1 $files_per_peer); do
            local virtual_file="${LABELED_ROOT}/${self_ip}_${peer}_${counter}"
            echo "[entrypoint] Synthesizing labeled file ${virtual_file}"
            echo "${self_ip} -> ${peer}, sample payload ${counter}.${seq}" > "$virtual_file"
            counter=$((counter + 1))
            created=$((created + 1))
        done
    done

    if [ "$created" -eq 0 ]; then
        cat <<EOF
[entrypoint] Warning: no peers detected to create labeled files.
Set SYNC_WORKER_IPS to a comma-separated list of worker IPs (excluding master).
EOF
    else
        echo "[entrypoint] Created ${created} labeled files for synchronization smoke test."
        echo "[entrypoint] Labeled directory: ${LABELED_ROOT}"
    fi
}

print_usage_hint() {
    cat <<EOF

[entrypoint] Smoke-test directories ready:
  INPUT   = ${INPUT_ROOT}
  LABELED = ${LABELED_ROOT}   # sync phase reads from here automatically
Invoke the worker (final output dir is arbitrary, labeled dir is hard-coded):
  sbt worker/run -- <master ip:port> -I ${INPUT_ROOT} -O /final-output
EOF
}

if [ "${ROLE:-worker}" = "master" ]; then
    echo "[entrypoint] Master node detected. Generating shared smoke-test fixtures for inspection..."
else
    echo "[entrypoint] Worker node detected. Preparing synchronization smoke-test fixtures..."
fi

create_small_input
create_virtual_labeled_files
print_usage_hint

# Execute the CMD (or command from docker-compose)
exec "$@"
