#!/bin/bash

set -e

echo "Starting container with ROLE=${ROLE}"

if [ "$ROLE" != "master" ]; then
    echo "Worker node detected. Generating test data..."
    
    RECORDS=335544
    
    NUM_DIRS=${NUM_DIRS:-4} # n of input directory = 4
    FILES_PER_DIR=${FILES_PER_DIR:-4}  # n of input files per directory = 4
    
    echo "Creating ${NUM_DIRS} directories with ${FILES_PER_DIR} files each (32MB per file)"
    
    for dir_num in $(seq 1 $NUM_DIRS); do
        DIR="/data${dir_num}/input"
        mkdir -p "$DIR"
        echo "Creating files in ${DIR}..."
        
        for file_num in $(seq 1 $FILES_PER_DIR); do
            OUTPUT_FILE="${DIR}/partition.${file_num}"
            HEX_STRING=$(head -c 20 /dev/urandom | od -An -tx1 | tr -d '[:space:]' | tr '[:lower:]' '[:upper:]')
            RAND_INT=$(echo "ibase=16;$HEX_STRING" | bc)
            echo "  Generating ${OUTPUT_FILE} (${RECORDS} records, ~32MB)..."
            gensort -a -b${RAND_INT} ${RECORDS} "$OUTPUT_FILE"
        done
    done
    
    echo "Data generation complete!"
    echo "Total: $((NUM_DIRS * FILES_PER_DIR)) files, $((NUM_DIRS * FILES_PER_DIR * 32))MB of data"
fi

# Execute the CMD (or command from docker-compose)
exec "$@"
