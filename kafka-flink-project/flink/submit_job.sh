#!/bin/bash
# Script to submit the Flink job to the cluster

FLINK_JOBMANAGER="localhost:8081"

echo "Submitting Flink job..."
docker-compose exec jobmanager flink run \
    -m $FLINK_JOBMANAGER \
    -py /opt/flink/usrlib/process_orders_table.py \
    -d

echo ""
echo "Job submitted. Check status at http://localhost:8081"
