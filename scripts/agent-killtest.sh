#!/bin/bash
D=/Users/william-work/Documents/GitHub/temporal-agent-demo; NET=eventually-eats_default
RUN="docker run --rm --network $NET -v $D:/work -w /work golang ./bin/agent"
t0=$(date +%s); ts(){ echo "[+$(( $(date +%s) - t0 ))s $(date +%H:%M:%S)] $*"; }
docker rm -f agent-worker >/dev/null 2>&1
docker run -d --name agent-worker --network $NET -v $D:/work -w /work golang ./bin/agent worker >/dev/null
ID=agent-run-$(date +%H%M%S); echo $ID > $D/last-run-id
$RUN start input.json $ID >/dev/null && ts "run started: $ID"
until [ "$($RUN state $ID 2>/dev/null)" = "2" ]; do sleep 3; [ $(( $(date +%s) - t0 )) -gt 900 ] && { ts "TIMEOUT waiting for step 2"; docker logs agent-worker 2>&1 | tail; exit 1; }; done
ts "steps 1-2 done; step 3 is calling the model"
sleep 8
docker kill agent-worker >/dev/null; ts "worker KILLED during step 3"
sleep 10
docker start agent-worker >/dev/null; ts "worker started again"
$RUN result $ID > $D/result.json; ts "run finished"
cat $D/result.json
echo ---- ATTEMPTS
docker compose -f /Users/william-work/Documents/GitHub/eventually-eats/compose.yaml exec -T temporal temporal workflow show -w $ID -o json 2>/dev/null | python3 -I -c "
import json,sys
for e in json.load(sys.stdin)['events']:
  a=e.get('activityTaskStartedEventAttributes')
  if a: print(e['eventTime'][11:19], 'CallModel attempt', a.get('attempt'), (a.get('lastFailure') or {}).get('message',''))
"
echo ---- WORKER LOG; docker logs agent-worker 2>&1 | grep -E 'step|Started Worker' | cut -c1-200
