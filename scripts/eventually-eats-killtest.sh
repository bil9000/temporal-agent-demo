#!/bin/bash
cd /Users/william-work/Documents/GitHub/eventually-eats
t0=$(date +%s); ts(){ echo "[+$(( $(date +%s) - t0 ))s $(date +%H:%M:%S)] $*"; }
ID=$(curl -s -m 20 -X POST http://127.0.0.1:9999/api/order -H 'content-type: application/json' \
  -d '{"collection":true,"email":"test@example.com","products":[{"productId":1,"quantity":2}]}' | sed -E 's/.*"orderId":"([^"]+)".*/\1/')
ts "order placed: $ID (TakePayment runs 5s)"
echo "$ID" > /Users/william-work/Documents/GitHub/temporal-agent-demo/eventually-eats-last-order-id
sleep 2
docker compose kill workflow >/dev/null 2>&1; ts "worker KILLED mid-payment"
sleep 10
ts "worker container: $(docker compose ps -a --format '{{.Service}} {{.State}}' | grep workflow)"
docker compose start workflow >/dev/null 2>&1; ts "worker container started again (rebuilding)"
until docker compose logs workflow --since 2m 2>&1 | grep -q 'Started Worker'; do sleep 2; done; ts "worker back up"
until docker compose exec -T temporal temporal workflow query -w "$ID" --type GET_STATUS 2>/dev/null | grep -q PENDING; do sleep 2; [ $(( $(date +%s) - t0 )) -gt 240 ] && { ts "gave up waiting for PENDING"; break; }; done
ts "status now: $(docker compose exec -T temporal temporal workflow query -w "$ID" --type GET_STATUS 2>/dev/null | grep -o '"status":"[A-Z]*"')"
docker compose exec -T temporal temporal workflow update execute -w "$ID" --name UPDATE_STATUS --input '"COMPLETED"' >/dev/null 2>&1; ts "restaurant sent COMPLETED"
sleep 4
ts "workflow: $(docker compose exec -T temporal temporal workflow describe -w "$ID" 2>/dev/null | grep -m1 -i 'status')"
echo ---- HISTORY; docker compose exec -T temporal temporal workflow show -w "$ID" 2>&1 | cut -c1-160
