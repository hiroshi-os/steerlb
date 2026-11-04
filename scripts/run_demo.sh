#!/usr/bin/env bash
set -e

cleanup() {
  echo "Stopping all demo processes..."
  kill $(jobs -p) 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "Starting 5 chaos backends..."
./bin/chaosbackend -addr :9001 -id backend-1 &
./bin/chaosbackend -addr :9002 -id backend-2 &
./bin/chaosbackend -addr :9003 -id backend-3 &
./bin/chaosbackend -addr :9004 -id backend-4 &
./bin/chaosbackend -addr :9005 -id backend-5 &

sleep 1

echo "Starting steerlb proxy on :8080 (admin :9901)..."
./bin/steerlb -config steerlb.yaml &

sleep 2

echo "==> Sending curl to steerlb (:8080):"
curl -s http://127.0.0.1:8080/
echo ""
curl -s http://127.0.0.1:8080/
echo ""

echo "==> Querying /admin/endpoints (:9901):"
curl -s http://127.0.0.1:9901/admin/endpoints
echo ""

echo "==> Injecting fault on backend-1 (:9001):"
curl -s -X POST http://127.0.0.1:9001/chaos -d '{"err_rate": 0.5}'
echo ""

echo "==> Sending requests through steerlb under fault:"
for i in {1..5}; do
  curl -s http://127.0.0.1:8080/
  echo ""
done

echo "Demo completed successfully. Exiting..."
