#!/usr/bin/env python3
"""
Mock WebUI cluster test - Makes 1 picodata instance appear as 800 instances
Usage: python test_webui_mock_cluster.py --backend http://localhost:8080 --instances 800 --failure-rate 5
"""

import argparse
import json
import sys
from flask import Flask, request, Response, jsonify
import requests
from typing import Optional

app = Flask(__name__)

# Configuration
BACKEND_URL = "http://localhost:8080"
TOTAL_INSTANCES = 800
FAILURE_RATE = 5  # Every Nth instance will be offline (0 = no failures)

# Base ports for fake instances
BASE_BINARY_PORT = 3301
BASE_PG_PORT = 4327
BASE_HTTP_PORT = 8081

def generate_instance(index: int, replicaset_num: int, instances_in_rs: int, position_in_rs: int) -> dict:
    """Generate a single instance with proper state"""
    instance_num = index + 1

    # Determine if this instance should be offline
    is_offline = FAILURE_RATE > 0 and (instance_num % FAILURE_RATE == 0)

    # Last instance in each replicaset is the leader (if online)
    is_leader = (position_in_rs == instances_in_rs - 1) and not is_offline

    return {
        "httpAddress": f"0.0.0.0:{BASE_HTTP_PORT + index}",
        "version": "25.6.0-125-gc8649d493",
        "failureDomain": {
            "zone": f"zone-{(index % 10) + 1}",  # Distribute across 10 zones
            "rack": f"rack-{(index % 50) + 1}"   # Distribute across 50 racks
        },
        "isLeader": is_leader,
        "currentState": "Offline" if is_offline else "Online",
        "targetState": "Online",
        "name": f"i{instance_num}",
        "binaryAddress": f"127.0.0.1:{BASE_BINARY_PORT + index}",
        "pgAddress": f"127.0.0.1:{BASE_PG_PORT + index}",
        "memory": {
            "usable": 67108864,
            "used": 0 if is_offline else 21474836  # Offline instances show 0 usage
        },
        "capacityUsage": 0.0 if is_offline else 0.32
    }

def generate_replicaset(replicaset_num: int, start_instance_idx: int, instances_per_rs: int) -> dict:
    """Generate a replicaset with instances"""
    instances_in_this_rs = min(instances_per_rs, TOTAL_INSTANCES - start_instance_idx)

    instances = []
    total_usable = 0
    total_used = 0

    for i in range(instances_in_this_rs):
        instance_idx = start_instance_idx + i
        inst = generate_instance(instance_idx, replicaset_num, instances_in_this_rs, i)
        instances.append(inst)

        total_usable += inst["memory"]["usable"]
        total_used += inst["memory"]["used"]

    capacity_usage = (total_used / total_usable) if total_usable > 0 else 0.0

    return {
        "version": "25.6.0-125-gc8649d493",
        "state": "Online",
        "instanceCount": instances_in_this_rs,
        "uuid": f"c43dc4f6-d261-4f3e-9cea-e23f4f89{replicaset_num:04d}",
        "instances": instances,
        "capacityUsage": capacity_usage,
        "memory": {
            "usable": total_usable,
            "used": total_used
        },
        "name": f"default_{replicaset_num + 1}"
    }

def generate_tiers_response(instances_per_rs: int = 3) -> list:
    """Generate the complete /api/v1/tiers response"""
    num_replicasets = (TOTAL_INSTANCES + instances_per_rs - 1) // instances_per_rs

    replicasets = []
    instance_idx = 0

    for rs_num in range(num_replicasets):
        if instance_idx >= TOTAL_INSTANCES:
            break
        replicasets.append(generate_replicaset(rs_num, instance_idx, instances_per_rs))
        instance_idx += instances_per_rs

    # Calculate tier-level aggregates
    total_usable = sum(rs["memory"]["usable"] for rs in replicasets)
    total_used = sum(rs["memory"]["used"] for rs in replicasets)
    capacity_usage = (total_used / total_usable) if total_usable > 0 else 0.0

    tier = {
        "replicasets": replicasets,
        "replicasetCount": len(replicasets),
        "rf": 3,
        "bucketCount": 3000 * len(replicasets),
        "instanceCount": TOTAL_INSTANCES,
        "can_vote": True,
        "name": "default",
        "services": ["test-service"],
        "memory": {
            "usable": total_usable,
            "used": total_used
        },
        "capacityUsage": capacity_usage
    }

    return [tier]

def generate_cluster_response(tiers: list) -> dict:
    """Generate the /api/v1/cluster response from tiers data"""
    tier = tiers[0]

    # Count online/offline instances
    online_count = 0
    offline_count = 0

    for rs in tier["replicasets"]:
        for inst in rs["instances"]:
            if inst["currentState"] == "Online":
                online_count += 1
            else:
                offline_count += 1

    return {
        "capacityUsage": tier["capacityUsage"],
        "clusterName": "mock-cluster-800",
        "clusterVersion": "25.6.0-125-gc8649d493",
        "currentInstaceVersion": "25.6.0-125-gc8649d493",
        "replicasetsCount": tier["replicasetCount"],
        "instancesCurrentStateOnline": online_count,
        "instancesCurrentStateOffline": offline_count,
        "memory": tier["memory"],
        "plugins": ["audit-log 1.0.0", "mock-plugin 2.0.0"]
    }

@app.route('/api/v1/tiers', methods=['GET'])
def mock_tiers():
    """Return mocked tiers data"""
    print(f"[MOCK] Serving /api/v1/tiers with {TOTAL_INSTANCES} instances")

    # Forward auth header if present
    headers = {}
    if 'Authorization' in request.headers:
        headers['Authorization'] = request.headers['Authorization']

    # Check if we need to authenticate against real backend first
    if 'Authorization' in request.headers:
        # Validate token with real backend
        try:
            resp = requests.get(f"{BACKEND_URL}/api/v1/config", headers=headers, timeout=5)
            if resp.status_code == 401:
                return Response("Unauthorized", status=401)
        except Exception as e:
            print(f"[WARN] Could not validate auth with backend: {e}")

    tiers = generate_tiers_response()
    return jsonify(tiers)

@app.route('/api/v1/cluster', methods=['GET'])
def mock_cluster():
    """Return mocked cluster data"""
    print(f"[MOCK] Serving /api/v1/cluster with {TOTAL_INSTANCES} instances")

    # Forward auth header if present
    headers = {}
    if 'Authorization' in request.headers:
        headers['Authorization'] = request.headers['Authorization']

    # Check if we need to authenticate against real backend first
    if 'Authorization' in request.headers:
        try:
            resp = requests.get(f"{BACKEND_URL}/api/v1/config", headers=headers, timeout=5)
            if resp.status_code == 401:
                return Response("Unauthorized", status=401)
        except Exception as e:
            print(f"[WARN] Could not validate auth with backend: {e}")

    tiers = generate_tiers_response()
    cluster = generate_cluster_response(tiers)
    return jsonify(cluster)

@app.route('/api/v1/config', methods=['GET'])
def proxy_config():
    """Proxy config request to real backend"""
    print("[PROXY] Forwarding /api/v1/config to real backend")
    try:
        resp = requests.get(f"{BACKEND_URL}/api/v1/config", timeout=5)
        return Response(resp.content, status=resp.status_code,
                       headers=dict(resp.headers))
    except Exception as e:
        print(f"[ERROR] Failed to proxy config: {e}")
        # Return default config if backend is unavailable
        return jsonify({"isAuthEnabled": False})

@app.route('/api/v1/session', methods=['GET', 'POST'])
def proxy_session():
    """Proxy session (login/refresh) requests to real backend"""
    print(f"[PROXY] Forwarding /api/v1/session {request.method} to real backend")
    try:
        headers = {k: v for k, v in request.headers if k.lower() != 'host'}

        if request.method == 'POST':
            resp = requests.post(
                f"{BACKEND_URL}/api/v1/session",
                json=request.get_json(),
                headers=headers,
                timeout=5
            )
        else:
            resp = requests.get(
                f"{BACKEND_URL}/api/v1/session",
                headers=headers,
                timeout=5
            )

        return Response(resp.content, status=resp.status_code,
                       headers=dict(resp.headers))
    except Exception as e:
        print(f"[ERROR] Failed to proxy session: {e}")
        return Response("Backend unavailable", status=503)

@app.route('/metrics', methods=['GET'])
def proxy_metrics():
    """Proxy metrics request to real backend"""
    print("[PROXY] Forwarding /metrics to real backend")
    try:
        resp = requests.get(f"{BACKEND_URL}/metrics", timeout=5)
        return Response(resp.content, status=resp.status_code,
                       content_type=resp.headers.get('content-type'))
    except Exception as e:
        print(f"[ERROR] Failed to proxy metrics: {e}")
        return Response("# No metrics available\n", status=200,
                       content_type="text/plain")

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def proxy_static(path):
    """Proxy all other requests (static files, etc.) to real backend"""
    url = f"{BACKEND_URL}/{path}"
    if request.query_string:
        url += f"?{request.query_string.decode()}"

    try:
        headers = {k: v for k, v in request.headers if k.lower() != 'host'}
        resp = requests.get(url, headers=headers, timeout=10)

        excluded_headers = ['content-encoding', 'content-length',
                           'transfer-encoding', 'connection']
        response_headers = {k: v for k, v in resp.headers.items()
                           if k.lower() not in excluded_headers}

        return Response(resp.content, status=resp.status_code,
                       headers=response_headers)
    except Exception as e:
        print(f"[ERROR] Failed to proxy {path}: {e}")
        return Response("Not found", status=404)

def main():
    global BACKEND_URL, TOTAL_INSTANCES, FAILURE_RATE

    parser = argparse.ArgumentParser(
        description='Mock WebUI server for testing large clusters',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Mock 800 instances with every 5th instance offline
  %(prog)s --backend http://localhost:8080 --instances 800 --failure-rate 5

  # Mock 1000 instances with all online
  %(prog)s --backend http://localhost:8080 --instances 1000 --failure-rate 0

  # Custom proxy port
  %(prog)s --backend http://localhost:8080 --port 9090 --instances 800
        """
    )

    parser.add_argument('--backend', default='http://localhost:8080',
                       help='Real picodata HTTP URL (default: http://localhost:8080)')
    parser.add_argument('--port', type=int, default=8888,
                       help='Port to run mock server on (default: 8888)')
    parser.add_argument('--instances', type=int, default=800,
                       help='Total number of instances to mock (default: 800)')
    parser.add_argument('--failure-rate', type=int, default=5,
                       help='Make every Nth instance offline (0 = all online, default: 5)')
    parser.add_argument('--host', default='0.0.0.0',
                       help='Host to bind to (default: 0.0.0.0)')

    args = parser.parse_args()

    BACKEND_URL = args.backend.rstrip('/')
    TOTAL_INSTANCES = args.instances
    FAILURE_RATE = args.failure_rate

    print("=" * 70)
    print("Mock WebUI Cluster Test Server")
    print("=" * 70)
    print(f"Backend:           {BACKEND_URL}")
    print(f"Mock server:       http://{args.host}:{args.port}")
    print(f"Total instances:   {TOTAL_INSTANCES}")
    print(f"Failure rate:      Every {FAILURE_RATE} instance(s) offline"
          if FAILURE_RATE > 0 else "All instances online")

    if FAILURE_RATE > 0:
        offline_count = TOTAL_INSTANCES // FAILURE_RATE
        online_count = TOTAL_INSTANCES - offline_count
        print(f"Expected online:   {online_count}")
        print(f"Expected offline:  {offline_count}")

    print("=" * 70)
    print(f"\nOpen in browser: http://localhost:{args.port}/\n")

    # Check if backend is available
    try:
        resp = requests.get(f"{BACKEND_URL}/api/v1/config", timeout=2)
        print(f"✓ Backend is reachable (status: {resp.status_code})\n")
    except Exception as e:
        print(f"⚠ Warning: Cannot reach backend: {e}")
        print("  Make sure picodata is running with --http-listen\n")

    app.run(host=args.host, port=args.port, debug=False)

if __name__ == '__main__':
    main()
