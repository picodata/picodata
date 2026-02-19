# WebUI Mock Cluster Testing

This guide explains how to test the WebUI with a large simulated cluster (800+ instances) using only 1 real picodata instance.

## Overview

The `test_webui_mock_cluster.py` script acts as a **proxy server** that:
- Intercepts WebUI API requests (`/api/v1/cluster`, `/api/v1/tiers`)
- Returns mocked data simulating hundreds of instances
- Forwards authentication and static assets to the real picodata instance
- Can simulate instance failures (every Nth instance appears offline)

## Quick Start

### 1. Install Dependencies

```bash
# Using pip
pip install -r test_webui_requirements.txt

# Or using poetry (if in the project)
poetry install
poetry run pip install flask requests
```

### 2. Start Real Picodata Instance

```bash
# Build with webui feature
make build-dev

# Start with HTTP server
./target/debug/picodata run --http-listen 0.0.0.0:8080
```

### 3. Start Mock Server

```bash
# Default: 800 instances, every 5th offline, proxy on port 8888
python3 test_webui_mock_cluster.py

# Or with custom settings
python3 test_webui_mock_cluster.py \
  --backend http://localhost:8080 \
  --port 9090 \
  --instances 1000 \
  --failure-rate 10
```

### 4. Open WebUI

Open your browser to: **http://localhost:8888**

The WebUI will show 800 instances (or whatever you configured) instead of just 1.

## Usage Examples

### 800 instances, every 5th offline
```bash
python3 test_webui_mock_cluster.py \
  --instances 800 \
  --failure-rate 5
```
Result: 640 online, 160 offline

### 1000 instances, all online
```bash
python3 test_webui_mock_cluster.py \
  --instances 1000 \
  --failure-rate 0
```
Result: 1000 online, 0 offline

### 500 instances, every 2nd offline
```bash
python3 test_webui_mock_cluster.py \
  --instances 500 \
  --failure-rate 2
```
Result: 250 online, 250 offline

### Custom backend and port
```bash
python3 test_webui_mock_cluster.py \
  --backend http://192.168.1.100:8080 \
  --port 7777 \
  --instances 800
```

## Command-Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--backend` | `http://localhost:8080` | URL of real picodata instance |
| `--port` | `8888` | Port to run mock server on |
| `--instances` | `800` | Total number of instances to simulate |
| `--failure-rate` | `5` | Every Nth instance is offline (0 = all online) |
| `--host` | `0.0.0.0` | Host to bind to |

## How It Works

### Request Flow

```
Browser → Mock Server (port 8888) → Real Picodata (port 8080)
          ↓
      /api/v1/cluster    → Mocked response (800 instances)
      /api/v1/tiers      → Mocked response (267 replicasets)
      /api/v1/config     → Proxied to real backend
      /api/v1/session    → Proxied to real backend (auth)
      / (static files)   → Proxied to real backend (webui assets)
```

### Mocked Data Structure

- **Instances**: Named `i1` through `iN`
- **Replicasets**: 3 instances each, named `default_1`, `default_2`, etc.
- **Failure domains**: Distributed across 10 zones and 50 racks
- **State**: Every Nth instance marked as "Offline" (configurable)
- **Ports**: Sequential allocation (binary: 3301+, pg: 4327+, http: 8081+)

### What's Real vs. Mocked

| Component | Source |
|-----------|--------|
| WebUI static files | Real backend (proxied) |
| Authentication | Real backend (proxied) |
| Cluster info | **Mocked** (800 instances) |
| Tier/instance data | **Mocked** (800 instances) |
| Configuration | Real backend (proxied) |

## Testing Scenarios

### Test WebUI Performance
```bash
# Test with increasing instance counts
for n in 100 500 800 1000 1500 2000; do
  python3 test_webui_mock_cluster.py --instances $n --port 8888
  # Open browser, test UI responsiveness, then Ctrl+C
done
```

### Test Failure Scenarios
```bash
# 50% failure rate
python3 test_webui_mock_cluster.py --instances 800 --failure-rate 2

# 90% online (every 10th offline)
python3 test_webui_mock_cluster.py --instances 800 --failure-rate 10

# Total cluster failure (all offline)
python3 test_webui_mock_cluster.py --instances 800 --failure-rate 1
```

### Test Filtering/Grouping
The mocked instances include:
- **10 failure domain zones**: `zone-1` through `zone-10`
- **50 failure domain racks**: `rack-1` through `rack-50`

You can test WebUI filtering by these domains.

## Troubleshooting

### Backend not reachable
```
⚠ Warning: Cannot reach backend
```
**Fix**: Make sure picodata is running with `--http-listen`:
```bash
./target/debug/picodata run --http-listen 0.0.0.0:8080
```

### Port already in use
```
Address already in use
```
**Fix**: Use a different port:
```bash
python3 test_webui_mock_cluster.py --port 9999
```

### Authentication errors
If WebUI shows login errors, the mock server properly proxies auth to the real backend. Check that the real picodata instance has authentication configured correctly.

## Development Notes

### Customizing Mocked Data

To customize the mocked response (e.g., different memory values, capacity usage):

1. Edit the `generate_instance()` function in `test_webui_mock_cluster.py`
2. Modify instance properties like `memory.usable`, `capacityUsage`, etc.
3. Restart the mock server

### Adding More Endpoints

To mock additional endpoints:

```python
@app.route('/api/v1/your-endpoint', methods=['GET'])
def mock_your_endpoint():
    # Generate and return mocked data
    return jsonify({"your": "data"})
```

### Performance Considerations

- The mock server generates data on-demand (not pre-cached)
- For 800 instances: response generation takes ~50-100ms
- For 2000+ instances: consider caching the generated responses

## Static Test Data

The script also generated `tiers800.json` which contains pre-generated JSON for 800 instances. You can use this file for:
- Manual testing
- Copying into other test scripts
- Reference for the expected data structure

```bash
# View the structure
python3 -m json.tool tiers800.json | head -50
```
