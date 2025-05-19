# Grafana Dashboards for Picodata

## Provisioning for Grafana dashboard

Copy the `Picodata-provisioning.json` dashboard file into the `provisioning/dashboards/` directory.

In the same `provisioning/dashboards/` folder, create a file named `default.yml` with this content:

```yaml
apiVersion: 1
providers:
  - name: default
    folder: ''
    type: file
    options:
      path: provisioning/dashboards
```

Besides, you can optionally add a `datasource` to this file. For that, place a file called `default.yaml` in `provisioning/datasources/` with the following content (the `prometheus` server is available at http://localhost:9090):

```yaml
datasources:
  - name: Prometheus
    type: prometheus
    orgId: 1
    url: http://localhost:9090
```

## Running Picodata with Monitoring
This guide shows two ways to get a three-node Picodata cluster up and running, configure Prometheus for metrics scraping, and visualize everything in Grafana.

1. Run a 3 node Picodata cluster
  - ```bash
     picodata run --instance-dir i1 --init-replication-factor=2 --iproto-listen 127.0.0.1:3301 --pg-listen 127.0.0.1:5432 --http-listen 127.0.0.1:8081
     ```
  - ```bash
     picodata run --instance-dir i2 --listen 127.0.0.1:3302 --pg-listen 127.0.0.1:5434 --http-listen 127.0.0.1:8082 --peer 127.0.0.1:3301
    ```
  - ```bash
     picodata run --instance-dir i3 --listen 127.0.0.1:3303 --pg-listen 127.0.0.1:5435 --http-listen 127.0.0.1:8083 --peer 127.0.0.1:3301
    ```  
2. Populate the cluster with data
3. Setup Monitoring
   - **Manual Prometheus and Grafana setup**
      - Use `prometheus.yml` from this directory and launch it:
        ```bash
        prometheus --config.file=./prometheus/prometheus.yml
        ```
      - Start Grafana
        1. Ensure Grafana is running:
           ```bash
           sudo systemctl start grafana-server
           ```
   - **Docker Compose**
      - Use `docker-compose.yml` from this directory to setup the monitoring stack. It will start a Grafana instance with a pre-configured `datasource.yml` on `3000` port and a Prometheus instance with pre-configured `prometheus.yml` on `9090` port.
      ```bash
      docker-compose up -d
      ```
4. Open your browser to http://127.0.0.1:3000.
   - In Grafana:
      - Log in with `admin` as username and `grafana` as password.
      - Click `Create your first dashboard`
      - Click `Import a dashboard`.
      - Upload the `Picodata.json` file from your `picodata/monitoring/dashboard/` directory.
      - Select the Prometheus data source (or provisioned default).

Picodata dashboard supports Grafana 8, 9, 10, and 11. For detailed provisioning and configuration guidance, see Grafana’s official [documentation](https://grafana.com/docs/grafana/latest/administration/provisioning/)