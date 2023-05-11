import os
import subprocess


# You need xk6 installed to run k6 for tarantool
# (https://github.com/grafana/xk6).
class K6:
    concurrency: int
    duration: str
    summary_name: str
    path: str
    program: str

    def __init__(
        self,
        concurrency: int,
        duration: str,
        name: str,
        path: str,
        program: str,
    ):
        self.concurrency = concurrency
        self.duration = duration
        self.name = name
        self.path = path
        self.env = ""
        self.program = program

    @property
    def metrics_program(self):
        return """
            import { Rate } from "k6/metrics";
            import tarantool from "k6/x/tarantool";

            export const successRate = new Rate("success");

            export function updateSuccessRate(resp) {
              if (resp.data[0][0] === null ) {
                successRate.add(false);
              } else {
                successRate.add(true);
              }
            }

            export function errorSuccessRate() {
              successRate.add(false);
            }

            export function tryTarantoolCall(client, func, args) {
              try {
                  var resp = tarantool.call(client, func, args);
                  return resp;
              } catch (e) {
                  console.log(e);
                  return null;
              }
            }

            export function callTarantool(client, func, args)  {
              var resp = tryTarantoolCall(client, func, args);
              if (resp == null) {
                  errorSuccessRate();
              } else {
                  updateSuccessRate(resp);
              }
            }
        """

    @property
    def k6(self):
        return os.path.join(self.path, "k6")

    @property
    def metrics(self):
        return os.path.join(self.path, "metrics.js")

    @property
    def summary(self):
        return os.path.join(self.path, "{}_summary.json".format(self.name))

    @property
    def script(self):
        return os.path.join(self.path, "{}.js".format(self.name))

    def _build(self):
        subprocess.run(
            [
                "xk6",
                "build",
                "--with",
                "github.com/WeCodingNow/xk6-tarantool",
                "--output",
                self.k6,
            ],
            check=True,
        )
        self._env_save()
        os.environ["PATH"] = "{}:{}".format(self.path, self.env)

    def _env_save(self):
        self.env = os.environ.get("PATH", "")

    def _env_restore(self):
        os.environ["PATH"] = self.env

    def _start(self):
        os.makedirs(self.path, exist_ok=True)
        self._build()
        with open(self.metrics, "w") as f:
            f.write(self.metrics_program)
        with open(self.script, "w") as f:
            f.write(self.program)

    def _stop(self):
        if os.path.exists(self.k6):
            os.remove(self.k6)
            self._env_restore()
        if os.path.exists(self.script):
            os.remove(self.script)
        if os.path.exists(self.metrics):
            os.remove(self.metrics)

    def run(self):
        try:
            self._start()
            subprocess.run(
                [
                    "k6",
                    "run",
                    "-u",
                    str(self.concurrency),
                    "-d",
                    self.duration,
                    self.script,
                    "--summary-export",
                    self.summary,
                ],
                check=True,
            )
        finally:
            self._stop()
