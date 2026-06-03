This is a tool for benchmarking pgproto connection establishment throughput.

It will spam connections to `psql://user:Password1@127.0.0.1:4327` with different
concurrency levels and write results to `trials.csv`.

If you have two experiment runs that you want to compare, you can use
a vibecoded script in `analysis`.
