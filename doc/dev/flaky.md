## How we deal with flaky tests

### Flaky tests and problems they cause

Integration tests are by their nature flaky. In other words, they sometimes
fail due to nondeterminism in the environment and not because of the changes in code.

Pytest has a pretty good [page](https://docs.pytest.org/en/stable/explanation/flaky.html) on flaky tests;
it explains what they are and what tools exist in the ecosystem to deal with them.

Flaky tests cause all sorts of problems. If a test fails sporadically, it prevents code from being merged.
People waste their time retrying CI pipelines over and over again. So having flaky tests makes us less
productive as a team. CI loses trust; people just rerun pipelines without even looking into details.
It becomes tempting to merge Merge Requests with red CI.

Can we get rid of flaky tests entirely? For sure we **must** aim at building deterministic tests.
But this is a constant battle, new changes get introduced, because we build distributed system
and use real network we rely on timeouts in our tests. Stuff gets overlooked in test code.
So having flaky tests is almost inevitable.

Another thing to keep in mind is that flaky test is not just an annoying reason for you to hit
a restart button in CI but it can represent a real bug in the code which can affect real customers.

### Fighting with flaky tests

What can we do to fight flaky tests?

#### Careful design / review

First thing is careful design of new tests. Be critical to possible race conditions and timeouts that might
be relevant for a test. Frequently cause of the failure is absence of barriers. For example when you execute
two operations in a row, but second one will succeed or fail depending on whether first operation completed
on certain nodes of the cluster. Since we build distributed system changes we make are not instantaneous.
Different nodes receive data with different timing, some earlier some later. So in case completion of certain
action is important for next steps of the test, be sure to add a retry with a timeout to make sure that expected
change made it to relevant nodes before continuing to next steps.

#### Rerunning Tests many Times

One way to see if test is flaky or not is to run it a couple of times to see whether there are any failures.
Running a test 50 times is OK, but running it more times may be necessary to catch tests that flake more rarely.

There are several ways you can get the "run test multiple times" behavior:

##### Looping in your favorite shell

The simplest way is doing a while loop that will stop at first test failure with your shell,
but doing it at scale is more involved than using pytest plugins (see the next section).

A simple setup:
- with `bash`/`zsh`: `for i in $(seq 1 50); do clear; poetry run pytest -s [TEST_PATH]::[TEST_NAME] || break; done`
- with `fish`: `for i in (seq 1 50); clear; poetry run pytest -s [TEST_PATH]::[TEST_NAME]; or break; end`

You can do several improvements to this setup:
- Skip a no-op call to `cargo` in each iteration to make sure the test binaries are up-to-date by passing `CI=1` environment variable
- Run the loop in multiple terminal windows. This requires segregating network port ranges used by different pytest instances,
  which can be done by explicitly passing `--base-port [NUMBER]` to each instance. Spreading instances 100 ports apart should be enough for our codebase.

Then the command for each window becomes:
- with `bash`/`zsh`: `make build-dev; while clear && CI=1 poetry run pytest --base-port 3303 -s [TEST_PATH]::[TEST_NAME]; do true; done`
- with `fish`: `make build-dev; while clear && CI=1 poetry run pytest --base-port 3303 -s [TEST_PATH]::[TEST_NAME]; end`

(add 100 to the `--base-port` number for each window you run the command in).

##### Using `pytest-flake-finder` and `pytest-xdist`

You can also use [`pytest-flake-finder`](https://pypi.org/project/pytest-flakefinder/) and
[`pytest-xdist`](https://pypi.org/project/pytest-xdist/) pytest plugins, which come pre-installed in our testing environment.

`pytest-flake-finder` allows you to run a single test multiple times within a single pytest invocation.
To do that you should pass `--flake-finder` to enable the plugin and control how much you want to rerun tests either with
`--flake-runs=[COUNT]` (by number of runs) or `--flake-max-minutes=[DURATION]` (by run time).

`pytest-xdist` allows you to execute multiple tests in parallel, utilizing more CPU cores that might otherwise be idle.
You can enable it with `-n [COUNT]` (use an explicit number of processes) or `-n auto` (use as many processes as your computer has physical CPU cores).
I find that `-n auto` usually does not result in full system load (which you want to minimize time to reproduce a flake),
so using a number 2x the number of your physical CPU codes might be better.

Here's the command line to utilize both plugins:

```bash
poetry run pytest --flake-finder --flake-runs=1000 -n auto [TEST_PATH]::[TEST_NAME]
```

You might also want to use `-x` flag to stop testing on first test failure, which allows you to see the output of the test failure earlier.

##### Simulating CPU contention

Additional stress factor for the test can be limited amount of CPU which sometimes happen when CI machine is under higher load.
To emulate this locally consider using `taskset` and `cpulimit` utilities.

#### Searching for flaky tests on master branch

The `tools/flaky_finder.py` script is integrated into CI and regularly updates statistics on flaky tests for the last 30 days on the master in the `pico-dev` chat.

You can also run it locally. Here are a few examples:

```shell
# Find flaky tests from the last 7 days.
./tools/flaky_finder.py -d 7

# Find flaky tests that match a regular expression.
./tools/flaky_finder.py -f 'test_ddl_ok|test_failover'

# Get stack traces for specific tests.
./tools/flaky_finder.py -vf 'test_dml_ok'

# More options.
./tools/flaky_finder.py --help
```

#### Keeping Pipelines Green

It happens that some tests are flaky for long period of time because either investigation was unsuccessful
(we add more logs and attempt fixes but test still fails) or we don't have immediate resources to investigate
the failure.

Main goal is to allow people to merge new code without being blocked on red CI caused by flaky tests.
To implement that goal we use [pytest-rerunfailures](https://github.com/pytest-dev/pytest-rerunfailures) plugin.
It works via rerunning tests that are marked as flaky automatically without failing the whole pipeline.

An example:

```python
@pytest.mark.flaky(reruns=3)
def test_ddl_create_table_unfinished_from_snapshot(cluster: Cluster):
    """
    flaky: https://git.picodata.io/core/picodata/-/issues/871
    """
```

As you see there is not only a `@pytest.mark` thing on top of the function but also a link to issue in the
function doc comment. So for each active flaky test we want to have an open ticket.
It gives us the ability to track progress on investigation and in general number of such tickets gives us
a metric to assess how bad the situation is. It also can be a release blocker if we have too many flaky tests
since this can indicate general instability of the project.

Automatically retrying flaky tests is good because it keeps pipelines green but this way we lose track of
tests that are still problematic. So to deal with them we have small automation that posts test failures to
corresponding tickets. This is why appropriately naming your ticket is important. For details on the automation see
our `conftest.py` for calls to GitLab API.

### Call to Action

In conclusion, everyone is encouraged to investigate unrelated test failures in their patches. Found problems need to be fixed
in the same merge request with a separate commit, or in case there is no obvious fix the test needs to be marked as flaky and
ticket with relevant details must be created.
This way everybody will have better focus on their tasks and we have better velocity
and success as a team in general.
