## Maintainer of the Week

### What It Means for Codebase to Be Healthy

Each codebase needs to be properly maintained to be healthy.

Key points that represent a healthy codebase include but are not limited to:

- Tests are fast and not flaky
- Master CI is always green
- Code coverage setup helps to avoid missing critical cases in tests
- Our Grafana dashboard and metrics contain enough clues to debug production issues
- In general CI is fast, logically easy to follow
- Tech debt is kept at minimum level

During development of the product typically features are prioritized over codebase health.
This is fine if we return later to revisit the problem and clean up created tech debt.
But in case we don't come back to fix our stuff then it only gets worse over time.

This initiative is aimed at creating a healthy balance between feature development and
keeping codebase well maintained.

### Lack of Directly Responsible Individual

Currently there is no Directly Responsible Individual, so maintenance tasks are done sporadically.
The observation is that we're not converging towards sweet spot, in other words problems keep boiling
without proper fixes and maintenance falls behind which impacts our velocity as a team, and in general creates
excessive friction for development.

This initiative aims to have one person responsible only for maintenance tasks during each week.
This way we continuously work on making things better and keep healthy balance between maintenance and feature work.

Additionally, this way more people will become more familiar with CI and build system.

### The Process

There is a schedule where each week is assigned to a particular person – maintainer of the week (MOTW).
MOTW is the person that for one week changes focus from milestone tasks to work on codebase health.

Duty tasks are assigned to MOTW in advance. The task is picked in coordination with the manager.
There can be several tasks depending on their size. During the week MOTW works on resolving these tasks,
paying special attention to CI health of the master branch.

Monday after the week on duty MOTW presents results of his work during Monday team meeting.
This is important to showcase completed work and make people aware of it.

### What MOTW Tasks Can Look Like?

First of all – flaky tests. We have quite of a problem with flaky tests and currently
CI in master is more often red than green. Sometimes one have to retry jobs 5+ times to get stuff merged.
This is a BIG problem. Aside from flaky tests we have artifacts coming from several submodules
being merged together which we need to clean up. For example at the time of the writing
tarantool-module still contains its own workspace. We have `doc` and `docs` paths in our root directory.
We do not have coverage setup and we do not run our tests under asan.

Other areas of work include but are not limited to:

- Our Grafana dashboard
- CI/build system improvements/fixes
- Documentation improvements
- General tooling improvements
- Refactorings (yes, intentionally last one in the list)

### Details on Rotation

Key trait of the maintainer is autonomy. Being picked as a participant in the rotation is
an acknowledgment that person knows the project well, can communicate across teams
and has the ability to perform across our wide tech stack.

Managers do not participate in the rotation, because they can't provide enough dedication
to the task because of the manager responsibilities.
Though if manager volunteers to join the list that's OK.
