## How do we triage incoming issues?

The goal of the process is to assign tickets to milestones and prioritise work
so that important things are completed in timely manner, fulfilling our promises
to external users of our product. Namely, solutions department and directly our clients.

Milestone represents a particular release. Since we use calver, milestone looks
like `25.1 - Datamart`. Where Datamart is the codename of the release. Codename
usually represents a feature that is most desired by our users, or a project
that'll directly benefit from the release.

### Important labels

We use labels to categorize issues.

Some of the important labels:

- **domain/**\* - the issue is relevant for specific domain, e g domain/sql
- **bug** - known product defect
- **debt** - this label is designated for follow up issues that need to be urgently resolved.
  For example some issue was discovered in the MR but was moved from the MR to be worked
  on separately
- **refactor** - the issue represents a refactoring we'd like to eventually complete
- **feature request** - issue represents a feature that is requested by one of our users
- **triaged** - issues passed through triage meeting
- **needs-more-info** - issue is not clear enough to be worked on, requires clarifications
- **icebox** - low priority, likely will not be worked on unless priority is changed
- **p/high** - high priority task critical to on our business requirements and product roadmap
  Important: be mindful when setting this label. Konstantin Osipov should always be in to loop
- **p/roadmap** - this ticket is a priority, but a long term one
- **breaking-change** - ticket introducing a breaking change
- **ux** - for tickets affecting ux of our users
- **dev-ux** - for tickets affecting our ux as developers of picodata

These are the most popular labels affecting planning decisions.
Full list of labels can be viewed [here](https://git.picodata.io/groups/core/-/labels)

### Lifecycle of the issue

Initially, fresh tickets may not be properly labeled.
So, to deal with that, there's a weekly triage meeting involving all team leaders.

The goal of the meeting is to properly label and assign approximate milestones
to as many tickets as possible. Tickets with unclear milestone get surfaced up
to Konstantin Osipov for final desicion.

If a ticket is deemed sufficiently clear, it gets marked with **triaged** label.

Otherwise **needs-more-info** label is used with a comment requesting clarifications,
usually from the author of the issue. Alternatively, one of the team members can be
assigned to investigate the issue to decide how to proceed with it.

If **triaged** label has not been added yet, the issue will be picked up on the next meeting and
the process will repeat until the issue becomes **triaged**.

The milestone worth mentioning is special **Picodata Next**. It contains tasks that we
eventually plan to do, but they fall outside of the planning horizon. Note that difference
with **icebox** label is not that big. Compared to **Picodata Next** milestone **icebox** issues
are not planned to be completed at all unless we change their priority.

Next, for each milestone there is a meeting with Konstantin Osipov to assign **p/high** label to tickets.
These are the tickets that we identify as release blockers and should resolve first,
before switching to tickets without **p/high**. Note that all **p/high** assignments should go through
Konstantin Osipov since they may require planning adjustments.

There may be separate triage meeting with solutions department to ensure our roadmaps align well.

Finally, issues with **p/high** label are assigned to people and we continue our journey towards a release.
