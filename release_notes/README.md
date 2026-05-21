# Changelog fragments

This directory hosts the user-facing changelog flow. Per-MR fragments
live under `release_notes/unreleased/` (one Markdown file per
user-visible change); the release manager consolidates all of them into
a single `RELEASE_NOTES.md` section at release time and deletes the
consumed files.

The model is adapted from
[Tarantool's process](https://github.com/tarantool/tarantool/blob/master/doc/changelogs.md),
which in turn is inspired by
[GitLab's changelog](https://docs.gitlab.com/ee/development/changelog.html).
The motivation is the same:

- **No merge conflicts** — every MR touches a different file, so
  parallel MRs do not fight over a shared `RELEASE_NOTES.md`.
- **Editable post-merge** — a fragment can be reworded by a follow-up MR
  without amending the commit that introduced it.
- **Easy to backport** — a fragment is just a file; cherry-pick it between
  release branches the same way you cherry-pick the code change.

## File format

```markdown
$ cat release_notes/unreleased/equality-facts.md
## feat/sql

- Added equality-facts analysis pass over the relational plan ([!2901]).
- Optimize an extra subplan clone in single-replicaset dispatch ([!2902]).
```

- Filename: `<short-slug>.md`. Pick a slug that names the change, not the
  MR number — fragments may be reworded or merged, and slugs are easier
  to read in a directory listing.
- First non-blank line: `## <type>` or `## <type>/<category>`.
- After the header: one or more bullets. GitHub-flavoured Markdown is
  fully supported (code, links, multi-line bullets).
- Write GitLab MR references inline as `[!1234]` for Picodata or
  `[tarantool!1234]` for Tarantool — the generator expands both forms
  to full links.

### Allowed types

| Token         | Renders under          |
|---------------|------------------------|
| `breaking`    | Breaking changes (hoisted to top) |
| `feat`        | Features               |
| `fix`         | Bug fixes              |
| `refactor`    | Refactor               |
| `docs` / `doc`| Documentation          |
| `perf`        | Performance            |
| `test`        | Testing                |
| `chore`       | Miscellaneous Tasks    |
| `revert`      | Revert                 |
| `build`       | Build                  |

The token mirrors the Conventional Commits prefix. `<category>` (after
the `/`) is free-form scope: `sql`, `replication`, `pgproto`, `webui`,
`cli`, etc.

### Referencing the GitLab MR

Write Picodata MR references inline as `[!1234]` and Tarantool MR
references as `[tarantool!1234]` — the generator expands both to full
links. If the Picodata inline reference is missing but the
fragment-introducing commit carries a `Part-of: !1234` trailer or a
`See merge request !1234` line, the tool emits a warning naming the
detected MR so you can add the inline reference in a follow-up; it does
**not** auto-append the link (that would attach it to the last bullet
and misrepresent multi-bullet fragments). There is no functionality to
link a GitLab issue in the [#1234] format.

### Release-manager note (optional)

To leave a hint for the maintainer that should NOT be published verbatim,
append a `----` separator at the end of the fragment:

```markdown
## breaking/cli

- Renamed `--foo` to `--bar` ([!2950]).

----

Migration: existing scripts must be updated; the `--foo` form is
rejected with an explicit error at startup.
```

Everything before `----` is published as-is. Everything after `----`
lands inside a top-of-section comment in `RELEASE_NOTES.md`; the
release manager reads it while editing the section and removes the
comment before merging the release MR.

## When is a fragment required?

Anything user-visible: new functionality, behaviour changes, bug fixes,
removed/renamed CLI flags or SQL syntax, security fixes, notable
performance characteristics.

Internal-only work (pure refactor with no behavioural effect, test
churn, build-tooling changes) does not need a fragment.

## Generation

The release manager runs `make prepare-release TAG=X.Y.Z` (or
`tools/release_changelog.py X.Y.Z`) on the release branch. The script:

1. Renders every `release_notes/unreleased/*.md` fragment through the
   `tools.release_notes` Python renderer.
2. Splices the result into `RELEASE_NOTES.md`.
3. `git rm`s every consumed fragment from `release_notes/unreleased/`.

See `doc/dev/generating-changelog.md` for the full release flow.
