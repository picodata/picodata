# Generating CHANGELOG.md and RELEASE_NOTES.md

Picodata maintains two files, both regenerated only on release branches:

- **CHANGELOG.md** — developer-facing summary. Derived from commit
  subjects (Conventional Commits) by `git-pico-cliff` (see `cliff.toml`).
- **RELEASE_NOTES.md** — user-facing detail. Derived from per-MR fragments
  under `release_notes/unreleased/` (see `release_notes/README.md`
  for the file format).

master is never regenerated automatically.

## Commit-time check (every MR)

One CI gate runs on every MR; the same check runs locally via the
`commit-msg` hook installed by `make setup-hooks`. Both share the
single source of truth at `.pre-commit-config.yaml`:

- `validate-commit-headers` — every commit subject must follow
  [Conventional Commits v1.0.0](https://www.conventionalcommits.org/en/v1.0.0/).

```bash
make setup-hooks
```

## Adding a changelog fragment

When your MR has user-visible impact, add a fragment under
`release_notes/unreleased/<short-slug>.md`. The file is a single H2 header
plus a bullet list. See `release_notes/README.md` for the full spec, allowed types,
and the optional `----` release-manager note convention.

Internal-only MRs (pure refactor with no behavioural effect, test churn,
build tooling) do not need a fragment.

## Preparing a release

Cut a release on its `<MAJOR.MINOR>` branch (long-lived; patch
releases `X.Y.1`, `X.Y.2`, ... live there):

```bash
git switch 26.2
make prepare-release TAG=26.2.2
```

`tools/release_changelog.py` prints a warning if the current branch
does not look like `X.Y` (it still proceeds).

### One-time migration of pre-tooling unreleased content

The legacy `CHANGELOG.md` keeps hand-written prose under a
`## [X.Y.Z] - Unreleased` heading. The first automated run on a release
branch **replaces that block in place** with the cliff-generated commit
summary. Any prose that wasn't already captured by a Conventional
Commits subject will be lost unless it is moved to a fragment first.

Before the first `make prepare-release` on a given release branch:

1. Open `CHANGELOG.md`, find the `## [X.Y.Z] - Unreleased` block.
2. For each bullet that conveys real user-visible context (not just a
   restatement of the commit subject), create a fragment under
   `release_notes/unreleased/` per `release_notes/README.md`.
3. Commit the fragments. Then run `make prepare-release TAG=...` —
   the unreleased block is now safe to be replaced.

Under the hood `tools/release_changelog.py`:

1. Asserts the working tree is clean and the tag does not yet exist.
2. Generates a `[26.2.2]` block in CHANGELOG.md via
   `git-pico-cliff --tag 26.2.2`.
3. Generates a `[26.2.2]` block in RELEASE_NOTES.md via the
   `tools.release_notes` Python renderer, combining every fragment in
   `release_notes/unreleased/`.
4. Splices both blocks into the two files.
5. `git rm`s every consumed fragment from `release_notes/unreleased/`
   (RELEASE_NOTES has just absorbed them).
6. Stages everything.

### Useful flags

- `--commit` — also create a `chore: prepare release X.Y.Z` commit instead
  of leaving the changes staged.

## Local preview from any branch

To preview what a section would look like without preparing a real release:

```bash
# Dev-facing summary (commit subjects since the last tag):
git-pico-cliff --unreleased

# User-facing detail (current fragments):
python3 tools/release_notes.py --unreleased
```

`git-pico-cliff --unreleased` anchors on the nearest ancestor tag of HEAD.
On master that is the latest `.0` tag (e.g. `26.2.0`); on a release branch
it is the latest patch tag (e.g. `26.2.1`).

To preview an arbitrary range for the CHANGELOG side, pass it positionally:

```bash
git-pico-cliff --tag 26.2.2 26.2.1..HEAD
```

`tools/release_notes.py` does not use a git range — it always combines
exactly the fragments in `release_notes/unreleased/`.

## Tools reference

| File | Purpose |
|------|---------|
| `cliff.toml` | git-pico-cliff config: section ordering, skip rules, GitLab MR linking |
| `.pre-commit-config.yaml` | pre-commit config used by the local `commit-msg` hook and the `validate-commit-headers` CI job |
| `tools/release_notes.py` | renders the RELEASE_NOTES section from `release_notes/unreleased/*.md`; usable as a shell command or Python module |
| `tools/release_changelog.py` | entry point: produces both sections for an upcoming tag |
| `release_notes/unreleased/` | directory of pending changelog fragments |

## Notes

- The picodata fork of git-cliff lives at
  <https://git.picodata.io/core/git-pico-cliff>; it updates GitLab
  integration to be more suitable for picodata's git flow than upstream git-cliff
