---
name: polodb-release
description: Prepare, publish, resume, or verify a PoloDB release across GitHub and crates.io. Use when Codex needs to bump PoloDB versions, create a release PR, wait for protected-branch checks and approval, tag a merged release, publish the Rust crates in dependency order, trigger the repository's release workflow, add GitHub Release notes, or diagnose a partially completed PoloDB release.
---

# PoloDB Release

Release PoloDB from a clean, reviewed commit. Treat tags and crates.io uploads as immutable production actions.

## Establish the release state

1. Confirm the requested semantic version and whether the user authorized the full release or only preparation.
2. Inspect `git status --short --branch`, the current branch, remote, existing tags, and open release PRs.
3. Preserve unrelated work. Honor the user's requested worktree strategy; do not create an extra worktree without need.
4. Read `.github/workflows/release.yml` before assuming how releases are triggered. It currently requires `workflow_dispatch` with a `tag` input; pushing a tag alone does not run it.
5. Inspect the latest published versions before choosing version bumps:

   ```bash
   cargo search polodb-librocksdb-sys --limit 1
   cargo search polodb_core --limit 1
   cargo search polodb --limit 1
   ```

6. Stop before any irreversible action if the requested version or tag already exists unexpectedly.

## Prepare the release PR

1. Start from the latest `origin/master` on `release/<version>` unless the user specifies another branch.
2. Compare the previous release tag to `master`, then update only the crates that changed. Never infer a sys bump from changes already shipped in an older release:
   - `src/librocksdb-sys/Cargo.toml`: `polodb-librocksdb-sys`
   - `src/polodb_core/Cargo.toml`: `polodb_core` and its sys dependency requirement
   - `src/polodb/Cargo.toml`: `polodb` and its core dependency requirement
3. Regenerate or update `Cargo.lock` so workspace package versions match the manifests.
4. Add a concise top section to `CHANGELOG.md`. Link the PRs that provide the user-visible changes.
5. Inspect the packaged sys crate. Exclude unused vendored documentation, Java bindings, benchmarks, or third-party test data if they inflate the upload, but never exclude native sources needed by `build.rs`.
6. Review the exact diff and run `git diff --check` before staging only the intended files.

## Validate before opening the PR

Run the release-level workspace tests:

```bash
cargo metadata --locked --format-version 1 --no-deps
cargo test --locked --release --verbose --workspace --exclude py-binding-polodb
```

If server tests fail only because the execution sandbox blocks local socket binding, rerun the identical test command with authorized socket permissions. Do not skip or weaken the tests.

Run a full publish dry-run for every crate planned for publication whose dependencies already exist on crates.io. Skip examples for unchanged crates:

```bash
cargo publish --dry-run --locked --allow-dirty -p polodb-librocksdb-sys
cargo publish --dry-run --locked --allow-dirty -p polodb_core
cargo publish --dry-run --locked --allow-dirty -p polodb
```

If a bumped internal dependency has not been published yet, a dependent dry-run is expected to fail resolution. In that case:

- run `cargo package --locked --allow-dirty --no-verify --list -p <package>` before the PR;
- record why the dependent dry-run is deferred;
- run the full dependent dry-run after publishing its prerequisite and before uploading it.

The sys dry-run must compile the exact packaged archive and report an acceptable compressed size. Investigate package contents rather than using `--no-verify` to hide a packaging problem.

Open a ready-for-review PR with:

- version and dependency changes;
- release highlights;
- all validation commands and results;
- the intended crate publication order.

## Respect protected-branch gates

1. Wait for every required check, including `Rust / Required CI Gate`.
2. Require the configured approval. Never use administrator merge to bypass the approval safeguard.
3. If the author cannot self-approve and auto-merge is unavailable, report the exact PR link and wait for another maintainer.
4. Merge only after the checks and approval are satisfied.

## Create the immutable tag

After merge:

1. Fetch `master` and tags, switch to local `master`, and fast-forward to `origin/master`.
2. Resolve the PR merge commit and verify its tree contains the reviewed release changes.
3. Confirm `v<version>` does not exist locally or remotely.
4. Create an annotated tag on the merge commit:

   ```bash
   git tag -a v<version> <merge-commit> -m "release: v<version>"
   git push origin v<version>
   ```

5. Verify the peeled remote tag points to the merge commit. Never move a published release tag.

## Publish crates.io packages

Require `cargo login` locally when no token is configured. Ask the user to run it in their terminal; never request or print a crates.io token in chat or logs.

Confirm ownership before uploading:

```bash
cargo owner --list polodb-librocksdb-sys
cargo owner --list polodb_core
cargo owner --list polodb
```

Publish only from the clean tagged commit. Publish changed crates in dependency order:

1. `polodb-librocksdb-sys`
2. `polodb_core`
3. `polodb`

For each crate:

1. Confirm the target version is not already published.
2. Run its full `cargo publish --dry-run --locked -p <package>` if that exact dependency graph has not yet been verified.
3. Run `cargo publish --locked -p <package>`.
4. Wait until `cargo search` shows the new version before validating or publishing the dependent crate.

Do not continue after an ambiguous upload failure until the registry is checked. A client-side failure may occur after crates.io accepted the upload.

### Recover from proxy upload failures

If a large upload repeatedly fails with HTTP/2 `STREAM_CLOSED`:

1. Check crates.io first. Do not blindly retry.
2. Check whether HTTP, HTTPS, or ALL proxy variables are set without printing their values.
3. If policy permits direct crates.io access, retry only the upload command without proxy variables and disable Cargo multiplexing:

   ```bash
   env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
     -u http_proxy -u https_proxy -u all_proxy \
     CARGO_HTTP_MULTIPLEXING=false \
     cargo publish --locked --no-verify -p <already-verified-package>
   ```

Use `--no-verify` here only after the identical packaged crate has already passed full verification. Preserve proxy settings for all other commands.

## Build and publish the GitHub Release

Trigger the existing workflow after the tag is visible remotely:

```bash
gh workflow run release.yml --repo PoloDB/PoloDB -f tag=v<version>
```

Monitor the dispatched run through completion. Verify all jobs succeed:

- Resolve release tag
- Build on MacOS
- Build on Ubuntu
- Build on Windows
- Publish GitHub Release

Verify that the published Release is neither a draft nor a prerelease and contains:

- `polodb-darwin-x64`
- `polodb-linux-x64`
- `polodb-win32-x64.exe`

## Add release notes

Do not leave the GitHub Release body empty. Summarize `CHANGELOG.md` in English and include:

- a one-sentence release overview;
- a `Highlights` section with direct PR links;
- `cargo add polodb_core@<version>` for the embedded Rust library;
- `cargo install polodb --version <version>` for the standalone CLI server;
- links to all crates published for the release;
- a note about the attached CLI binaries;
- `https://github.com/PoloDB/PoloDB/compare/v<previous>...v<version>`.

Use `gh release edit v<version> --notes-file <file>` and reread the Release afterward to confirm the body and assets.

## Final verification

Before reporting success, verify all of the following:

- local `master`, `origin/master`, and the peeled release tag resolve to the expected merge commit;
- the worktree is clean;
- every published crate version is visible in the crates.io index;
- the GitHub Actions release run succeeded;
- the public Release page has notes and all three assets.

Return direct links to the PR, Actions run, GitHub Release, and crates.io versions. Clearly identify anything incomplete; never describe a partial release as complete.
