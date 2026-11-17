# Staging

`staging` is this repo's integration branch. `main` is what production runs.

1. **Changes go to `staging` by PR**, Dependabot's included. CI runs on the PR
   and on the push, and `staging` carries the same required checks as `main`.
2. **Every push to `staging` publishes `ghcr.io/pete-builds/open-setlist-stash:staging`**
   (plus an immutable `sha-<commit>` tag). A staging tenant runs that image
   against its own database, so a change is played with before any real
   community sees it. `:latest` still follows `main` only.
3. **Promotion is a PR from `staging` into `main`, merged with a merge commit**
   (not squash), so the tree on `main` is exactly the tree the staging tenant
   ran. Afterwards `staging` is fast-forwarded to `main` to stay level.
4. Commits that reach `main` directly (the weekly lockfile loop opens its PRs
   there) are merged back into `staging` by a PR before the next promotion.

Self-hosters are unaffected: pin a release tag (`:0.3.0`) or follow `:latest`.
