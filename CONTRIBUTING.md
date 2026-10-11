# Contributing

By submitting a contribution (pull request, patch, or any other change) you
agree that your contribution is licensed under the **GNU General Public
License v3.0 or later** (`GPL-3.0-or-later`), the same license as the rest of
this project — see [`LICENSE`](LICENSE).

There is no separate contributor licensing agreement to sign. The standard
license terms, and the fact that you offered the contribution for inclusion,
are sufficient.

If a contribution vendors third-party material, that material must be under a
license compatible with `GPL-3.0-or-later`, and it must be recorded in
[`THIRD-PARTY-NOTICES.md`](THIRD-PARTY-NOTICES.md) with its origin, license,
and attribution.

## Running the test suite

```bash
python -m pytest
```

This runs every component's test suite, discovered from the repo root.

## Pull requests

Every pull request to `main` must close an issue: put `Closes #<issue>` in the
description, once per issue. The `Require linked issue` check blocks the merge
otherwise, and the `Protect main` ruleset (`.github/rulesets/protect-main.json`) has no bypass
actors. Update the live ruleset from it with:

```bash
id=$(gh api repos/BrentIO/SkyFollower/rulesets --jq '.[] | select(.name=="Protect main") | .id')
gh api repos/BrentIO/SkyFollower/rulesets/$id --method PUT --input .github/rulesets/protect-main.json
```
