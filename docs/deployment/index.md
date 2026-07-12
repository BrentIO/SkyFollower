# Deployment

SkyFollower is a single monorepo, but it deploys as four independent
hosts — each one clones the repo and brings up exactly one Docker Compose
file. See [Getting Started](/getting-started/) for the commands to
actually bring a host up once you know which compose file it runs.

<!--@include: ../../README.md#host-topology-->

<!--@include: ../../README.md#compose-files-->

<!--@include: ../../README.md#components-->

<!--@include: ../../README.md#configuration-->

See the component READMEs for the full list of settings fields:

- [receiver/README.md](https://github.com/BrentIO/SkyFollower/blob/main/receiver/README.md)
- [processor/README.md](https://github.com/BrentIO/SkyFollower/blob/main/processor/README.md)
- [data-runners/README.md](https://github.com/BrentIO/SkyFollower/blob/main/data-runners/README.md) — logging convention, plus per-runner READMEs (e.g. [data-runners/ourairports/README.md](https://github.com/BrentIO/SkyFollower/blob/main/data-runners/ourairports/README.md))

::: tip
These per-component READMEs will get their own pages in this docs site —
until then, the links above go straight to GitHub.
:::
