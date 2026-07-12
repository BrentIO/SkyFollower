# Deployment

SkyFollower is a single monorepo, but it deploys as four independent
hosts — each one clones the repo and brings up exactly one Docker Compose
file. See [Getting Started](/getting-started/) for the commands to
actually bring a host up once you know which compose file it runs.

<!--@include: ../../README.md#host-topology-->

<!--@include: ../../README.md#compose-files-->

<!--@include: ../../README.md#components-->

<!--@include: ../../README.md#configuration-->

See the component pages for the full list of settings fields:
[Receiver](/components/receiver), [Processor](/components/processor),
[Archive Processor](/components/archive-processor), and
[Data Runners](/data-runners/) (logging convention, plus one page per
runner).
