# Getting Started

SkyFollower runs across up to four hosts, each bringing up exactly one
Docker Compose file. See [Deployment](/deployment/) for the full host
topology and compose-file mapping before you start — this page only
covers the commands to actually bring each host up.

Clone the repo on every host that will run a SkyFollower component, then
copy the example settings for whichever components run on that host (see
[Deployment](/deployment/#configuration) for the full list) and fill in
real values before starting containers.

<!--@include: ../../README.md#quick-start-->

## Next steps

- [Deployment](/deployment/) — host topology, compose-file mapping, and full component list
- Component READMEs for settings fields: [receiver](https://github.com/BrentIO/SkyFollower/blob/main/receiver/README.md), [processor](https://github.com/BrentIO/SkyFollower/blob/main/processor/README.md), [data runners](https://github.com/BrentIO/SkyFollower/blob/main/data-runners/README.md)
