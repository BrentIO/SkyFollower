<script setup>
import OpenApiViewer from "./OpenApiViewer.vue";
</script>

# OpenAPI

The management-ui backend's full REST API — rules/areas configuration,
read-only reference-data lookups, and the archive search (Athena/Glue)
endpoints — rendered interactively from
[`specs/openapi.yaml`](https://github.com/BrentIO/SkyFollower/blob/main/specs/openapi.yaml).

::: tip No auth
management-ui has no authentication (single-instance, trusted-network
deployment). The "Try it
out" button below sends real requests to whatever base URL you point it
at — nothing is called automatically, but if you use it against your own
instance, remember there's no auth in front of it.
:::

<ClientOnly>
  <OpenApiViewer />
</ClientOnly>
