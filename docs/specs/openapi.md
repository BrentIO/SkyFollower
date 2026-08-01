<script setup>
import OpenApiViewer from "./OpenApiViewer.vue";
</script>

# OpenAPI

The management-ui backend's full REST API — rules/areas configuration,
read-only reference-data lookups, and the archive search (Athena/Glue)
endpoints — rendered interactively from
[`specs/openapi.yaml`](https://github.com/BrentIO/SkyFollower/blob/main/specs/openapi.yaml).

::: tip No auth
management-ui has no authentication (home lab deployment) — the "Try it"
console is hidden here to avoid encouraging live requests against a
reader's own unauthenticated instance straight from this public docs
page. Use `curl`/Postman/etc. against your own instance instead.
:::

<ClientOnly>
  <OpenApiViewer />
</ClientOnly>
