# Cordis TypeScript SDK

Use generated service clients and the SDK Context for business calls. Node
programs start with `run` from `@cordis-x/sdk/adapters/process`.

Browser embeddings import `serve` and `portConnection` from
`@cordis-x/sdk/adapters/browser`. The embedding supplies an owned MessagePort
carrying complete binary Session frames and establishes the corresponding host
connection. The Session handles the same generated calls, retained scopes,
interface references, cancellation, resource drain and teardown as Node.

An activation Context cannot be saved for later UI events. Retain a Cordis Scope
and enter a new callback for each interaction. DOM nodes and React functions stay
in the client; public generated values cross the transport. Browser embedding
code owns window/origin admission and closes the port on loss. Node-only stdio
and HTTP setup are not included in the browser entry.

Generated clients expose static `contract()` and `interface()` descriptions for
binding declarations. Generated `pb_*` namespaces expose request/response schemas
alongside these clients without requiring an author-maintained service map.
