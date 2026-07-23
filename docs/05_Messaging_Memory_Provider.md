# Messaging, Memory, and AI Providers

## Typed events

`Event` validates project ID, event type/version, producer, optional target, payload, priority, correlation/causation IDs, replay flag, and UTC timestamp. Critical event types have payload validators so malformed task/checkpoint/action events do not enter routing.

## Reliable publication

`EventRepository.save_event()` writes the event and a pending outbox row. `OutboxDispatcherActor` reserves only rows for its project, publishes to the project Dragonfly stream, and records delivery. Attempts use bounded backoff and expired reservations are reclaimable.

This yields at-least-once delivery. Consumers must be idempotent; event IDs and receipts provide the deduplication identity.

DoD completion does not depend on event delivery alone. After integration the worker invokes the evaluator/supervisor handler and persists `DOD_EVALUATED`; the project evidence generation remains pending for periodic reconciliation if that handoff fails. Replanning task-created events use a deterministic UUID derived from project, causal evaluation run, and task so duplicate PM delivery cannot multiply notifications.

## Trigger routing

`TriggerEngineActor` consumes the project stream with a consumer group and resolves agent subscriptions from PostgreSQL. It publishes a copy to each matching per-agent inbox. Targeted events route only to their target. Inbox and project streams have configured maximum lengths.

Workers create/claim an `event_receipts` row before handling. `PROCESSING` leases are expiring, `PROCESSED` receipts make redelivery idempotent, and stale Dragonfly pending entries are reclaimed with `XAUTOCLAIM`. Workers acknowledge only after the processed receipt commits, so a crash before that point permits recovery.

## Memory broker

Workers never query Milvus/MongoDB/MinIO directly for conversational context. `MemoryBrokerActor` enforces project and scope boundaries and constructs catch-up packets.

### Write path

1. Validate scope and identity.
2. Store the complete authoritative memory body, length, and hash in PostgreSQL; large records begin in `PENDING_OBJECT`.
3. For a large body, upload to versioned MinIO, verify size/SHA-256, then store URI/version plus an inline preview and mark `READY`; persist `OBJECT_FAILED` and raise if this saga cannot finish.
4. Add the complete TTL-bound working copy to MongoDB only after the durable long-term step succeeds.
5. If importance qualifies and an embedding provider is available, create the embedding through the gateway and upsert the scoped reference in Milvus.
6. Log semantic-index failure without losing the durable memory.

### Read path

A catch-up packet combines:

- recent durable project events;
- recent MongoDB documents in allowed scopes;
- PostgreSQL long-term memory records;
- Milvus semantic hits filtered by project/scope/private owner;
- reference bodies from inline PostgreSQL or MinIO as allowed.

The broker bounds item counts and total prompt characters. Private memory is readable only by its owner. Large raw repository dumps and secrets should never be promoted as memory.

## Provider registry

Profiles in `providers.yaml` include:

| ID | Family | Environment |
|---|---|---|
| `openai` | OpenAI | `OPENAI_API_KEY` |
| `anthropic` | Claude | `ANTHROPIC_API_KEY` |
| `gemini` | Google Gemini | `GEMINI_API_KEY` |
| `deepseek` | DeepSeek | `DEEPSEEK_API_KEY` |
| `moonshot` | Kimi | `MOONSHOT_API_KEY` |
| `alibaba` | Qwen/DashScope | `DASHSCOPE_API_KEY` |
| `zai` | GLM | `ZAI_API_KEY` |
| `minimax` | MiniMax | `MINIMAX_API_KEY` |
| `ollama` | Local Ollama | `OLLAMA_API_BASE` |

Each profile declares chat/JSON/tool/reasoning/vision capabilities as applicable, egress hosts, and `low`, `standard`, `high`, `critical` models.

Model defaults were verified against provider documentation at implementation time, but provider catalogs change. Production operators should pin/override model variables based on their account and region.

Reference catalogs: [OpenAI](https://developers.openai.com/api/docs/models), [Claude](https://platform.claude.com/docs/en/about-claude/models/overview), [Gemini](https://ai.google.dev/gemini-api/docs/models), [DeepSeek](https://api-docs.deepseek.com/quick_start/pricing/), [Moonshot](https://platform.moonshot.ai/docs/guide/prompt-best-practice), [Alibaba Model Studio](https://help.aliyun.com/en/model-studio/models), [Z.AI](https://docs.z.ai/guides/overview/overview), [MiniMax](https://platform.minimax.io/docs/api-reference/api-overview), and [Ollama](https://registry.ollama.com/library).

## Routing

A request declares purpose, messages, budget/project ID, actor role, optional explicit preference, complexity, and required capabilities. Candidate selection applies explicit preference, role order, global order, availability, capability, and model-prefix validation.

Purpose can derive complexity—for example, heartbeat compression is low, ordinary action selection is standard, code/infrastructure review is high, and conflict/safety/DoD judgment is critical. The infrastructure agent persists the initial provider/model assignment, while the gateway can fall back at call time.

## Gateway safety and reliability

- Recognized credentials/private keys are replaced before prompt hashing/transmission.
- Custom API base host must match the provider allowlist; remote production bases require HTTPS.
- Daily/monthly budgets are atomically reserved in Dragonfly and settled to actual cost.
- A semaphore caps simultaneous calls.
- Each failed provider increments a circuit; open circuits recover after a TTL.
- Attempts are bounded and use jittered backoff.
- JSON-object requests must parse as JSON objects.
- An append-only `provider_call_intents` row is persisted before external egress; prompt/response hashes, provider/model, usage, cost, latency, redaction status, and error types are then linked in append-only `provider_calls` rows.

No keys or response bodies are stored in provider audit rows.

Per-criterion code/security review remains routed through this gateway. Reviewer actors use strict structured verdict schemas and bind prompt content to the artifact's exact committed-diff SHA-256 and length; oversized, mismatched, provider, and parse uncertainty becomes append-only `INCONCLUSIVE` evidence and is never cached. A smaller local semaphore and bounded LRU/single-flight cache is keyed by review kind, criterion hash, subject commit, artifact checksum, content hash, and risk where applicable. Only successful exact-snapshot decisions are reusable, and canceling one waiting caller cannot cancel the shared provider computation.

## Embeddings

The configured embedding model also uses the provider gateway. Returned dimension must equal `AGENTOS_EMBEDDING_DIMENSION`. Changing dimension requires a compatible new Milvus collection. If the embedding provider is unavailable, normal long-term and lexical retrieval remain available.
