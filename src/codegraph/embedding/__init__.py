"""M3 T5: pluggable Embedder implementations. `base.Embedder` is the structural
Protocol every provider satisfies (`model_id`, `dim`, `embed_batch`, `embed_query`);
`factory.make_embedder(cfg: EmbeddingConfig)` dispatches on `cfg.provider` to
`local.LocalEmbedder` / `openai_emb.OpenAIEmbedder` / `voyage.VoyageEmbedder`.
`fake.FakeEmbedder` is a dependency-free, deterministic stand-in for unit tests and
any degraded-default use that needs an `Embedder` without a real model/API key.

Not wired into the indexing pipeline yet -- that's M3 T6 (`chunk+embed` stage)."""
