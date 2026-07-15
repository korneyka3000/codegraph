"""M3 T3/T4: AST-чанкер + контекстная аугментация. `splitter.chunk_file` режет
FileFacts на symbol-aligned, size-bounded ChunkRec — retrieval-единицу M3 (chunk_id ->
symbol_id -- мост "поиск -> граф"). `augment.build_header`/`augment_text`/
`fill_headers` строят graph-aware текстовый заголовок поверх staged узлов/рёбер для
каждого чанка (embedder/fulltext-контекст, см. augment.py)."""
