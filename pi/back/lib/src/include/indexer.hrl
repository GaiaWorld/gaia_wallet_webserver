%状态记录
-record(state, {
				doc_handler,
				cur, 
				max, 
				start_time,
				is_k_gram,
				cache_timeout,
				cache_capacity,
				cache,
				self
  }).
-define(CLOCK, indexer_clock).
-define(CLOCK_FREQUENCY, 60000).
-define(CACHE_COLLATE, cache_collate).
-define(INDEXER_HIBERNATE_TIMEOUT, 3000).
-define(MAX_UINT32_LENGTH, 16#ffffffff).
-define(UINT32_START, 1).
-define(INIT_CACHE_SIZE, 0).
-define(INIT_CACHE, []).
-define(DOC_HANDLE, doc_handle).
-define(INDEX_HANDLER_FLAG, index_doc).
-define(BATCH_INDEX_HANDLER_FLAG, index_docs).
-define(TOKEN_HANDLER, "token_handler").
-define(LANGUAGE_HANDLER, "language_handler").
-define(DESTORY_HANDLER, "destory_handler").
-define(DEFAULT_HANDLER, none).
-define(CACHE_TIMEOUT, "cache_timeout").
-define(DEFAULT_CACHE_TIMEOUT, 5000).
-define(CACHE_CAPACITY, "cache_capacity").
-define(DEFAULT_CACHE_CAPACITY, 128 * 1024).
-define(IS_K_GRAM, "is_k_gram").
-define(DEFAULT_K_GRAM, false).
-define(DOC_SYS_TIME_KEY, '$time').
-define(REPLY_KEY, indexer_reply).
-define(BATCH_REPLY_KEY, indexer_batch_reply).
-define(MAP_REPLY, map_reply).
-define(ERROR_OUT_OF_DOCID, out_of_docid).

