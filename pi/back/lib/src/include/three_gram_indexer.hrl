%状态记录
-record(state, {
				path,
				index_count,
				lock,
				collate_time,
				wait_queue
	}).
-define(QUEUE_COLLATE, queue_collate).
-define(THREE_GRAM_INVERTED_HIBERNATE_TIMEOUT, 3000).
-define(K_GRAM_FLAG, k_gram).
-define(INDEX_DIR_PATH, "dir_path").
-define(INDEX_COUNT, "index_count").
-define(DEFAULT_INDEX_COUNT, 1).
-define(KGRAM_NAME, "kgram_name").
-define(COLLATE_TIME, "collate_time").
-define(DEFAULT_COLLATE_TIME, 500).
-define(MAP_REPLY, map_reply).
-define(LOCK_UNUSED, 0).
-define(K_GRAM_FILE_PREFIX, "_kgram_").
-define(INDEX_FILE_SPLIT_CHAR, "_").
-define(K_GRAM_TABLE(Number), list_to_atom(lists:concat([?MODULE, "_kgram_", Number]))).
-define(RETRIEVALER_WILDCARD, $$).