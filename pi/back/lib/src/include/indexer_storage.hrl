%状态记录
-record(state, {
				path,
				inverted_count,
				merge_map_handler,
				inverted_handler,
				inverted_storage,
				dict_count,
				dict_map_handler,
				dict_handler,
				dict_storage,
				lock,
				collate_time,
				wait_queue
	}).
-define(QUEUE_COLLATE, queue_collate).
-define(INVERTED_HIBERNATE_TIMEOUT, 3000).
-define(STORAGE_FLAG, storage).
-define(COLLATE_TIME, "collate_time").
-define(DEFAULT_COLLATE_TIME, 500).
-define(INDEX_DIR_PATH, "dir_path").
-define(INVERTED_COUNT, "inverted_count").
-define(DEFAULT_INVERTED_COUNT, 8).
-define(MERGE_MAP_HANDLER, "merge_map_handler").
-define(INVERTED_HANDLER, "inverted_handler").
-define(INVERTED_NAME, "inverted_name").
-define(DICT_COUNT, "dict_count").
-define(DEFAULT_DICT_COUNT, 2).
-define(DICT_HANDLER, "dict_handler").
-define(DICT_NAME, "dict_name").
-define(DEFAULT_HANDLER, none).
-define(MAP_REPLY, map_reply).
-define(LOCK_UNUSED, 0).
-define(DICT_FILE_PREFIX, "_dict_").
-define(INVERTED_FILE_PREFIX, "_inverted_").
-define(INDEX_FILE_SPLIT_CHAR, "_").
-define(DICT_TABLE(Number), list_to_atom(lists:concat([?MODULE, "_dict_", Number]))).
-define(INVERTED_TABLE(Number), list_to_atom(lists:concat([?MODULE, "_inverted_", Number]))).


