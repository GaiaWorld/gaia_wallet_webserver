%状态记录
-record(state, {
				path,
				kgram_index_count,
				dict_count,
				inverted_count,
				receives,
				collate_time,
				cache_capacity,
				cache
	}).
-define(CACHE_COLLATE, cache_collate).
-define(RETRIEVAL_HIBERNATE_TIMEOUT, 3000).
-define(MAPREDUCE_REPLY, mapreduce_reply).
-define(RETRIEVAL_FLAG, retrieval).
-define(RETRIEVAL_OK_FLAG, retrieval_ok).
-define(INDEX_DIR_PATH, "dir_path").
-define(KGREM_INDEX_COUNT, "kgrem_index_count").
-define(DEFAULT_KGREM_INDEX_COUNT, 1).
-define(DICT_COUNT, "dict_count").
-define(DEFAULT_DICT_COUNT, 2).
-define(INVERTED_COUNT, "inverted_count").
-define(DEFAULT_INVERTED_COUNT, 8).
-define(COLLATE_TIME, "collate_time").
-define(DEFAULT_COLLATE_TIME, 60 * 1000).
-define(CACHE_TIMEOUT, "cache_timeout").
-define(DEFAULT_CACHE_TIMEOUT, 10 * 60 * 1000).
-define(CACHE_CAPACITY, "cache_capacity").
-define(DEFAULT_CACHE_CAPACITY, 10 * 1024 * 1024).
-define(MAX_WEEK_OF_YEAR, 52).
-define(K_GRAM_TABLE(Owen, Week, Hash), list_to_atom(lists:concat([?MODULE, "_kgram_", Week, "_", Hash, pid_to_list(Owen)]))).
-define(DICT_TABLE(Owen, Week, Hash), list_to_atom(lists:concat([?MODULE, "_dict_", Week, "_", Hash, pid_to_list(Owen)]))).
-define(INVERTED_TABLE(Owen, Week, Hash), list_to_atom(lists:concat([?MODULE, "_inverted_", Week, "_", Hash, pid_to_list(Owen)]))).
-define(DEFAULT_NULL_DOCID, -1).
-define(AND_STACK_KEY, and_stack).
-define(RESULT_FLAG, result).

