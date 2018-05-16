%状态记录
-record(state, {
				path,
				index_count,
				keys,
				traverse_storage,
				collate_time,
				wait_queue
	}).
-define(QUEUE_COLLATE, queue_collate).
-define(TRAVERSE_HIBERNATE_TIMEOUT, 3000).
-define(TRAVERSE_FLAG, traverse).
-define(INDEX_DIR_PATH, "dir_path").
-define(INDEX_COUNT, "index_count").
-define(DEFAULT_INDEX_COUNT, 1).
-define(KEYS, "keys").
-define(TRAVERSE_NAME, "traverse_name").
-define(COLLATE_TIME, "collate_time").
-define(DEFAULT_COLLATE_TIME, 500).

