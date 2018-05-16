%%进程上下文记录
-record(btsmv_context, {
						self,
						path,
						checksum,
						dat_reader,
						idx_reader,
						persistence_id,
						idx_persistence_id,
						buffer,
						write_queue,
						wait_frees,
						cache_capacity,
						persistence_factor,
						wait_frees_timer,
						persistence_timeout,
						persistence_timer,
						collate_timeout,
						collate_timer,
						btqmv,
						logger,
						btpm,
						commit_uid
}).
-define(BTSMV_HIBERNATE_TIMEOUT, 3000).
-define(WRITE_LOCK, write_lock).
-define(INDEX, idx).
-define(KEY_INDEX, 0).
-define(VALUE_INDEX, 1).
-define(INIT_VSN, 1).
-define(INIT_UID, 0).
-define(DAT_INDEX, 0).
-define(IDX_INDEX, 1).
-define(R_FILE_TYPE, r).
-define(DAT_EXT, ".dat").
-define(IDX_EXT, ".idx").
-define(DATA_MAX_STORAGE_SIZE, 16#100000000000).
-define(INTERRUPT_STATE, interrupt).
-define(IS_IDX, is_idx).
-define(DEFAULT_IS_IDX, false).
-define(DAT_CACHE_ARGS, dat_cache).
-define(DEFAULT_DAT_CACHE_ARGS, {bts_cache, map, 64, 64, 300000}).
-define(IDX_CACHE_ARGS, idx_cache).
-define(DEFAULT_IDX_CACHE_ARGS, {bts_cache, order, 64, 64, 300000}).
-define(DAT_ENTRY_BYTE_SIZE, dat_entry_size).
-define(IDX_ENTRY_BYTE_SIZE, idx_entry_size).
-define(DEFAULT_IDX_ENTRY_BYTE_SIZE, 16#1).
-define(DEREF, 0).
-define(DEFAULT_WAIT_FREE_TIMEOUT, 10).
-define(DEFAULT_TIME_FREE_COUNT, 100).
-define(COLLATE_TIMEOUT, collate_timeout).
-define(DEFALUT_COLLATE_TIMEOUT, 360000000). %暂时不整理,整理需要有加载支持
-define(CACHE_CAPACITY, cache_capacity).
-define(DEFAULT_CACHE_CAPACITY, 67108864).
-define(PERSISTENT_CACHE_FACTOR, persistent_factor).
-define(DEFAULT_PERSISTENT_CACHE_FACTOR, 0.9).
-define(PERSISTENT_CACHE_TIMEOUT, persisten_timeout).
-define(DEFAULT_PERSISTENT_CACHE_TIMEOUT, 300000).
-define(DATA_ASYN_WAIT, data_wait).