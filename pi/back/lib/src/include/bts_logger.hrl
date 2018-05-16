%%进程上下文记录
-record(bts_logger_context, {
							self,
							sync_request,
							dat_cache,
							idx_cache,
							log_file,
							snapshoot_file,
							log_fd,
							snapshoot_fd,
							serializer,
							sync_level,
							buffer_size,
							sync_timer,
							max_error_times,
							error_count,
							btpm,
							btsm,
							commit_uid
}).
-define(BTS_LOGGER_HIBERNATE_TIMEOUT, 3000).
-define(KEY_INDEX, 0).
-define(VALUE_INDEX, 1).
-define(DAT_FREE_BLOCKS, dat_free_blocks).
-define(IDX_FREE_BLOCKS, idx_free_blocks).
-define(LOG_EXT, ".log").
-define(SNAPSHOOT_EXT, ".snap").
-define(RESTORE_EXT, ".restore").
-define(EMPTY_FD, undefined).
-define(SYNC_LEVEL, sync_level).
-define(AUTO_SYNC_LEVEL, auto).
-define(DEFAULT_SYNC_LEVEL, {16#1000, 100}).
-define(ERVER_SYNC_LEVEL, erver).
-define(LOG_SERIALIZER, log_serializer).
-define(DEFAULT_LOG_SERIALIZER, {bts_logger, []}).
-define(MAX_ERROR_TIMES, max_error_times).
-define(DEFAULT_MAX_ERROR_TIMES, 10).
