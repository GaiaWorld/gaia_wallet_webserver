%%进程上下文记录
-record(btpm_context, {
						self,
						sup_file,
						checksum,
						dat_cache,
						dat_alloter,
						idx_cache,
						idx_alloter,
						serialize,
						deserialize,
						sup_fd,
						dat_fd,
						idx_fd,
						logger,
						btsm
}).
-define(BTPM_HIBERNATE_TIMEOUT, 3000).
-define(DAT_INDEX, 0).
-define(IDX_INDEX, 1).
-define(COMMITED_EXT, ".commited").
-define(SUP_EXT, ".sup").
-define(DAT_EXT, ".dat").
-define(IDX_EXT, ".idx").
-define(IS_IDX, is_idx).
-define(DEFAULT_IS_IDX, false).
-define(SERIALIZER, serializer).
-define(DEFAULT_SERIALIZER, {btpm, []}).
-define(DESERIALIZE, deserialize).
-define(DEFAULT_DESERIALIZE, {btpm, []}).
-define(DEREF, 0).