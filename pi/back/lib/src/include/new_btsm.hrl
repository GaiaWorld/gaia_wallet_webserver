-define(DEFAULT_SAVE_PATH, "./").
-define(DEFAULT_SNAPSHOT_EXT, ".snap").
-define(DEFAULT_SUP_EXT, ".sup").
-define(DEFAULT_DAT_EXT, ".dat").
-define(DEFAULT_COMPRESS, none).
-define(DEFAULT_CHECKSUM, true).
-define(RW_FILE_TYPE, rw).
-define(R_FILE_TYPE, r).
-define(W_FILE_TYPE, w).
-define(DEFAULT_MAX_DATA_SIZE, 16#100000000000).
-define(DEFAULT_SYNC_FLAG, true).
-define(SUP_FILE_INIT_INDEX, 0).
-define(DAT_ALLOC_CODE, 11).
-define(DAT_FREE_CODE, 10).
-define(SUP_LIMIT_SIZE, 16#100000000).
-define(INIT_COMMIT_LOC, 0).
-define(INIT_COMMIT_UID, 0).
-define(FREED, -1).
%%进程上下文
-record(state, {
					path,
					sup_prefix,
					sup_compress,
					no_sync_sup,
					sup_proc,
					is_new_sup,
					new_sup_proc,
					idx_checksum,
					idxes,
					idxm,
					dat_compress,
					dat_checksum,
					no_sync_dat,
					dat_handle,
					sup_index,
					sup_loc,
					commit_uid,
					reqs
	}).
-define(BTSM_HIBERNATE_TIMEOUT, 3000).
-define(DEFAULT_REQUEST_SIZEOUT, 16#2f08).
-define(DEFAULT_REQUEST_TIMEOUT, 10000).
-define(SAVE_PATH_KEY, save_path).
-define(SAVE_FILE_KEY, save_file).
-define(SUP_COMPRESS, sup_compress).
-define(SUP_CHECKSUM, sup_checksum).
-define(IDX_COMPRESS, idx_compress).
-define(IDX_CHECKSUM, idx_checksum).
-define(DAT_COMPRESS, dat_compress).
-define(DAT_CHECKSUM, dat_checksum).
-define(REQS_SIZEOUT, sizeout).
-define(REQS_TIMEOUT, timeout).
-define(NO_SYNC_DATA_FLAG, no_sync_dat).
-define(NO_SYNC_SUP_FLAG, no_sync_sup).

