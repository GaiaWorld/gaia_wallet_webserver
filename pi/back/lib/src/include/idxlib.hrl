-define(KEY_INDEX, 0).
-define(DEFAULT_FILE_SPLIT, "_").
-define(IDX_ALLOC_CODE, 1).
-define(IDX_FREE_CODE, 0).
-record(wlock, {
					from,
					type,
					key,
					value,
					vpoint,
					vsn,
					time,
					index,
					writer,
					handle,
					frees,
					codes,
					vfree,
					root_points,
					points,
					rollback,
					idxl,
					idxl_
	}).