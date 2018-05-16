-define(DEFAULT_PSIZE, 16#10).
-define(DEFAULT_MIN_ORDER, 0).
-define(DEFAULT_MAX_ORDER, 18).
-define(DEFAULT_STORAGE_SIZE, 16#80000000).
-define(MAX_PSIZE, 16#40).
-define(MAX_STORAGE_SIZE, 16#100000000000).
-define(MAX_SIZE_PER_ALLOC, 16#7fffffff).	
-record(state, {
				owner=undefined,
				psize=?DEFAULT_PSIZE,
				min_order=?DEFAULT_MIN_ORDER,
				max_order=?DEFAULT_MAX_ORDER,
                free_table=undefined
			}).
-define(POW(X), (1 bsl (X))).
-define(FREE_AREA_TABLE(Owner), list_to_atom(lists:concat(["free_area@", Owner]))).
-define(BSYS_HIBERNATE_TIMEOUT, 3000).
-define(STORAGE_SIZE_KEY, s_size).
-define(PSIZE_KEY, p_size).
-define(PBOF, -1).
-define(PINIT, 0).
-define(FREE_FLAG, 0).
-define(BUSY_FLAG, 1).
-define(ZERO_FREE, 0).
-define(INIT_FREE, 1).
-define(EMPTY_AREA, undefined).
-define(EMPTY_BLOCK, undefined).
-define(MIN_PSIZE, 16#1).