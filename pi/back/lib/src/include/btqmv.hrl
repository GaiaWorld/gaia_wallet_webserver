-define(RW_FILE_TYPE, rw).
-record(btqmv_context, {
						self,
						btsmv
}).
-define(BTQMV_HIBERNATE_TIMEOUT, 3000).
-define(INDEX, idx).
-define(KEY_INDEX, 0).
-define(VALUE_INDEX, 1).
-define(INIT_VSN, 1).
-define(ROOT_REF_KEY, root_ref).
-define(DEREF, 0).
-define(INIT_REF, 1).
-define(INTERRUPT_STATE, interrupt).
-define(DATA_ASYN_WAIT, data_wait).

