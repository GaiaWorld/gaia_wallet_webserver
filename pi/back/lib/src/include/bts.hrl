-record(bts_context, {
					sid
}).
-define(BTS_HIBERNATE_TIMEOUT, 3000).
-define(SCOPE_OPT, scope).
-define(PRIVATE_SCOPE, 16#abcd).
-define(PROTECTED_SCOPE, 16#dcba).
-define(PUBLIC_SCOPE, 16#0).
-define(DEFAULT_MAX_DATA_SIZE, 16#100000000000).
-define(EMPTY, undefined).
-define(ASYN_WAIT, wait).
-define(READ_FLAG, r).
-define(WRITE_FLAG, w).
-define(IS_FIX_LENGTH, is_fix).
-define(DEFAULT_FIX_LENGTH, true).
-define(BTS_PATH, bts_path).
-define(DEFAULT_BTS_PATH(Name), lists:concat(["./", Name, "/"])).
-define(IS_CHECKSUM, checksum).
-define(DEFAULT_CHECKSUM, true).