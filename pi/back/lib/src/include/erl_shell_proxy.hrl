-define(DEFAULT_MAX_CONSOLE_PER_NODE, 1).
-define(DEFAULT_MAX_SCRIPT_PER_NODE, 1).

-record(state, {
					auth_table,
					max_console_per_node=?DEFAULT_MAX_CONSOLE_PER_NODE,
					max_script_per_node=?DEFAULT_MAX_SCRIPT_PER_NODE,
					shell_table,
					shell_pid_table,
					shell_index_table,
					console_time,
					script_time,
					timer
			}).
-define(SHELL_PROXY_HIBERNATE_TIMEOUT, 30000).

-define(AUTH_TABLE_FLAG, auth_table).
-define(MAX_CONSOLE_PER_NODE_FLAG, max_console_per_node).
-define(MAX_SCRIPT_PER_NODE_FLAG, max_script_per_node).

-define(CHECK_TIME, check_time).
-define(DEFAULT_CHECK_TIME, 1000).
-define(CHECK_TIMEOUT, check_timeout).
-define(CONSOLE_RESPONSE_TIMEOUT, console_response_timeout).
-define(DEFAULT_CONSOLE_RESPONSE_TIMEOUT, 30000).
-define(SCRIPT_RESPONSE_TIMEOUT, script_response_timeout).
-define(DEFAULT_SCRIPT_RESPONSE_TIMEOUT, 30000).
-define(DEFAULT_RPC_TIMEOUT, 3000).

-define(DEFAULT_CMD_MAX_LENGTH, 4096).
-define(DEFAULT_SHELL_HEAD(Node), lists:concat(["Eshell ", Node, "\n"])).

-define(DEFAULT_MIN_TIMEOUT, 1000).

-define(CONSOLE_TYPE, 0).
-define(SCRIPT_TYPE, 1).