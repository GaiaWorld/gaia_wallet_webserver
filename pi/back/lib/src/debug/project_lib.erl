%% 
%% @doc 项目公共函数库
%%


-module(project_lib).

%% ====================================================================
%% Include files
%% ====================================================================
-define(INIT_CFG, init).
-define(START_CFG, start).
-define(SOCKET_CFG, zm_net_server).
-define(TCP_PORT_CFG, zm_session).
-define(HTTP_PORT_CFG, zm_http).
-define(LOG_SERVER_CFG, zm_logger).
-define(LOG_CFG, zm_log).
-define(TABLE_CFG, zm_db).
-define(EVENT_CFG, zm_event).
-define(TIMER_CFG, zm_dtimer).
-define(RES_CFG, zm_resource).
-define(PROCESS_CFG, zm_pid_dist).
-define(SAMPLE_CFG, zm_sample).
-define(UNIQUE_CFG, zm_unique_int).
-define(COMMUNICATOR_CFG, zm_communicator).
-define(USER_CFG, zm_config).

-define(CODE, zm_code).
-define(RES, zm_res_loader).

%% ====================================================================
%% API functions
%% ====================================================================
-export([timer_to_string/1, get_config_item/1, parse_socket_cfg/1, parse_tcp_cfg/1, parse_http_cfg/1, parse_log_server_cfg/1, 
		 parse_log_cfg/1, parse_table_cfg/1, parse_event_cfg/1, parse_timer_cfg/1]).

%%
%%将定时器时间描述转换为字符串
%%
timer_to_string({Date, Time}) ->
	{parse_date(Date), Time}.

%%
%%获取指定配置项
%%
get_config_item(?SOCKET_CFG) ->
	[ports];
get_config_item(?TCP_PORT_CFG) ->
	[cmd, access, access_type, timeout];
get_config_item(?HTTP_PORT_CFG) ->
	[cmd];
get_config_item(?LOG_SERVER_CFG) ->
	[log_server, type];
get_config_item(?LOG_CFG) ->
	[level, source, type];
get_config_item(?TABLE_CFG) ->
	[table, copy];
get_config_item(?EVENT_CFG) ->
	[source, event, type, timeout];
get_config_item(?TIMER_CFG) ->
	[source, type, time, timeout];
get_config_item(_) ->
	[].

  
%%
%%解析socket配置
%%
parse_socket_cfg([{zm_tcp_server, _, [Project, LocalPorts, _, _, SSL, AcceptCount, MaxConnectCount, AddressFilter, {_, _, _} = Con]}]) ->
	[
		{project, Project},
		{ports, term_to_string(LocalPorts)},
		{ssl, term_to_string(SSL)},
		{instance_count, AcceptCount},
		{connect_count, MaxConnectCount},
		{filter, term_to_string(AddressFilter)},
		{connect, term_to_string(Con)}
	];
parse_socket_cfg([{_, _, _}]) ->
	[].

%%
%%解析tcp接口配置
%%
parse_tcp_cfg([{Src, Cmd} , Handles, ErrorHandle, Access, Type]) ->
	{ExcType, Timeout}=if
		Type > 0 ->
			{sequence, Type};
		Type < 0 ->
			{concurrent, erlang:abs(Type)};
		true ->
			{sync, 0}
	end,
	[
	 {source, Src},
	 {cmd, Cmd},
	 {handles, term_to_string(Handles)},
	 {error_handle, term_to_string(ErrorHandle)},
	 {access, begin
				case Access of
					0 ->
						send;
					1 ->
						request
				end
			  end
	 },
	 {access_type, ExcType},
	 {timeout, Timeout}
	].

%%
%%解析http接口配置
%%
parse_http_cfg([{Src, Cmd}, Handles, ErrorHandle]) ->
	[
	 {source, Src},
	 {cmd, Cmd},
	 {handles, term_to_string(Handles)},
	 {error_handle, term_to_string(ErrorHandle)}
	].

%%
%%解析日志服务配置
%%
parse_log_server_cfg([Name, Mod, L]) ->
	[{log_server, Name}, {type, Mod}|L].

%%
%%解析日志配置
%%
parse_log_cfg([{Level, Src, Type}, Filter]) ->
	[
	 {level, Level},
	 {source, Src},
	 {type, Type},
	 {filter, term_to_string(Filter)}
	].
	
%%
%%解析表配置
%%
parse_table_cfg([Table, Duplicate, L]) ->
	[{table, Table}, {copy, Duplicate}|[begin 
											case Key of
												interval ->
													{Key, parse_table_time(Value)};
												snapshot ->
													{Key, parse_table_time(Value)};
												format ->
													{Key, term_to_string(Value)};
												_ ->
													KV
											end
										end || KV = {Key, Value} <- L]].

%%
%%解析事件处理器配置
%%
parse_event_cfg([{Src, Event}, Handle, Type]) ->
	{ExcType, Timeout}=if
		Type > 0 ->
			{sync, Type};
		true ->
			{async, Type}
	end,
	[
	 {source, Src},
	 {event, Event},
	 {handle, term_to_string(Handle)},
	 {type, ExcType},
	 {timeout, Timeout}
	].

%%
%%解析定时器配置
%%
parse_timer_cfg([{Src, Type}, Handle, {time, Time, Timeout}]) ->
	[
	 {source, Src},
	 {type, Type},
	 {handle, term_to_string(Handle)},
	 {time, Time},
	 {timeout, Timeout}
	];
parse_timer_cfg([{Src, Type}, Handle, {date, Date, Time, Conflict, Timeout}]) ->
	[
	 {source, Src},
	 {type, Type},
	 {handle, term_to_string(Handle)},
	 {date, term_to_string(Date)},
	 {time, term_to_string(Time)},
	 {conflict, term_to_string(Conflict)},
	 {timeout, Timeout}
	].

%% ====================================================================
%% Internal functions
%% ====================================================================

term_to_string(Term) ->
	lists:flatten(io_lib:format("~p", [Term])).

parse_date({y, Y}) ->
	{yearly, Y};
parse_date({m, M}) ->
	{monthly, M};
parse_date({w, W}) ->
	{weekly, W};
parse_date({d, D}) ->
	{everyday, D};
parse_date({dw, DW}) ->
	{weekday, DW};
parse_date({wy, WY}) ->
	{week_of_year, WY};
parse_date({dy, DY}) ->
	{day_of_year, DY}.

parse_table_time({any, Day}) ->
	{dw, Day};
parse_table_time({Date, Hour}) ->
	{parse_date(Date), {Hour, 0, 0}}.

