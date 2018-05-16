%% @author lenovo
%% @doc js工作进程


-module(z_js_worker).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Include files
%% ====================================================================
%根据操作系统获取文件监控模块
-define(GET_V8C_NAME(), case os:type() of
							{win32, _} ->
								"v8c";
							_ ->
								"libv8c"
						end).
-define(V8C_NAME, ?GET_V8C_NAME()).
-define(V8C_MOD, list_to_atom(?V8C_NAME)).

%%空
-define(NIL, nil).
%%成功
-define(OK, "ok").
%%错误
-define(ERROR, "error").
%%错误提示
-define(ERROR_DESC, "error").
%%错误文件
-define(ERROR_FILE, "file").
%%错误文件
-define(ERROR_LINE, "line").
%%错误文件
-define(ERROR_SOURCE, "source").
%%错误文件
-define(ERROR_LOCATION, "location").
%%错误文件
-define(ERROR_STACKTRACE, "stacktrace").

%%异步返回关键字
-define(ASYN_REPLY_KEY, "ok").
%%异步引用关键字
-define(ASYN_REF_KEY, "ref").
%%js异步调用关键字
-define(ASYN_CALL_KEY, "$erl_asyn_call").

%%进程休眠超时时长
-define(HIBERNATE_TIMEOUT, 30000).


%% ====================================================================
%% API functions
%% ====================================================================
-export([spawn_link/3, new/1, new/2, get/3, get/4, eval/3, hygienic_eval/3, call/5, hygienic_call/5, call/4, destroy/2]).

%%
%%启动js工作进程
%%
spawn_link(Scheduler, VM, Root) ->
	gen_server:start_link({local, z_lib:to_atom([?MODULE, erlang:unique_integer([positive])])}, ?MODULE, {Scheduler, VM, Root}, []).

%%
%%构建一个空环境
%%
new(Worker) ->
	gen_server:call(Worker, {new_context, ?NIL}).

%%
%%构建一个指定环境的复制
%%
new(Worker, Context) when is_integer(Context), Context > 0 ->
	gen_server:call(Worker, {new_context, Context}).

%%
%%获取指定环境的指定全局对象
%%
get(Worker, Context, Chain) when is_integer(Context), is_list(Chain), Context > 0, length(Chain) > 0 ->
	gen_server:call(Worker, {get, Context, Chain}).

%%
%%获取指定环境的指定全局对象的指定成员
%%
get(Worker, Context, ObjName, MemberName) when is_integer(Context), is_list(ObjName), is_list(MemberName), Context > 0, length(ObjName) > 0, length(MemberName) > 0 ->
	gen_server:call(Worker, {get, Context, ObjName, MemberName}).

%%
%%在指定环境中执行指定脚本
%%
eval(Worker, Context, Script) when is_integer(Context), is_list(Script), Context > 0, length(Script) > 0 ->
	gen_server:call(Worker, {eval, Context, Script}).

%%
%%在指定环境的临时复制中执行指定脚本
%%
hygienic_eval(Worker, Context, Script) when is_integer(Context), is_list(Script), Context > 0, length(Script) > 0 ->
	gen_server:call(Worker, {hygienic_eval, Context, Script}).

%%
%%在指定环境中调用函数
%%
call(Worker, Context, ObjName, FunName, ArgJson) 
  when is_integer(Context), is_list(ObjName), is_list(FunName), is_list(ArgJson), Context > 0, length(ObjName) > 0, length(FunName) > 0 ->
	gen_server:call(Worker, {call, Context, ObjName, FunName, ArgJson}).

%%
%%在指定环境的临时复制中调用函数
%%
hygienic_call(Worker, Context, ObjName, FunName, ArgJson) 
  when is_integer(Context), is_list(ObjName), is_list(FunName), is_list(ArgJson), Context > 0, length(ObjName) > 0, length(FunName) > 0 ->
	gen_server:call(Worker, {hygienic_call, Context, ObjName, FunName, ArgJson}).

%%
%%在指定环境中调用函数
%%
call(Worker, Context, Chain, ArgJson) 
  when is_integer(Context), is_list(Chain), is_list(ArgJson), Context > 0, length(Chain) > 0 ->
	gen_server:call(Worker, {call, Context, Chain, ArgJson}).

%%
%%移除指定环境
%%
destroy(Worker, Context) when is_integer(Context), Context > 0 ->
	gen_server:call(Worker, {destroy, Context}).


%% ====================================================================
%% Behavioural functions
%% ====================================================================
-record(state, {scheduler, vm, root, count, froms}).

%% init/1
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:init-1">gen_server:init/1</a>
-spec init(Args :: term()) -> Result when
	Result :: {ok, State}
			| {ok, State, Timeout}
			| {ok, State, hibernate}
			| {stop, Reason :: term()}
			| ignore,
	State :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
init({Scheduler, VM, Root}) ->
	erlang:process_flag(scheduler, Scheduler), %将工作进程绑定到指定的调度器上
	{ok, #state{scheduler = Scheduler, vm = VM, root = Root, count = 0, froms = maps:new()}}.


%% handle_call/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_call-3">gen_server:handle_call/3</a>
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
	Result :: {reply, Reply, NewState}
			| {reply, Reply, NewState, Timeout}
			| {reply, Reply, NewState, hibernate}
			| {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason, Reply, NewState}
			| {stop, Reason, NewState},
	Reply :: term(),
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity,
	Reason :: term().
%% ====================================================================
handle_call({new_context, ?NIL}, _, #state{vm = VM, root = Root} = State) ->
	Reply={ok, Context}=(?V8C_MOD):v8c_new_context(VM, Root),
	{ok, VCHandle}=(?V8C_MOD):v8c_new_handle(VM, Context),
	ok=(?V8C_MOD):v8c_bind_process(self(), VCHandle),
	{reply, Reply, State, ?HIBERNATE_TIMEOUT};
handle_call({new_context, Context}, _, #state{vm = VM} = State) ->
	Reply={ok, ChildContext}=(?V8C_MOD):v8c_new_context(VM, Context),
	{ok, VCHandle}=(?V8C_MOD):v8c_new_handle(VM, ChildContext),
	ok=(?V8C_MOD):v8c_bind_process(self(), VCHandle),
	{reply, Reply, State, ?HIBERNATE_TIMEOUT};
handle_call({get, Context, Chain}, _, #state{vm = VM} = State) ->
	{reply, handle_reply((?V8C_MOD):v8c_chain_get(VM, Context, Chain)), State, ?HIBERNATE_TIMEOUT};
handle_call({get, Context, ObjName, MemberName}, _, #state{vm = VM} = State) ->
	{reply, handle_reply((?V8C_MOD):v8c_get(VM, Context, ObjName, MemberName)), State, ?HIBERNATE_TIMEOUT};
handle_call({eval, Context, Script}, From, #state{vm = VM, count = Count, froms = Froms} = State) ->
	(?V8C_MOD):v8c_eval(VM, Context, Count, Script),
	{noreply, State#state{count = Count + 1, froms = maps:put(Count, From, Froms)}, ?HIBERNATE_TIMEOUT};
handle_call({hygienic_eval, Context, Script}, _, #state{vm = VM} = State) ->
	{reply, handle_reply((?V8C_MOD):v8c_hygienic_eval(VM, Context, Script)), State, ?HIBERNATE_TIMEOUT};
handle_call({call, Context, ObjName, FunName, ArgJson}, From, #state{vm = VM, count = Count, froms = Froms} = State) ->
	(?V8C_MOD):v8c_call(VM, Context, Count, ObjName, FunName, ArgJson),
	{noreply, State#state{count = Count + 1, froms = maps:put(Count, From, Froms)}, ?HIBERNATE_TIMEOUT};
handle_call({hygienic_call, Context, ObjName, FunName, ArgJson}, _, #state{vm = VM} = State) ->
	{reply, handle_reply((?V8C_MOD):v8c_hygienic_call(VM, Context, ObjName, FunName, ArgJson)), State, ?HIBERNATE_TIMEOUT};
handle_call({call, Context, Chain, ArgJson}, From, #state{vm = VM, count = Count, froms = Froms} = State) ->
	(?V8C_MOD):v8c_chain_call(VM, Context, Count, Chain, ArgJson),
	{noreply, State#state{count = Count + 1, froms = maps:put(Count, From, Froms)}, ?HIBERNATE_TIMEOUT};
handle_call({destroy, Context}, _, State) ->
	{reply, (?V8C_MOD):v8c_destroy_context(Context), State, ?HIBERNATE_TIMEOUT};
handle_call(_, _, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.


%% handle_cast/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2">gen_server:handle_cast/2</a>
-spec handle_cast(Request :: term(), State :: term()) -> Result when
	Result :: {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason :: term(), NewState},
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_cast(_, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.


%% handle_info/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_info-2">gen_server:handle_info/2</a>
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
	Result :: {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason :: term(), NewState},
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_info([${, $o, $k, $ , $:, $ |R] = AsynReply, #state{froms = Froms} = State) ->
	%处理异步返回
	NewFroms = try zm_json:decode(AsynReply) of
		Data -> 
			handle_asyn_reply(Froms, Data)
	catch 
		_:_ ->
			case R of
				[${, $e, $r, $r, $o, $r, $ , $:, $ |E] ->
					[H, T]=z_lib:split(E, ", $ref$ : "),
					[$}|T0]=lists:reverse(T),
					Count=list_to_integer(lists:reverse(T0)),
					case maps:get(Count, Froms, ?NIL) of
						?NIL ->
							Froms;
						From ->
							[$}|T1]=lists:reverse(H),
							z_lib:reply(From, {error, lists:reverse(T1)}),
							maps:remove(Count, Froms)
					end;
				_ ->
					[H, T]=z_lib:split(R, ", $ref$ : "),
					[$}|T0]=lists:reverse(T),
					Count=list_to_integer(lists:reverse(T0)),
					case maps:get(Count, Froms, ?NIL) of
						?NIL ->
							Froms;
						From ->
							z_lib:reply(From, {ok, H}),
							maps:remove(Count, Froms)
					end
			end
	end,
	{noreply, State#state{froms = NewFroms}, ?HIBERNATE_TIMEOUT};
handle_info([${, $", $$, $e, $r, $l, $_, $a, $s, $y, $n, $_, $c, $a, $l, $l, $", $ , $:, $ , $[|_] = AsynCall, State) ->
	%处理js函数的异步调用请求
	try zm_json:decode(AsynCall) of
		Data -> 
			handle_asyn_call(Data)
	catch 
		_:Reason ->
			error_logger:error_msg("======>js worker receive message error~n Pid:~p~n Scheduler:~p~n VM:~p~n Reason:~p~n", 
								   [self(), State#state.scheduler, State#state.vm, Reason]),
			io:format("===json decode error===, ~ts~n", [lists:flatten(z_lib:split(AsynCall, "{\"$erl_asyn_call\" : "))])
	end,
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_info(timeout, State) ->
	{noreply, State, hibernate};
handle_info(_, State) ->
    {noreply, State}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
	Reason :: normal
			| shutdown
			| {shutdown, term()}
			| term().
%% ====================================================================
terminate(Reason, State) ->
    error_logger:error_msg("======>~p stop~n Pid:~p~n Reason:~p~n State:~p~n", [?MODULE, self(), Reason, State]).


%% code_change/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
	Result :: {ok, NewState :: term()} | {error, Reason :: term()},
	OldVsn :: Vsn | {down, Vsn},
	Vsn :: term().
%% ====================================================================
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

%%处理返回
handle_reply({ok, "{ok : Infinity}"}) ->
	{ok, infinity};
handle_reply({ok, "{ok : undefined}"}) ->
	{ok, undefined};
handle_reply({ok, "{ok : [object Object]}"}) ->
	{ok, "[object Object]"};
handle_reply({ok, Json}) ->
	try zm_json:decode(Json) of
		T ->
			case sb_trees:is_defined(?OK, T) of
				false ->
					%返回错误
					{error, {z_lib:get_value(T, ?ERROR_DESC, ?NIL), 
							 z_lib:get_value(T, ?ERROR_FILE, ?NIL), 
							 z_lib:get_value(T, ?ERROR_LINE, ?NIL), 
							 z_lib:get_value(T, ?ERROR_SOURCE, ?NIL), 
							 z_lib:get_value(T, ?ERROR_LOCATION, ?NIL), 
							 z_lib:get_value(T, ?ERROR_STACKTRACE, ?NIL)}};
				true ->
					{ok, z_lib:get_value(T, ?OK, ?NIL)}
			end
	catch
		_:_ ->
			{ok, Json}
	end;
handle_reply(Reply) ->
	{error, {invalid_reply, Reply}}.

%%处理异步返回
handle_asyn_reply(Froms, T) ->
	Count=z_lib:get_value(T, ?ASYN_REF_KEY, ?NIL),
	case maps:get(Count, Froms, ?NIL) of
		?NIL ->
			Froms;
		From ->
			R=z_lib:get_value(T, ?ASYN_REPLY_KEY, ?NIL),
			z_lib:reply(From, {ok, R}),
			maps:remove(Count, Froms)
	end.

%%处理异步调用请求
handle_asyn_call(T) ->
	case z_lib:get_value(T, ?ASYN_CALL_KEY, ?NIL) of
		?NIL ->
			ignore;
		{_, ["io", "format"|A]} ->
			handle_asyn_print(A);
		{Len, [M, F|A]} ->
			todo
	end.

%%处理异步打印
handle_asyn_print(A) ->
	io:format("~ts~n", [lists:concat(lists:reverse(handle_asyn_print(A, [])))]).

%%处理参数数据
handle_asyn_print([?NIL|T], L) ->
	%空列表
	handle_asyn_print(T, L);
handle_asyn_print([Arg|T], L) when is_number(Arg) ->
	%数字
	handle_asyn_print(T, [Arg|L]);
handle_asyn_print([Arg|T], L) when is_atom(Arg) ->
	%原子
	handle_asyn_print(T, [Arg|L]);
handle_asyn_print([Arg|T], L) when is_list(Arg) ->
	%字符串
	handle_asyn_print(T, [Arg|L]);
handle_asyn_print([{Len, Arg}|T], L) when is_integer(Len), is_list(Arg) ->
	%数组
	handle_asyn_print(T, ["]"|handle_array(Arg, ["["|L])]);
handle_asyn_print([Arg|T], L) when is_tuple(Arg) ->
	%对象
	handle_asyn_print(T, ["}"|handle_object(sb_trees:to_list(Arg), ["{"|L])]);
handle_asyn_print([_|T], L) ->
	%未知
	handle_asyn_print(T, L);
handle_asyn_print([], L) ->
	L.

%%处理列表参数数据
handle_array([?NIL|T], L) ->
	%空列表
	handle_array(T, L);
handle_array([Arg|T], L) when is_number(Arg) ->
	%数字
	handle_array(T, [",", Arg|L]);
handle_array([Arg|T], L) when is_atom(Arg) ->
	%原子
	handle_array(T, [",", Arg|L]);
handle_array([Arg|T], L) when is_list(Arg) ->
	%字符串
	handle_array(T, [",", Arg|L]);
handle_array([{Len, Arg}|T], L) when is_integer(Len), is_list(Arg) ->
	%数组
	handle_array(T, [",", "]"|handle_array(Arg, ["["|L])]);
handle_array([Arg|T], L) when is_tuple(Arg) ->
	%对象
	handle_array(T, [",", "}"|handle_object(sb_trees:to_list(Arg), ["{"|L])]);
handle_array([_|T], L) ->
	%未知
	handle_array(T, L);
handle_array([], []) ->
	[];
handle_array([], [","|L]) ->
	L;
handle_array([], L) ->
	L.

%%处理对象参数数据
handle_object([?NIL|T], L) ->
	%空列表
	handle_object(T, L);
handle_object([{Key, Value}|T], L) when is_number(Value) ->
	%数字
	handle_object(T, [",", Value, ":", Key|L]);
handle_object([{Key, Value}|T], L) when is_atom(Value) ->
	%原子
	handle_object(T, [",", Value, ":", Key|L]);
handle_object([{Key, Value}|T], L) when is_list(Value) ->
	%字符串
	handle_object(T, [",", Value, ":", Key|L]);
handle_object([{Key, {Len, Value}}|T], L) when is_integer(Len), is_list(Value) ->
	%数组
	handle_object(T, [",", "]"|handle_array(Value, ["[", ":", Key|L])]);
handle_object([{Key, Value}|T], L) when is_tuple(Value) ->
	%对象
	handle_object(T, [",", "}"|handle_object(sb_trees:to_list(Value), ["{", ":", Key|L])]);
handle_object([_|T], L) ->
	%未知
	handle_array(T, L);
handle_object([], []) ->
	[];
handle_object([], [","|L]) ->
	L;
handle_object([], L) ->
	L.

%%解析调用链
parse_chain(Chain) ->
	StrL=string:tokens(Chain, "."),
	{length(StrL), [trim(Obj, $ ) || Obj <- StrL]}.
	
%%清理字符串首尾指定字符
trim(Text, Char) ->
	string:strip(string:strip(Text, right, Char), left, Char).