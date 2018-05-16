%%% -------------------------------------------------------------------
%%% Author  : Administrator
%%% Description :
%%%
%%% Created : 2011-7-6
%%% -------------------------------------------------------------------
-module(erl_shell_proxy).
-behaviour(gen_server).

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("erl_shell_proxy.hrl").
-include("erl_shell_simulator.hrl").

%% --------------------------------------------------------------------
%% External exports
-export([start_link/1, clean_con/1, open/2, apply/4, access/4, access/3, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% External functions
%% ====================================================================

start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

open(Args, Node) when is_list(Args), is_atom(Node) ->
	gen_server:call(?MODULE, {open, Args, Node}).

apply(Args, Node, {M, F, Bin} = MF, Timeout) when is_list(Args), is_atom(Node), is_atom(M), 
											is_atom(F), is_binary(Bin), is_integer(Timeout), Timeout >= ?DEFAULT_MIN_TIMEOUT  ->
	case gen_server:call(?MODULE, {apply, self(), Args, Node, MF}) of
		ok ->
			wait_shell_response(Timeout, "");
		E ->
			E
	end.

%%清理节点终端会话
clean_con(Node) ->
	gen_server:call(?MODULE, {?CLEAN_CON, Node}).

access({Node, ShellName} = Shell, Cmd, false, Timeout) when is_atom(Node), is_atom(ShellName), is_list(Cmd), length(Cmd) =< ?DEFAULT_CMD_MAX_LENGTH, 
															   is_integer(Timeout), Timeout >= ?DEFAULT_MIN_TIMEOUT ->
	gen_server:cast(?MODULE, {self(), ?INPUT_ACTION, Shell, Cmd}),
	wait_shell_response(Timeout, "");
access({Node, ShellName} = Shell, Cmd, true, Timeout) when is_atom(Node), is_atom(ShellName), is_list(Cmd), length(Cmd) =< ?DEFAULT_CMD_MAX_LENGTH, 
															  is_integer(Timeout), Timeout >= ?DEFAULT_MIN_TIMEOUT ->
	gen_server:cast(?MODULE, {self(), ?FORMAT_INPUT_ACTION, Shell, Cmd}),
	wait_shell_response(Timeout, "").

access(Shell, Cmd, IS_FROMAT) ->
	access(Shell, Cmd, IS_FROMAT, ?DEFAULT_RPC_TIMEOUT).

stop({Node, ShellName}) when is_atom(Node), is_atom(ShellName) ->
	gen_server:call(?MODULE, {?STOP_ACTION, Node, ShellName}).

%% ====================================================================
%% Server functions
%% ====================================================================

%% --------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%% --------------------------------------------------------------------
init(Args) ->
	ok = net_kernel:monitor_nodes(true),
	MaxConsoleCount=z_lib:get_value(Args, ?MAX_CONSOLE_PER_NODE_FLAG, ?DEFAULT_MAX_CONSOLE_PER_NODE),
	MaxScriptCount=z_lib:get_value(Args, ?MAX_SCRIPT_PER_NODE_FLAG, ?DEFAULT_MAX_SCRIPT_PER_NODE),
	CheckTime=z_lib:get_value(Args, ?CHECK_TIME, ?DEFAULT_CHECK_TIME),
	ConsoleResponseTimeout=z_lib:get_value(Args, ?CONSOLE_RESPONSE_TIMEOUT, ?DEFAULT_CONSOLE_RESPONSE_TIMEOUT),
	ScriptResponseTimeout=z_lib:get_value(Args, ?SCRIPT_RESPONSE_TIMEOUT, ?DEFAULT_SCRIPT_RESPONSE_TIMEOUT),
	Auth_Table=z_lib:get_value(Args, ?AUTH_TABLE_FLAG, ""),
    {ok, #state{auth_table = Auth_Table, max_console_per_node = MaxConsoleCount, max_script_per_node = MaxScriptCount, 
				shell_pid_table = sb_trees:empty(), shell_table = sb_trees:empty(), shell_index_table = sb_trees:empty(), 
				console_time = ConsoleResponseTimeout, script_time = ScriptResponseTimeout, timer = erlang:start_timer(CheckTime, self(), {?CHECK_TIMEOUT, CheckTime})}}.

%% --------------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_call({open, Args, Node}, _From, #state{max_console_per_node = MaxConsoleCount, shell_table = ShellTable, shell_pid_table = ShellPidTable, shell_index_table = ShellIndexTable} = State) ->
	case check_shell_amount(get_node_shell_count(ShellTable, {Node, ?CONSOLE_TYPE}), MaxConsoleCount) of
		{ok, Amount} ->
			case is_active(Node) of
				true ->
					case create_shell(Args, Node, ?CONSOLE_TYPE, ?DEFAULT_RPC_TIMEOUT) of
						{ok, ShellName, ShellPid, ShellTitle} ->
							bind_shell(ShellPid),
							{reply, {ok, {Node, ShellName}, ShellTitle, 
									 lists:concat(["(", Node, ")1>"])}, State#state{shell_table = sb_trees:enter({Node, ?CONSOLE_TYPE}, Amount + 1, ShellTable),
																					shell_pid_table = sb_trees:enter(ShellPid, {ShellName, ?CONSOLE_TYPE, Node, none, 0}, ShellPidTable),
																					shell_index_table = sb_trees:enter(ShellName, ShellPid, ShellIndexTable)}};
						Other ->
							{reply, Other, State, ?SHELL_PROXY_HIBERNATE_TIMEOUT}
					end;
				false ->
					{reply, {error, invalid_node}, State, ?SHELL_PROXY_HIBERNATE_TIMEOUT}
			end;
		{out_of_shell, _} = Error ->
			{reply, {error, Error}, State, ?SHELL_PROXY_HIBERNATE_TIMEOUT}
	end;
handle_call({apply, Src, Args, Node, MF}, _From, #state{max_script_per_node=MaxScriptCount, shell_table=ShellTable, shell_pid_table = ShellPidTable, shell_index_table = ShellIndexTable, script_time = ScriptResponseTimeout} = State) ->
	case check_shell_amount(get_node_shell_count(ShellTable, Node), MaxScriptCount) of
		{ok, Amount} ->
			case is_active(Node) of
				true ->
					case create_shell(Args, Node, ?SCRIPT_TYPE, ?DEFAULT_RPC_TIMEOUT) of
						{ok, ShellName, ShellPid, _} ->
							bind_shell(ShellPid),
							ShellPid ! {?APPLY_ACTION, Src, self(), MF},
							{reply, ok, State#state{shell_table=sb_trees:enter({Node, ?SCRIPT_TYPE}, Amount + 1, ShellTable),
													shell_pid_table=sb_trees:enter(ShellPid, {ShellName, ?SCRIPT_TYPE, Node, Src, zm_time:now_millisecond() + ScriptResponseTimeout}, ShellPidTable),
													shell_index_table = sb_trees:enter(ShellName, ShellPid, ShellIndexTable)}, ?SHELL_PROXY_HIBERNATE_TIMEOUT};
						Other ->
							{reply, Other, State, ?SHELL_PROXY_HIBERNATE_TIMEOUT}
					end;
				false ->
					{reply, {error, invalid_node}, State, ?SHELL_PROXY_HIBERNATE_TIMEOUT}
			end;
		{out_of_shell, _} = Error ->
			{reply, {error, Error}, State, ?SHELL_PROXY_HIBERNATE_TIMEOUT}
	end;
handle_call({?STOP_ACTION, _Node, ShellName}, Src, #state{shell_index_table=ShellIndexTable} = State) ->
	case sb_trees:lookup(ShellName, ShellIndexTable) of
		{_, ShellPid} ->
			ShellPid ! {?STOP_ACTION, Src, self()};
		none ->
			z_lib:reply(Src, stop_ok),
			break
	end,
	{noreply, State, ?SHELL_PROXY_HIBERNATE_TIMEOUT};

handle_call({?CLEAN_CON, Node}, _From, #state{shell_table = ShellTable, shell_pid_table = ShellPidTable, shell_index_table = ShellIndexTable} = State) ->
	{NewShellTable, NewShellPidTable, NewShellIndexTable}=clean_(sb_trees:iterator(ShellPidTable), Node, ShellTable, ShellPidTable, ShellIndexTable),
	{reply, ok, State#state{shell_table = NewShellTable, 
				shell_pid_table = NewShellPidTable, 
				shell_index_table = NewShellIndexTable}, ?SHELL_PROXY_HIBERNATE_TIMEOUT};
handle_call(_, _, State) ->
    {reply, invalid_request, State, ?SHELL_PROXY_HIBERNATE_TIMEOUT}.

%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast({From, ?INPUT_ACTION, {Node, ShellName}, Input}, #state{shell_pid_table = ShellPidTable, shell_index_table = ShellIndexTable, console_time = ConsoleResponseTimeout} = State) when is_pid(From) ->
	NewShellPidTable=case sb_trees:lookup(ShellName, ShellIndexTable) of
		{_, ShellPid} ->
			case sb_trees:lookup(ShellPid, ShellPidTable) of
				{_, {ShellName, Type, Node, _, _}} ->
					ShellPid ! {?INPUT_ACTION, self(), Input},
					sb_trees:update(ShellPid, {ShellName, Type, Node, From, zm_time:now_millisecond() + ConsoleResponseTimeout}, ShellPidTable);
				E ->
					From ! {error, E},
					ShellPidTable
			end;
		E ->
			From ! {error, E},
			ShellPidTable
	end,
	{noreply, State#state{shell_pid_table = NewShellPidTable}, ?SHELL_PROXY_HIBERNATE_TIMEOUT};
handle_cast(_, State) ->
    {noreply, State, ?SHELL_PROXY_HIBERNATE_TIMEOUT}.

%% --------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_info({io_request, ShellPid, Ref, Request}, #state{shell_pid_table = ShellPidTable} = State) ->
	NewState=case sb_trees:lookup(ShellPid, ShellPidTable) of
		{_, {ShellName, Type, Node, From, _}} when From =/= none ->
			try handle_output(Request) of
				?DEFAULT_EOF_FLAG ->
					From ! shell_response_tail;
				Result ->
					From ! {shell_response, Result}
			catch
				_:Reason ->
					From ! {shell_response, lists:flatten(io_lib:format("~p~n", [{error, Reason}]))}
			end,
			State#state{shell_pid_table = sb_trees:update(ShellPid, {ShellName, Type, Node, From, 0}, ShellPidTable)};
		_ ->
			State
	end,
	ShellPid ! {io_reply, Ref, ok},
	{noreply, NewState, ?SHELL_PROXY_HIBERNATE_TIMEOUT};
handle_info({stop_ok, Src, ShellName, Type, ShellPid}, #state{shell_table = ShellTable, shell_pid_table = ShellPidTable, shell_index_table = ShellIndexTable} = State) ->
	Node=node(ShellPid),
	case sb_trees:lookup({Node, Type}, ShellTable) of
		{_, Amount} ->
			z_lib:reply(Src, stop_ok),
			{noreply, State#state{shell_table = sb_trees:update({Node, Type}, Amount - 1, ShellTable),
								  shell_pid_table = sb_trees:delete(ShellPid, ShellPidTable),
								  shell_index_table = sb_trees:delete(ShellName, ShellIndexTable)}, ?SHELL_PROXY_HIBERNATE_TIMEOUT};
		_ ->
			{noreply, State, ?SHELL_PROXY_HIBERNATE_TIMEOUT}
	end;
	
handle_info({receive_timeout, ShellName, Type, ShellPid}, #state{shell_table = ShellTable, shell_pid_table = ShellPidTable, shell_index_table = ShellIndexTable} = State) ->
	Node=node(ShellPid),
	case sb_trees:lookup({Node, Type}, ShellTable) of
		{_, Amount} ->
			{noreply, State#state{shell_table = sb_trees:update({Node, Type}, Amount - 1, ShellTable),
								  shell_pid_table = sb_trees:delete(ShellPid, ShellPidTable),
								  shell_index_table = sb_trees:delete(ShellName, ShellIndexTable)}, ?SHELL_PROXY_HIBERNATE_TIMEOUT};
		_ ->
			{noreply, State, ?SHELL_PROXY_HIBERNATE_TIMEOUT}
	end;
handle_info({timeout, TimerRef, {?CHECK_TIMEOUT, CheckTime}}, #state{timer = TimerRef} = State) ->
	{noreply, collate(CheckTime, State), ?SHELL_PROXY_HIBERNATE_TIMEOUT};
handle_info(timeout, State) ->
	{noreply, State, hibernate};
handle_info({nodedown, Node}, State) ->
	#state{shell_table = ShellTable, shell_pid_table = ShellPidTable, shell_index_table = ShellIndexTable} = State,
	{NewShellTable, NewShellPidTable, NewShellIndexTable}=clean_(sb_trees:iterator(ShellPidTable), Node, ShellTable, ShellPidTable, ShellIndexTable),
	{noreply, State#state{shell_table = NewShellTable, 
				shell_pid_table = NewShellPidTable, 
				shell_index_table = NewShellIndexTable}, ?SHELL_PROXY_HIBERNATE_TIMEOUT};
handle_info(_, State) ->
    {noreply, State, ?SHELL_PROXY_HIBERNATE_TIMEOUT}.

%% --------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% --------------------------------------------------------------------
terminate(_, _) ->
    ok.

%% --------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% --------------------------------------------------------------------
code_change(_, State, _) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------

is_active(Node) ->
	lists:member(Node, nodes()).
	
get_node_shell_count(Shell_Table, Node) ->
	case sb_trees:lookup(Node, Shell_Table) of
		{_, Value} ->
			Value;
		none ->
			0
	end.

check_shell_amount(Amount, MaxCount) when Amount >= MaxCount ->
	{out_of_shell, Amount};
check_shell_amount(Amount, MaxCount) when Amount < MaxCount ->
	{ok, Amount}.

create_shell(Args, Node, Type, Timeout) ->
	Name=list_to_atom(erlang:ref_to_list(erlang:make_ref())),
	case public_lib:rpc(Node, erl_shell_simulator, start, [Name, Type, self(), Args], Timeout) of
		{value, {badrpc, Reason}} ->
			{error, Reason};
		{value, {ok, ShellName, ShellTitle}} ->
			case public_lib:rpc(Node, erlang, whereis, [ShellName], Timeout) of
				{value, {badrpc, Reason}} ->
					{error, Reason};
				{value, ShellPid} ->
					{ok, ShellName, ShellPid, ShellTitle};
				timeout ->
					{error, timeout}
			end;
		timeout ->
			{error, timeout}
	end.

bind_shell(Shell_Pid) ->
	group_leader(self(), Shell_Pid),
	ok.

handle_output({put_chars, _, _, _, [?DEFAULT_EOF_FLAG, _]}) ->
	?DEFAULT_EOF_FLAG;
handle_output({put_chars, _, Characters}) ->
	Characters;
handle_output({put_chars, _, Mod, Fun, Args}) ->
	apply(Mod, Fun, Args);
handle_output({put_chars, Characters}) ->
	Characters;
handle_output({put_chars, Mod, Fun, Args}) ->
	apply(Mod, Fun, Args).

collate(CheckTime, #state{shell_table = ShellTable, shell_pid_table = ShellPidTable, shell_index_table = ShellIndexTable} = State) ->
	{NewShellTable, NewShellPidTable, NewShellIndexTable}=collate1(sb_trees:iterator(ShellPidTable), zm_time:now_millisecond(), ShellTable, ShellPidTable, ShellIndexTable),
	State#state{shell_table = NewShellTable, 
				shell_pid_table = NewShellPidTable, 
				shell_index_table = NewShellIndexTable, 
				timer = erlang:start_timer(CheckTime, self(), {?CHECK_TIMEOUT, CheckTime})}.

collate1(Iterator, Now, ShellTable, ShellPidTable, ShellIndexTable) ->
	case sb_trees:next(Iterator) of
		{ShellPid, {ShellName, ?CONSOLE_TYPE, Node, From, Timeout}, NewIterator} when Now >= Timeout, Timeout > 0 ->
			collate_send(ShellPid, From),
			collate1(NewIterator, Now, collate_amount({Node, ?CONSOLE_TYPE}, ShellTable), sb_trees:delete(ShellPid, ShellPidTable), sb_trees:delete(ShellName, ShellIndexTable));
		{ShellPid, {ShellName, ?SCRIPT_TYPE, Node, Src, Timeout}, NewIterator} when Now >= Timeout, Timeout > 0 ->
			collate_send(ShellPid, Src),
			collate1(NewIterator, Now, collate_amount({Node, ?SCRIPT_TYPE}, ShellTable), sb_trees:delete(ShellPid, ShellPidTable), sb_trees:delete(ShellName, ShellIndexTable));
		{_, _, NewIterator} ->
			collate1(NewIterator, Now, ShellTable, ShellPidTable, ShellIndexTable);
		none ->
			{ShellTable, ShellPidTable, ShellIndexTable}
	end.

clean_(Iterator, Node, ShellTable, ShellPidTable, ShellIndexTable) ->
	case sb_trees:next(Iterator) of
		{ShellPid, {ShellName, Type, Node, _From, _Timeout}, NewIterator} ->
			clean_(NewIterator, Node, collate_amount({Node, Type}, ShellTable), sb_trees:delete(ShellPid, ShellPidTable), sb_trees:delete(ShellName, ShellIndexTable));
		{_, _, NewIterator} ->
			collate1(NewIterator, Node, ShellTable, ShellPidTable, ShellIndexTable);
		none ->
			{ShellTable, ShellPidTable, ShellIndexTable}
	end.

collate_send(ShellPid, From) ->
	ShellPid ! {?STOP_ACTION, From, self()},
	From ! {shell_response, lists:flatten(io_lib:format("~p~n", [{error, request_timeout}]))},
	From ! shell_response_tail.

collate_amount(Key, ShellTable) ->
	case sb_trees:lookup(Key, ShellTable) of
		{_, Amount} ->
			sb_trees:update(Key, Amount - 1, ShellTable);
		_ ->
			ShellTable
	end.
wait_shell_response(Timeout, Response) ->
	receive
		{shell_response, Result} ->
			wait_shell_response(Timeout, [Result|Response]);
		shell_response_tail ->
			{ok, lists:flatten(lists:reverse(Response))};
		stop_ok ->
			{ok, closed};
		{error, Reason} ->
			{error, Reason};
		_ ->
			wait_shell_response(Timeout, Response)
	after Timeout ->
		{error, timeout}
	end.