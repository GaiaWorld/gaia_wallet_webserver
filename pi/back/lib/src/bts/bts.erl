%% 
%% @doc 基于B树的存储引擎
%%


-module(bts).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Include files
%% ====================================================================
-include("bts.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1, open/2, close/1]).

%%
%%启动存储引擎
%%
start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%
%%打开一个指定名称的存储引擎
%%
open(Args, Name) ->
	gen_server:call(?MODULE, {open, Args, Name}, 120000).

%%
%%关闭一个指定的存储引擎
%%
close({Sid, _, _, _, _}) when is_integer(Sid) ->
	gen_server:call(?MODULE, {close, Sid}, 120000);
close(Name) ->
	gen_server:call(?MODULE, {close_named, Name}, 120000).

%% ====================================================================
%% Behavioural functions 
%% ====================================================================

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
init(Args) ->
	process_flag(trap_exit, true),
    {ok, #bts_context{sid = 0}}.


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
handle_call({open, Args, Name}, _, #bts_context{sid = Sid} = State) ->
	NewSid=Sid + 1,
	{reply, open_bts(Args, Name, NewSid), State#bts_context{sid = NewSid}, ?BTS_HIBERNATE_TIMEOUT};
handle_call({close, Sid}, _, State) ->
	{reply, close_bts(Sid), State, ?BTS_HIBERNATE_TIMEOUT};
handle_call({close_named, Name}, _, State) ->
	{reply, close_named_bts(Name), State, ?BTS_HIBERNATE_TIMEOUT};
handle_call(_, _, State) ->
    {noreply, State, ?BTS_HIBERNATE_TIMEOUT}.


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
handle_cast(Msg, State) ->
    {noreply, State}.


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
handle_info({'EXIT', Pid, Why}, State) ->
	%TODO...
	{noreply, State};
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
    ok.


%% code_change/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
	Result :: {ok, NewState :: term()} | {error, Reason :: term()},
	OldVsn :: Vsn | {down, Vsn},
	Vsn :: term().
%% ====================================================================
code_change(OldVsn, State, Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

register(Sid, Name, BTQM, BTSM, Logger, BTPM, IsFix) ->
	Key={?MODULE, Sid},
	MapKey={?MODULE, Name},
	case get(Key) of
		undefined ->
			put(Key, {Name, BTQM, BTSM, Logger, BTPM, IsFix}),
			put(MapKey, Sid),
			{ok, {Sid, BTQM, BTSM, Logger, BTPM}};
		_ ->
			{register_bts_error, {invalid_sid, Sid}}
	end.

start_btpm(IsFix, Args, Name) ->
	Path=z_lib:get_value(Args, ?BTS_PATH, ?DEFAULT_BTS_PATH(Name)),
	case filelib:ensure_dir(Path) of
		ok ->
			case btpm:start_link(Args, Name, Path) of
				{ok, BTPM} ->
					case btpm:load(BTPM) of
						{ok, SupBlock} ->
							start_btqm(IsFix, Args, Name, BTPM, SupBlock, Path);
						Reason ->
							{error, Reason}
					end;
				E ->
					E
			end;
		E ->
			E
	end.

start_btqm(false, Args, Name, BTPM, SupBlock, Path) ->
	case btqmv:start_link(Args, Name) of
		{ok, BTQMV} ->
			start_btsm(false, Args, Name, BTQMV, BTPM, SupBlock, Path);
		E ->
			E
	end;
start_btqm(true, Args, Name, BTPM, SupBlock, Path) ->
	case btqm:start_link(Args, Name) of
		{ok, BTQM} ->
			start_btsm(true, Args, Name, BTQM, BTPM, SupBlock, Path);
		E ->
			E
	end.

start_btsm(false, Args, Name, BTQMV, BTPM, SupBlock, Path) ->
	case btsmv:start_link(Args, Name) of
		{ok, BTSMV} ->
			continue_start(false, Args, Name, BTSMV, BTQMV, BTPM, SupBlock, Path);
		E ->
			E
	end;
start_btsm(true, Args, Name, BTQM, BTPM, SupBlock, Path) ->
	case btsm:start_link(Args, Name) of
		{ok, BTSM} ->
			continue_start(true, Args, Name, BTSM, BTQM, BTPM, SupBlock, Path);
		E ->
			E
	end.

continue_start(false, Args, Name, BTSMV, BTQMV, BTPM, SupBlock, Path) ->
	case btsmv:load(BTSMV, Args, Name, SupBlock) of
		{ok, DatHandle, IdxHandle} ->
			btqmv:set(BTQMV, BTSMV, DatHandle, IdxHandle),
			case bts_logger:start_link(Args, Name, Path) of
				{ok, Logger} ->
					btsmv:set(BTSMV, BTQMV, Logger, BTPM),
					DatCache=bs_variable:get_cache(DatHandle),
					DatAlloter=bs_variable:get_alloter(DatHandle),
					IdxCache=b_trees:get_cache(IdxHandle),
					IdxAlloter=b_trees:get_alloter(IdxHandle),
					bts_logger:set(Logger, DatCache, IdxCache, SupBlock, BTSMV, BTPM),
					ok=btpm:set(BTPM, DatCache, DatAlloter, IdxCache, IdxAlloter, BTSMV, Logger),
					case restore(false, Logger, BTSMV) of
						ok ->
							{ok, BTQMV, BTSMV, Logger, BTPM};
						E ->
							E
					end;
				ignore ->
					{error, ignore};
				E ->
					E
			end;
		E ->
			E
	end;
continue_start(true, Args, Name, BTSM, BTQM, BTPM, SupBlock, Path) ->
	case btsm:load(BTSM, Args, Name, SupBlock) of
		{ok, DatHandle, IdxHandle} ->
			btqm:set(BTQM, BTSM, DatHandle, IdxHandle),
			case bts_logger:start_link(Args, Name, Path) of
				{ok, Logger} ->
					btsm:set(BTSM, BTQM, Logger, BTPM),
					DatCache=b_trees:get_cache(DatHandle),
					DatAlloter=b_trees:get_alloter(DatHandle),
					IdxCache=b_trees:get_cache(IdxHandle),
					IdxAlloter=b_trees:get_alloter(IdxHandle),
					bts_logger:set(Logger, DatCache, IdxCache, SupBlock, BTSM, BTPM),
					ok=btpm:set(BTPM, DatCache, DatAlloter, IdxCache, IdxAlloter, BTSM, Logger),
					case restore(false, Logger, BTSM) of
						ok ->
							{ok, BTQM, BTSM, Logger, BTPM};
						E ->
							E
					end;
				ignore ->
					{error, ignore};
				E ->
					E
			end;
		E ->
			E
	end.

restore(false, Logger, BTSMV) ->
	case bts_logger:check(Logger, 10000) of
		ok ->
			ok;
		{restore, RestoreLogFile} ->
			case bts_logger:restore(Logger, {bts_logger, restore, BTSMV}, RestoreLogFile, 60000) of
				{ok, _Count} ->
					case file:rename(RestoreLogFile, lists:concat([RestoreLogFile, ".", z_lib:now_millisecond()])) of
						ok ->
							ok;
						E ->
							E
					end;
				E ->
					E
			end;
		E ->
			E
	end;
restore(true, Logger, BTSM) ->
	case bts_logger:check(Logger, 10000) of
		ok ->
			ok;
		{restore, RestoreLogFile} ->
			case bts_logger:restore(Logger, {bts_logger, restore, BTSM}, RestoreLogFile, 60000) of
				{ok, _Count} ->
					case file:rename(RestoreLogFile, lists:concat([RestoreLogFile, ".", z_lib:now_millisecond()])) of
						ok ->
							ok;
						E ->
							E
					end;
				E ->
					E
			end;
		E ->
			E
	end.

%%打开一个指定的存储引擎
open_bts(Args, Name, NewSid) ->
	case get({?MODULE, Name}) of
		undefined ->
			IsFix=z_lib:get_value(Args, ?IS_FIX_LENGTH, ?DEFAULT_FIX_LENGTH),
			case start_btpm(IsFix, Args, Name) of
				{ok, BTQM, BTSM, Logger, BTPM} ->
					register(NewSid, Name, BTQM, BTSM, Logger, BTPM, IsFix);
				E ->
					E
			end;
		Sid ->
			case get({?MODULE, Sid}) of
				undefined ->
					{error, invalid_sid};
				{_, BTQM, BTSM, Logger, BTPM, _} ->
					{ok, {Sid, BTQM, BTSM, Logger, BTPM}}
			end
	end.

close_btqm(false, BTQM, BTSM, Logger, BTPM) ->
	case btqm:stop(BTQM) of
		ok ->
			close_btsm(false, BTSM, Logger, BTPM);
		E ->
			E
	end;
close_btqm(true, BTQMV, BTSMV, Logger, BTPM) ->
	case btqmv:stop(BTQMV) of
		ok ->
			close_btsm(true, BTSMV, Logger, BTPM);
		E ->
			E
	end.

close_btsm(false, BTSM, Logger, BTPM) ->
	case btsm:stop(BTSM) of
		ok ->
			continue_close(Logger, BTPM);
		E ->
			E
	end;
close_btsm(true, BTSMV, Logger, BTPM) ->
	case btsmv:stop(BTSMV) of
		ok ->
			continue_close(Logger, BTPM);
		E ->
			E
	end.

continue_close(Logger, BTPM) ->
	case bts_logger:stop(Logger) of
		ok ->
			case btpm:stop(BTPM) of
				ok ->
					ok;
				E ->
					E
			end;
		E ->
			E
	end.
	
close_bts(Sid) ->
	case erase({?MODULE, Sid}) of
		{Name, BTQM, BTSM, Logger, BTPM, IsFix} ->
			erase({?MODULE, Name}),
			close_btqm(IsFix, BTQM, BTSM, Logger, BTPM);
		undefined ->
			{error, {close_bts_error, {bts_not_exist, Sid}}}
	end.

close_named_bts(Name) ->
	case get({?MODULE, Name}) of
		undefined ->
			{error, {close_bts_error, {bts_not_exist, Name}}};
		Sid ->
			close_bts(Sid)
	end.
