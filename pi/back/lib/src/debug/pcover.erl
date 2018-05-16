%% 
%% @doc 代码覆盖器
%%


-module(pcover).

%% ====================================================================
%% API functions
%% ====================================================================
-export([modules/0, start/1, start_dir/1, cover/1, stop/1]).

%%
%%查看当前可以被覆盖器处理的模块
%%
modules() ->
	cover:modules().

%%
%%初始化模块覆盖器
%%
start(Modules) ->
	Pid=case cover:start() of
		{ok, P} ->
			P;
		{error, {already_started, P}} ->
			P;
		{error, Reason} ->
			erlang:error(Reason)
	end,
	case start1(Modules) of
		ok ->
			{ok, Pid};
		E ->
			E
	end.

%%
%%初始化多模块覆盖器
%%
start_dir(Dir) ->
	Pid=case cover:start() of
		{ok, P} ->
			P;
		{error, {already_started, P}} ->
			P;
		{error, Reason} ->
			erlang:error(Reason)
	end,
	case cover:compile_beam_directory(Dir) of
		{ok, _} ->
			{ok, Pid};
		{error, _} = E ->
			E
	end.

%%
%%覆盖分析
%%
cover(Module) ->
	case cover:analyse(Module, calls, line) of
		{ok, CoverInfo} ->
			{ok, {Module, [{Line, Count} || {{Mod, Line}, Count} <- CoverInfo, Mod =:= Module]}};
		{error, _} = E ->
			E
	end.

%%
%%关闭覆盖器
%%
stop(Node) ->
	cover:stop(Node).

%% ====================================================================
%% Internal functions
%% ====================================================================

start1([Module|T]) ->
	case cover:compile_beam(Module) of
		{ok, _} ->
			start1(T);
		{error, Reason} ->
			{error, {Module, Reason}}
	end;
start1([]) ->
	ok.
