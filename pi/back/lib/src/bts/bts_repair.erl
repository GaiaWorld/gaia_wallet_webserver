%% 
%% @doc bts修复器
%%


-module(bts_repair).

%% ====================================================================
%% API functions
%% ====================================================================
-export([repair/3]).

%%
%% 修复
%%
repair(BTSM, SnapshootFileName, LogFileName) ->
	case {filelib:is_regular(SnapshootFileName), filelib:is_regular(LogFileName)}  of
		{true, true} ->
			todo;
		{false, true} ->
			todo;
		_ ->
			todo
	end.

%% ====================================================================
%% Internal functions
%% ====================================================================

bytes_to_log({M, A}, Bin) ->
	M:deserialize(A, Bin).
