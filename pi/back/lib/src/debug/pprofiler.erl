%% 
%% @doc 进程cpu性能剖析器
%%


-module(pprofiler).

%% ====================================================================
%% Include files
%% ====================================================================
-define(EOF_FLAG, "$eof").
-define(PROFILER_TIMEOUT, 3000).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start/1, report/1, stop/1]).

%%
%%开始剖析
%%
start(Pids) ->
	Profiler=new_profiler(Pids),
	receive
		{Profiler, Reply} ->
			Reply
	after ?PROFILER_TIMEOUT ->
		{error, start_timeout}
	end.

%%
%%导出剖析报表
%%
report({From, Profiler}) ->
	Profiler ! {From, report},
	report_loop([]).

%%
%%关闭剖析
%%
stop({From, Profiler}) ->
	Profiler ! {From, stop},
	receive
		{Profiler, stop_ok} ->
			ok
	after ?PROFILER_TIMEOUT ->
		{error, stop_timeout}
	end.


%% ====================================================================
%% Internal functions
%% ====================================================================

init_profiler(Pids) ->
	case eprof:start() of
		{ok, EProf} ->
			set_process(EProf, Pids);
		{error,{already_started, EProf}} ->
			set_process(EProf, Pids);
		{error, Reason} ->
			{error, {start_failed, Reason}}
	end.		

set_process(EProf, Pids) ->
	case eprof:profile(Pids) of
		profiling ->
			{ok, EProf};
		{error, Reason} ->
			{error, {profile_failed, Reason}}
	end.

handle_output({put_chars, _, _, _, [?EOF_FLAG, _]}) ->
	?EOF_FLAG;
handle_output({put_chars, _, M, F, A}) ->
	apply(M, F, A);
handle_output(_) ->
	"".

profiler_loop(From, Profiler) ->
	receive
		{From, report} ->
			eprof:stop_profiling(),
			eprof:analyze(),
			io:format(?EOF_FLAG),
			profiler_loop(From, Profiler);
		{From, stop} ->
			eprof:stop(),
			From ! {Profiler, stop_ok}
	end.

new_profiler(Pids) ->
	From=self(),
	Profiler=spawn(fun() ->
		Self=self(),
		case init_profiler(Pids) of
			{ok, _EProf} ->
				From ! {Self, {ok, {From, Self}}},
				profiler_loop(From, Self);
			E ->
				From ! {Self, E}
		end
	end),
	group_leader(From, Profiler),
	Profiler.

report_loop(Result) ->
	receive
		{io_request, Profiler, Proxy, Request} ->
			io:format("!!!!!!Proxy:~p~n", [Proxy]),
			try handle_output(Request) of
				?EOF_FLAG ->
					Profiler ! {io_reply, Proxy, ok},
					{ok, lists:flatten(lists:reverse(Result))};
				Output ->
					Profiler ! {io_reply, Proxy, ok},
					report_loop([Output|Result])
			catch
				_:Reason ->
					{error, {report_failed, Reason}}
			end
	end.

