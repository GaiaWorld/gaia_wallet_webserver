%% Author: Administrator
%% Created: 2011-7-6
%% Description: erlang控制台模拟器
-module(erl_shell_simulator).

%%
%% Include files
%%
-include("erl_shell_simulator.hrl").

%%
%% Exported Functions
%%
-export([get_simulator_name/1, start/4, eval/3, stop/1]).

%%
%% API Functions
%%

get_simulator_name(ShellName) when is_atom(ShellName) ->
	list_to_atom(lists:concat([?MODULE, ":", ShellName])).

start(Name, Type, From, Args) ->
	ShellName=get_simulator_name(Name),
	erlang:register(ShellName, spawn(fun() ->
				simulator(From, Args, ShellName, Type, ?INIT_LINE_NUM, [], get_export_funs([c, erlang]))
			end
		)),
	{ok, ShellName, shell_title_info(Args)}.

stop(ShellName) when is_atom(ShellName) ->
	case erlang:whereis(ShellName) of
		undefined ->
			{error, invalid_name};
		Pid ->
			erlang:unregister(ShellName),
			exit(Pid, kill)
	end.

%%
%% Local Functions
%%

simulator(From, Args, ShellName, Type, LineNum, Context, LocalFuns) ->
	Timeout=z_lib:get_value(Args, ?SHELL_TIMEOUT_FLAG, ?DEFAULT_SHELL_TIMEOUT),
	Node=node(),
	Self=self(),
	receive
		{?FORMAT_INPUT_ACTION, From, Input} ->
			io:format(lists:concat(["(", Node, ")", LineNum, ">~p~n"]), [Input]),
			{Value, NewContext}=eval(Input, Context, LocalFuns),
			io:format("~p~n", [Value]),
			io:format(?DEFAULT_EOF_FLAG),
			simulator(From, Args, ShellName, Type, LineNum + 1, NewContext, LocalFuns);
		{?INPUT_ACTION, From, Input} ->
			NewLineNum=LineNum + 1,
			{Value, NewContext}=eval(Input, Context, LocalFuns),
			io:format("~p~n", [Value]),
			io:format(lists:concat(["(", Node, ")", NewLineNum, ">"])),
			io:format(?DEFAULT_EOF_FLAG),
			simulator(From, Args, ShellName, Type, NewLineNum, NewContext, LocalFuns);
		{?APPLY_ACTION, Src, From, {M, F, Bin}} ->
			case load_script(M, Bin) of
				{module, M} ->
					io:format("~p~n", [invoke(M, F)]),
					io:format(?DEFAULT_EOF_FLAG),
					code:purge(ShellName),
					code:delete(ShellName),
					From ! {stop_ok, Src, ShellName, Self};
				{error, Reason} ->
					io:format("~p~n", [{load_script_error, Reason}]),
					io:format(?DEFAULT_EOF_FLAG),
					From ! {stop_ok, Src, ShellName, Self}
			end;
		{?STOP_ACTION, Src, From} ->
			From ! {stop_ok, Src, ShellName, Type, Self};
		_ ->
			simulator(From, Args, ShellName, Type, LineNum, Context, LocalFuns)
	after Timeout ->
			From ! {receive_timeout, ShellName, Type, Self}
	end.

console_eval(Str, Binding, LocalFuns) ->
    {ok, Ts, _} = erl_scan:string(Str),
    Ts1 = case lists:reverse(Ts) of
              [{dot, _}|_] -> Ts;
              TsR -> lists:reverse([{dot, 1}|TsR])
          end,
    {ok, Expr} = erl_parse:parse_exprs(Ts1),
    erl_eval:exprs(Expr, Binding, {value, fun(F, A) -> parse_fun(LocalFuns, F, A) end}, none).

eval(?DEFAULT_CLEAR_BIND_FUN, _, _) ->
	{ok, []};
eval([$f, $(|T], Context, _) ->
	case string:tokens(T, ").") of
		[VarName] ->
			{ok, proplists:delete(list_to_atom(VarName), Context)};
		_ ->
			{{error, undef}, Context}
	end;
eval(Input, Context, LocalFuns) ->
	try
		begin
			{value, Value, NewContext}=console_eval(Input, Context, LocalFuns),
			{Value, NewContext}
		end
	catch
		_:Reason -> 
			{{error, Reason}, Context}
	end.

load_script(Mod, Bin) ->
	code:load_binary(Mod, lists:concat([Mod, ".script"]), Bin).

invoke(M, F) ->
	try
		M:F()
	catch
		_:Reason ->
			{error, Reason}
	end.		

shell_title_info(_Args) ->
	lists:concat(["Erlang/", string:to_upper(atom_to_list(erlang:system_info(build_type))), " ", erlang:system_info(otp_release), 
				  " [", erlang:system_info(wordsize) bsl 3, "-bit]", " [kernel-poll:", epoll =:= element(2, lists:keyfind(kernel_poll, 1, erlang:system_info(check_io))), "]", 
				  " [sys_time:", tdate_to_sdate(calendar:now_to_local_time(os:timestamp())), "]", " [erl_time:", tdate_to_sdate(calendar:now_to_local_time(now())), "]\n\n",
				  "Eshell V", erlang:system_info(version), "\n"]). 

tdate_to_sdate({{Y, M, D}, {H, Mi, S}}) ->
	lists:concat([Y, "-", M, "-", D, " ", H, ":", Mi, ":", S]).

get_export_funs(Modules) when is_list(Modules) ->
	[{Module, element(2, lists:keyfind(exports, 1, Module:module_info()))} || Module <- Modules];
get_export_funs(Module) ->
	element(2, lists:keyfind(exports, 1, Module:module_info())).

parse_fun([{M, L}|T], F, A) ->
	case is_fun(L, F, length(A)) of
		false ->
			parse_fun(T, F, A);
		true ->
			try
				apply(M, F, A)
			catch
				_:Reason ->
					{error, Reason}
			end
	end;
parse_fun([], F, A) ->
	{error, {undef, list_to_atom(lists:concat([F, "/", length(A)]))}}.

is_fun([{F, N}|_], F, N) ->
	true;
is_fun([{_, _}|T], F, N) ->
	is_fun(T, F, N);
is_fun([], _, _) ->
	false.
	
