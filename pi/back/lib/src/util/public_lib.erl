%%@doc 公共函数，提供一些工具函数
%%@end
-module(public_lib).

%%%=======================INCLUDE======================
-define(PORT_CREATOR_NAME, os_cmd_port_creator).

%%%=======================EXPORT=======================
-export([get_local_address/1, parse_local_address_ipv4/1, check_node_host/1, get_node_name/1, get_node_host/1, dns/1, 
		 exec_local_cmd/2, exec_local_cmd/1, exec_local_parameter_cmd/2, 
		 md5_str/1, password/2, 
		 is_lists/1,
		 eval/2, rpc/5, arithmetic/2, fac/1, ladd/1, lmul/1, write_to_file/2, write_text_to_file/2, write_list_to_file/2, write_term_to_file/2, list_to_string/2, size/1, unique/1,
		 del_dir/2, del_file/1, get_number_condition_vars/1, check_number_condition/2, day_of_year/1]).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 获取本地ip地址
%% @spec get_local_address(NetId::list()) -> return()
%% where
%% return() = list() | {error,Reason}
%%@end
%% -----------------------------------------------------------------
get_local_address(NetId) when is_list(NetId) ->
	case os:type() of
		{unix, _} ->
			case inet:getifaddrs() of
				{ok, List} ->
					get_local_address1(NetId, List);
				Error ->
					Error
			end;
		{win32, _} ->
			{ok, Name} = inet:gethostname(),
			{ok, IP} = inet:getaddr(Name, inet),
			parse_local_address_ipv4(IP);
		_ ->
			throw({error, invalid_os})
	end.

get_local_address1(_, []) ->
	"";
get_local_address1(NetId, [{NetId, Info}|_]) ->
	parse_local_address_ipv4(z_lib:get_value(Info, addr, ""));
get_local_address1(NetId, [_|T]) ->
	get_local_address1(NetId, T).

%% -----------------------------------------------------------------
%%@doc 将内部ipv4地址转换为ipv4地址的字符串形式
%% @spec parse_local_address_ipv4({A::integer(),B::integer(),C::integer(),D::integer()}) -> return()
%% where
%%  return() = Address::list()
%%@end
%% -----------------------------------------------------------------
parse_local_address_ipv4({A,B,C,D}) when is_integer(A), A =< 255, is_integer(B), B =< 255, is_integer(C), C =< 255, is_integer(D), D =<255 ->
	lists:concat([A,".",B,".",C,".",D]).

%% -----------------------------------------------------------------
%%@doc 检查指定节点是正在运行
%% @spec check_node_host(SlaveNode::atom()) -> return()
%% where
%% return() = {not_running, Node} | {already_running, Node}
%%@end
%% -----------------------------------------------------------------
check_node_host(Slave_Node) ->
	Host0=get_node_host(Slave_Node),
	Host =case net_kernel:longnames() of
			true -> 
				dns(Host0);
	    	false -> 
				strip_host_name(Host0);
	    	ignored -> 
				throw({error, not_alive})
		end,
	Node = list_to_atom(lists:concat([get_node_name(Slave_Node), "@", Host])),
	case net_adm:ping(Node) of
		pang ->
		    {not_running, Node};
		pong -> 
		    {already_running, Node}
    end.

%% -----------------------------------------------------------------
%%@doc 获取标准节点名中的节点名称
%%```
%%  Name::list()
%%'''
%% @spec get_node_name(Node::atom()) -> Name
%%@end
%% -----------------------------------------------------------------
get_node_name(Node) when is_atom(Node)->
	[Name,_]=string:tokens(atom_to_list(Node), "@"),
	Name.

%% -----------------------------------------------------------------
%%@doc 获取标准节点名中的节点主机
%%```
%%  Host::list()
%%'''
%% @spec get_node_host(Node::atom()) -> Host
%%@end
%% -----------------------------------------------------------------
get_node_host(Node) when is_atom(Node) ->
	[_,Host]=string:tokens(atom_to_list(Node), "@"),
	Host.

%% -----------------------------------------------------------------
%%@doc 获取指定主机的DNS
%%```
%%  Host::string()
%%'''
%% @spec dns(H::h()) -> Host
%% where
%% h() = atom() | list()
%%@end
%% -----------------------------------------------------------------
dns(H) when is_atom(H); is_list(H) -> 
	{ok, Host} = net_adm:dns_hostname(H), 
	Host.

%%get strip host
strip_host_name([]) -> [];
strip_host_name([$.|_]) -> [];
strip_host_name([H|T]) -> [H|strip_host_name(T)].

%% -----------------------------------------------------------------
%%@doc 运行本地指定路径和名称的可运行文件
%% @spec exec_local_cmd(Path::list(), CMD::list()) -> return()
%% where
%% return() = list() | {error,Reason}
%%@end
%% -----------------------------------------------------------------
exec_local_cmd(Path, CMD) when is_list(Path), is_list(CMD) ->
	try
		validate(CMD),
    	case os:type() of
		{unix, _} ->
	    	os:cmd(filename:join(["/"++Path, CMD]));
		{win32, Wtype} ->
		    Command = case {os:getenv("COMSPEC"),Wtype} of
						{false,windows} -> 
							lists:concat(["command.com /c", CMD]);
				  		{false,_} -> 
							lists:concat(["cmd /c", CMD]);
				  		{Cspec,_} -> 
							lists:concat([Cspec," /c ",CMD])
			      end,
		    Port = open_port({spawn, Command}, [stream, in, eof, hide, stderr_to_stdout, {cd, Path}]),
		    get_data(Port, []);
		vxworks ->
	    	Command = lists:concat(["sh -c '", CMD, "'"]),
	    	Port = open_port({spawn, Command}, [stream, in, eof, stderr_to_stdout, {cd, Path}]),
	    	get_data(Port, [])
   		end
	catch
		_:Reason ->
			{error, Reason}
	end.

%% -----------------------------------------------------------------
%%@doc 运行本地指定名称的可运行文件，此可运行文件需要可以通过系统环境找到其路径
%% @spec exec_local_cmd(Command) -> return()
%% where
%% return() = list() | {error,Reason}
%%@end
%% -----------------------------------------------------------------
exec_local_cmd(Command) ->
	CMD=filename:basename(Command),
	case file:get_cwd() of
		{ok, Path} ->
			case filename:pathtype(Command) of
				absolute ->
					case os:find_executable(Command) of
						false ->
							{error, "invalid cmd"};
						CMD_PATH ->
							exec_local_cmd(filename:dirname(CMD_PATH), CMD)
					end;
				relative ->
					R=filename:dirname(filename:join(tl(string:tokens(Command,[$/])))),
					case string:sub_string(Command, 1, 2) of
						"./" ->
							exec_local_cmd(filename:join([Path, R]), CMD);
						".." ->
							[H|T]=lists:reverse(tl(lists:reverse(string:tokens(Path, [$/])))),
		   					exec_local_cmd(filename:join([filename:join([H++"/"|T]), R]), CMD);
						_ ->
							{error, "invalid relative"}
					end;
				_ ->
					{error, "invalid path"}
			end;
		Error ->
			Error
   	end.

%% -----------------------------------------------------------------
%%@doc 运行本地指定名称和指定参数的可运行文件
%% @spec exec_local_parameter_cmd(Command, Parameter) -> return()
%% where
%% return() = list() | {error,Reason}
%%@end
%% -----------------------------------------------------------------
exec_local_parameter_cmd(Command, Parameter) ->
	CMD=filename:basename(Command),
	case file:get_cwd() of
		{ok, Path} ->
			case filename:pathtype(Command) of
				absolute ->
					case os:find_executable(Command) of
						false ->
							{error, "invalid cmd"};
						CMD_PATH ->
							exec_local_cmd(filename:dirname(CMD_PATH), lists:concat([CMD, " ", Parameter]))
					end;
				relative ->
					case os:find_executable(Command) of
						false ->
							R=filename:dirname(filename:join(tl(string:tokens(Command,[$/])))),
							case string:sub_string(Command, 1, 2) of
								"./" ->
									exec_local_cmd(filename:join([Path, R]), lists:concat([CMD, " ", Parameter]));
								".." ->
									[H|T]=lists:reverse(tl(lists:reverse(string:tokens(Path, [$/])))),
				   					exec_local_cmd(filename:join([filename:join([H++"/"|T]), R]), lists:concat([CMD, " ", Parameter]));
								_ ->
									{error, "invalid relative"}
							end;
						CMD_PATH ->
							exec_local_cmd(filename:dirname(CMD_PATH), lists:concat([CMD, " ", Parameter]))
					end;
				_ ->
					{error, "invalid path"}
			end;
		Error ->
		   Error
   	end.

%% -----------------------------------------------------------------
%%@doc 获取指定字符串的32位md5字符串值
%% @spec md5_str(Str::list()) -> list()
%%@end
%% -----------------------------------------------------------------
md5_str(Str) when is_list(Str) ->
	Md5_bin =  erlang:md5(xmerl_ucs:to_utf8(Str)), 
    Md5_list = binary_to_list(Md5_bin), 
    lists:flatten(list_to_hex(Md5_list)). 

%% -----------------------------------------------------------------
%%@doc 获取指定字符串和指定密钥的32位字符串值
%% @spec password(Str,Key::list()) -> list()
%%@end
%% -----------------------------------------------------------------
password(Str, Key) when is_list(Key) ->
	password1(Str, Key, none).

%% -----------------------------------------------------------------
%%@doc 快速判断一个列表是否是真正意义上的列表(例如不是字符串的列表)，注意列表过大，效率会降低
%% @spec is_lists(List::term()) -> boolean()
%%@end
%% -----------------------------------------------------------------
is_lists(List) ->
	case term_to_binary(List) of
		<<131, 108, _/binary>> ->
			true;
		_ ->
			false
	end.

%% -----------------------------------------------------------------
%%@doc 执行指定运行环境的脚本
%%```
%%  Value::term()
%%'''
%% @spec eval(Str, Binding) -> return()
%% where
%% return() = {value, Value, NewBindings}
%%@end
%% -----------------------------------------------------------------
eval(Str, Binding) ->
    {ok, Ts, _} = erl_scan:string(Str),
    Ts1 = case lists:reverse(Ts) of
              [{dot, _}|_] -> Ts;
              TsR -> lists:reverse([{dot, 1}|TsR])
          end,
    {ok, Expr} = erl_parse:parse_exprs(Ts1),
    erl_eval:exprs(Expr, Binding).

%% -----------------------------------------------------------------
%%@doc 在指定节点异步执行指定的MFA，超时则中断
%%```
%%  Val = (Res :: term()) | {badrpc, Reason :: term()}
%%'''
%% @spec rpc(Node, Mod, Fun, Args, Timeout::integer()) -> return()
%% where
%% return() = {value, Val} | Timeout
%%@end
%% -----------------------------------------------------------------
rpc(Node, Mod, Fun, Args, Timeout) when is_integer(Timeout) ->
	Key=rpc:async_call(Node, Mod, Fun, Args),
	rpc:nb_yield(Key, Timeout).

%% -----------------------------------------------------------------
%%@doc 执行一个简单的四则表达式
%% @spec arithmetic(Expre::list(), Context::list()) -> return()
%% where
%% return() = {value, Value, NewContext} | {error,Reason}
%%@end
%% -----------------------------------------------------------------
arithmetic(Expre, Context) when is_list(Expre), is_list(Context) ->
	{ok, Ts, _} = erl_scan:string(Expre),
    Ts1 = case lists:reverse(Ts) of
              [{dot, _}|_] -> Ts;
              TsR -> lists:reverse([{dot, 1}|TsR])
          end,
    case erl_parse:parse_exprs(Ts1) of
		{ok, Expr} ->
			case arithmetic_filter(Expr) of
				{ok, New_Expr} ->
					erl_eval:exprs(New_Expr, Context);
				Error ->
					{error, Error}
			end;
		Error ->
			Error
	end.

%% -----------------------------------------------------------------
%%@doc 写内部数据到文件
%% @spec write_to_file(File::list(), Term::list()) -> return()
%% where
%% return() = list() | ok | {error,Reason}
%%@end
%% -----------------------------------------------------------------
write_to_file(File, Term) when is_list(Term) ->
	case z_lib:check_list_range(Term, 0, 16#10ffff) of
        false ->
            write_list_to_file(File, Term);
        _ ->
            write_text_to_file(File, Term)
    end;
write_to_file(File, Term) ->
	write_term_to_file(File, Term).

%% -----------------------------------------------------------------
%%@doc 写字符串到文件
%% @spec write_text_to_file(File::list(),Text::list()) -> return()
%% where
%% return() = ok | {error,Reason}
%%@end
%% -----------------------------------------------------------------
write_text_to_file(File, Text) when is_list(File), is_list(Text) ->
	case file:open(File, [append, raw]) of
		{ok, Io} ->
			try
				case z_lib:is_iodata(Text) of
            		true ->
               			file:write(Io, Text);
		            false ->
		                file:write(Io, unicode:characters_to_binary(Text))
		        end
			catch
				Type:Reason ->
					{Type, Reason}
			after
				file:close(Io)
			end;
		Error ->
			Error
	end.

%% -----------------------------------------------------------------
%%@doc 写列表到文件
%% @spec write_list_to_file(File::list(),L::list()) -> list()
%%@end
%% -----------------------------------------------------------------
write_list_to_file(File, L) when is_list(File), is_list(L) ->
	case file:open(File, [write]) of
		{ok, Io} ->
			try
				lists:foreach(fun(X) -> io:format(Io, "~p~n", [X]) end, L)
			catch
				Type:Reason ->
					{Type, Reason}
			after
				file:close(Io)
			end;
		Error ->
			Error
	end.

%% -----------------------------------------------------------------
%%@doc 写内部数据到文件
%% @spec write_term_to_file(File::list(),Term::term()) -> ok
%%@end
%% -----------------------------------------------------------------
write_term_to_file(File, Term) when is_list(File) ->
	case file:open(File, [write]) of
		{ok, Io} ->
			try
				io:format(Io, "~p~n", [Term])
			catch
				Type:Reason ->
					{Type, Reason}
			after
				file:close(Io)
			end;
		Error ->
			Error
	end.

%% -----------------------------------------------------------------
%%@doc 将列表转换为字符串
%% @spec list_to_string(L::list(),Split::list()) -> string()
%%@end
%% -----------------------------------------------------------------
list_to_string(L, Split) when is_list(L), is_list(Split) ->
	list_to_string(L, Split, []).

%% -----------------------------------------------------------------
%%@doc 获取列表和元组的长度
%% @spec size(Term::type()) -> integer()
%% where
%% type() = list() | tuple() | binary()
%%@end
%% -----------------------------------------------------------------
size(Term) when is_list(Term) ->
	erlang:length(Term);
size(Term) when is_tuple(Term); is_binary(Term) ->
	erlang:size(Term);
size(_) ->
	0.

%%去重，保证原列表的元素顺序
unique(L) ->
	unique(lists:sort(L), [], lists:reverse(L)).

unique([TableKey|T], TableKey, L) ->
	unique(T, TableKey, lists:delete(TableKey, L));
unique([TableKey|T], _, L) ->
	unique(T, TableKey, L);
unique([], _, L) ->
	lists:reverse(L).

%% -----------------------------------------------------------------
%%@doc 删除指定文件
%% @spec del_file(File::list()) -> return()
%% where
%% return() = ok | {error, Reason}
%%@end
%% -----------------------------------------------------------------
del_file(File) ->
	case filelib:is_regular(File) of
		true ->
			file:delete(File);
		false ->
			{error, invalid_file}
	end.

%% -----------------------------------------------------------------
%%@doc 递归删除目录
%% @spec del_dir(Dir,boolean()) -> return()
%% where
%% return() = ok | {error, Reason}
%%@end
%% -----------------------------------------------------------------
del_dir(Dir, true) ->
	case filelib:is_dir(Dir) of
		true ->
			case file:list_dir(Dir) of
				{ok, Files} ->
					case del_dir1(Files, Dir) of
						ok ->
							file:del_dir(Dir);
						E ->
							E
					end;
				E ->
					E
			end;
		false ->
			{error, invalid_dir}
	end;
del_dir(Dir, false) ->
	case filelib:is_dir(Dir) of
		true ->
			file:del_dir(Dir);
		false ->
			{error, invalid_dir}
	end.

get_number_condition_vars(Condition) ->
	get_number_condition_vars1(Condition, []).

check_number_condition({'not', Exp}, Variables) ->
	bool_exec('not', check_number_condition(Exp, Variables), none);
check_number_condition({_, _, _} = Condition, Variables) ->
	check_number_condition_tuple(Condition, Variables);
check_number_condition({Operation, L}, Variables) when is_list(L) ->
	check_number_condition_list(L, Variables, Operation, none).

day_of_year(Second) when is_integer(Second) ->
	day_of_year(z_lib:second_to_localtime(Second));
day_of_year({{Year, _, _} = Date, {_, _, _}}) ->
	Start=calendar:date_to_gregorian_days({Year, 1, 1}),
	Tail=calendar:date_to_gregorian_days(Date),
	Tail-Start+1;
day_of_year({Year, _, _} = Date) ->
  	Start=calendar:date_to_gregorian_days({Year, 1, 1}),
	Tail=calendar:date_to_gregorian_days(Date),
	Tail-Start+1.

%%%=================LOCAL FUNCTIONS=================

get_data(Port, Sofar) ->
    receive
		{Port, {data, Bytes}} ->
		    get_data(Port, [Sofar|Bytes]);
		{Port, eof} ->
		    Port ! {self(), close}, 
		    receive
				{Port, closed} ->
				    true
		    end, 
		    receive
				{'EXIT',  Port,  Reason} -> 
				    Reason
		    after 1 ->				% force context switch
			    ok
		    end, 
	    	lists:flatten(Sofar)
    end.

validate(Atom) when is_atom(Atom) ->
    ok;
validate(List) when is_list(List) ->
    validate1(List).

validate1([C|Rest]) when is_integer(C), 0 =< C, C < 256 ->
    validate1(Rest);
validate1([List|Rest]) when is_list(List) ->
    validate1(List),
    validate1(Rest);
validate1([]) ->
    ok.

list_to_hex(L) -> 
    lists:map(fun(X) -> int_to_hex(X) end, L). 
 
int_to_hex(N) when N < 256 -> 
    [hex(N div 16), hex(N rem 16)].
 
hex(N) when N < 10 -> 
    $0+N; 
hex(N) when N >= 10, N < 16 ->      
    $a + (N-10).

password1([], _, _) ->
	"";
password1(_, [], MD5) ->
	MD5;
password1(Str, [H|T], MD5) when is_integer(H), H>=0, H=<255 ->
	password1(Str, T, md5_str(lists:concat([Str, " ", H, " ", MD5]))).

arithmetic_filter([{match, Line0, {var, Line1, Var}, {op, _, _, _, _} = Operation}]) when is_atom(Var) ->
	case op_filter(Operation) of
		{ok, New_Operation} ->
			{ok, [{match, Line0, {var, Line1, Var}, New_Operation}]};
		Error ->
			Error
	end;
arithmetic_filter([{match, Line0, {var, Line1, Var}, {call, _, _, _} = Call}]) when is_atom(Var) ->
	case call_filter(Call, []) of
		{ok, New_Call} ->
			{ok, [{match, Line0, {var, Line1, Var}, New_Call}]};
		Error ->
			Error
	end;
arithmetic_filter([{op, _, _, _, _} = Operation]) ->
	case op_filter(Operation) of 
		{ok, New_Operation} ->
			{ok, [New_Operation]};
		Error ->
			Error
	end;
arithmetic_filter([{call, _, _, _} = Call]) ->
	case call_filter(Call, []) of
		{ok, New_Call} ->
			{ok, [New_Call]};
		Error ->
			Error
	end;
arithmetic_filter(Arith) ->
	{invalid_arithmetic, Arith}.

op_filter({op, Line, OP, One, Two}) ->
	case args_filter(One) of
		{ok, New_One} ->
			case args_filter(Two) of
				{ok, New_Two} ->
					{ok, {op, Line, OP, New_One, New_Two}};
				Error ->
					Error
			end;
		Error ->
			Error
	end;
op_filter(OP) ->
	{invalid_op, OP}.

call_filter({call, Line, {atom, _, _} = F, [Arg|T]}, L) ->
	case args_filter(Arg) of
		{ok, New_Arg} ->
			call_filter({call, Line, F, T}, [New_Arg|L]);
		Error ->
			Error
	end;
call_filter({call, Line, {atom, _, _} = F, []}, L) ->
	Args=lists:reverse(L),
	case call_trans(F) of
		{ok, New_F} ->
			{ok, {call, Line, New_F, Args}};
		_ ->
			{ok, {call, Line, F, Args}}
	end;
call_filter(Call, _) ->
	{invalid_call, Call}.

call_trans({atom, Line, pi}) ->
	{ok, {remote, Line, {atom, Line, math}, {atom, Line, pi}}};
call_trans({atom, Line, fac}) ->
	{ok, {remote, Line, {atom, Line, ?MODULE}, {atom, Line, fac}}};
call_trans({atom, Line, ladd}) ->
	{ok, {remote, Line, {atom, Line, ?MODULE}, {atom, Line, ladd}}};
call_trans({atom, Line, lmul}) ->
	{ok, {remote, Line, {atom, Line, ?MODULE}, {atom, Line, lmul}}};
call_trans({atom, Line, pow}) ->
	{ok, {remote, Line, {atom, Line, math}, {atom, Line, pow}}};
call_trans({atom, Line, sqrt}) ->
	{ok, {remote, Line, {atom, Line, math}, {atom, Line, sqrt}}};
call_trans({atom, Line, lg}) ->
	{ok, {remote, Line, {atom, Line, math}, {atom, Line, log}}};
call_trans({atom, Line, log}) ->
	{ok, {remote, Line, {atom, Line, math}, {atom, Line, log10}}};
call_trans({atom, Line, sin}) ->
	{ok, {remote, Line, {atom, Line, math}, {atom, Line, sin}}};
call_trans({atom, Line, cos}) ->
	{ok, {remote, Line, {atom, Line, math}, {atom, Line, cos}}};
call_trans({atom, Line, tan}) ->
	{ok, {remote, Line, {atom, Line, math}, {atom, Line, tan}}};
call_trans(_) ->
	break.

args_filter([]) ->
	{ok, []};
args_filter({integer, _, _} = Arg) ->
	{ok, Arg};
args_filter({float, _, _} = Arg) ->
	{ok, Arg};
args_filter({var, _, _} = Arg) ->
	{ok, Arg};
args_filter({op, _, _, _, _} = Operation) ->
	op_filter(Operation);
args_filter({call, _, _, _} = Call) ->
	call_filter(Call, []);
args_filter(Args) ->
	{invalid_args, Args}.
	
fac(1) -> 
	1; 
fac(N) -> 
	N * fac(N - 1).

ladd([]) ->
	0;
ladd([H|T]) ->
	H + ladd(T).

lmul([]) ->
	0;
lmul(L) when is_list(L) ->
	lmul1(L).
	
lmul1([]) ->
	1;
lmul1([H|T]) ->
	H * lmul1(T).

list_to_string([H|T], Split, []) ->
	list_to_string(T, Split, lists:concat([H]));
list_to_string([H|T], Split, L) ->
	list_to_string(T, Split, lists:concat([L, Split, H]));
list_to_string([], _, L) ->
	L.


del_dir1([], _) ->
	ok;
del_dir1(["."|T], Path) ->
	del_dir1(T, Path);
del_dir1([".."|T], Path) ->
	del_dir1(T, Path);
del_dir1([H|T], Path) ->
	File=filename:join([Path, H]),
	case del_file(File) of
		ok ->
			del_dir1(T, Path);
		{error, invalid_file} ->
			case del_dir(File, true) of
				ok ->
					del_dir1(T, Path);
				E ->
					E
			end;
		E ->
			E
	end.

get_number_condition_vars1({_, A, B}, L) ->
	NL=if
		   is_atom(A) ->
			   [{A, 0}|L];
		   true ->
			   L
	   end,
	NL1=if
			is_atom(B) ->
			   [{B, 0}|NL];
			true ->
				NL
		end,
	NL1;
get_number_condition_vars1({_, List}, L) ->
	get_number_condition_vars_(List, L).

get_number_condition_vars_([Exp|T], L) ->
	get_number_condition_vars_(T, get_number_condition_vars1(Exp, L));
get_number_condition_vars_([], L) ->
	L.
  
fill_var(Var, Variables) when is_atom(Var) ->
	case lists:keyfind(Var, 1, Variables) of
		{Var, Val} ->
			Val;
		false ->
			Var
	end;
fill_var(Var, _Variables) ->
	Var.

check_number_condition_tuple({Operation, A, B}, _Variables) when is_number(A), is_number(B) ->
	number_exec(Operation, A, B);
check_number_condition_tuple({Operation, A, B}, Variables) ->
	NA=case A of
		   {_, _, _} = A1 ->
			   check_number_condition_tuple(A1, Variables);
		   A when is_atom(A) ->
			   fill_var(A, Variables);
		   A when is_number(A) ->
			   A
	   end,
	NB=case B of
		   {_, _, _} = B1 ->
			   check_number_condition_tuple(B1, Variables);
		   B when is_atom(B) ->
			   fill_var(B, Variables);
		   B when is_number(B) ->
			   B
	   end,
	check_number_condition_tuple({Operation, NA, NB}, Variables).

check_number_condition_list([Exp|T], Variables, Operation, Boolean) when is_boolean(Boolean) ->
	check_number_condition_list(T, Variables, Operation, bool_exec(Operation, check_number_condition(Exp, Variables), Boolean));
check_number_condition_list([Exp|T], Variables, Operation, none) ->
	NewBoolean=case Operation of
				   'not' ->
					   not check_number_condition(Exp, Variables);
				   _ ->
					   check_number_condition(Exp, Variables)
	end,
	check_number_condition_list(T, Variables, Operation, NewBoolean);
check_number_condition_list([], _Variables, _Operation, Boolean) ->
	Boolean.
	
number_exec('>', A, B) when A > B ->
	true;
number_exec('>', _A, _B) ->
	false;
number_exec('<', A, B) when A < B ->
	true;
number_exec('<', _A, _B) ->
	false;
number_exec('=', A, B) when A =:= B ->
	true;
number_exec('=', _A, _B) ->
	false;
number_exec('!=', A, B) when A =/= B ->
	true;
number_exec('!=', _A, _B) ->
	false;
number_exec('>=', A, B) when A >= B ->
	true;
number_exec('>=', _A, _B) ->
	false;
number_exec('=<', A, B) when A =< B ->
	true;
number_exec('=<', _A, _B) ->
	false;
number_exec('+', A, B) ->
	A + B;
number_exec('-', A, B) ->
	A - B;
number_exec('*', A, B) ->
	A * B;
number_exec('/', A, B) ->
	A / B;
number_exec('div', A, B) ->
	A div B;
number_exec('rem', A, B) ->
	A rem B.

bool_exec('and', A, B) ->
	A and B;
bool_exec('or', A, B) ->
	A or B;
bool_exec('not', A, _B) when is_boolean(A) ->
	not A.