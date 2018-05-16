%% @author Administrator
%% @doc 参数类型是字符串指针时，如果为空串，则C层为NULL，返回值如果需要返回字符串而不是字符指针，则需要使用char#;N, N为字符缓冲区大小.

-module(lib_npc).
-behaviour(gen_server).
-define(NPCTIMEOUT, 5000).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(DEFAULT_MAX_HEAP_SIZE, 32 * 1024 * 1024).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1,register/5,register/2,release_resource/0,reload_module/2]).

start_link(Param) ->
	gen_server:start_link({local,?MODULE}, ?MODULE, Param, []).

register(ModuleName,StrRet, FunName, ParamInfo, Syn) ->
	
	gen_server:call(?MODULE, {register,ModuleName,StrRet, FunName, ParamInfo, Syn}, ?NPCTIMEOUT).


register(ParamInfoList, Timeout) ->
	gen_server:call(?MODULE,{register,ParamInfoList},Timeout).

release_resource() ->
    gen_server:call(?MODULE,{release_resource}, ?NPCTIMEOUT).

reload_module(FileName,Timeout) ->
	gen_server:call(?MODULE,{reload_module,FileName},Timeout).




%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {}).

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
init(Param) ->
	z_process:local_process(?MODULE, ?DEFAULT_MAX_HEAP_SIZE, false, true),
	case erl_vcall:init() of
		ok ->
			case create_table() of
				true ->
					{ok,#state{}};
			_ ->
				{error,create_table_error}
		     end;
		Ret ->
			 Ret
	end.



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
handle_call({register,ModuleName, StrRet,FunName, ParamInfo, Syn}, From, State) ->	
 
	{reply,register_imp_pre(ModuleName, StrRet,FunName, ParamInfo,Syn),State};


handle_call({register,ParamInfoList},From, State) ->
	{reply,register_imp_pre(ParamInfoList),State};

handle_call({release_resource}, From, State) ->
	{reply,release_resource_imp(),State};

handle_call({reload_module,FileName},From, State) ->
	{reply, reload_module_imp(FileName),State}.




register_imp_pre(ModuleName, StrRet,FunName, ParamInfo,Syn)->	
	StrRet2 = parse_ret_info(string:strip(StrRet), string:strip(ParamInfo)),
	ParamInfo2 = parse_paramInfo(ParamInfo), 
	register_imp(ModuleName,StrRet2,FunName,ParamInfo2, Syn).


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
handle_info(Info, State) ->
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



%% -----------------------------------------------------------------
%%@doc 批量注册函数 
%% @spec register(ParamInfoList::list()) -> return()
%% where
%% return() = list()
%%@end
%% -----------------------------------------------------------------


register_imp_pre([]) ->
	[];
register_imp_pre([H|L]) ->	   
	 {ModuleName,StrRet,FunName,ParamInfo,Syn} = H,	     
     [register_imp_pre(ModuleName,StrRet,FunName,ParamInfo,Syn)|register_imp_pre(L)].


register_imp([]) ->
	[];
register_imp([H|L]) ->	   
	 {ModuleName,StrRet,FunName,ParamInfo,Syn} = H,	     
     [register_imp(ModuleName,StrRet,FunName,ParamInfo, Syn)|register_imp(L)].



%% -----------------------------------------------------------------
%%@doc 生存动态代码字符串 
%% @spec generate_code(ParamType::string,ModuleName::string,FunName::string,Index::Number) -> return()
%% where
%%  return() = string()
%%@end
%% -----------------------------------------------------------------



%% -----------------------------------------------------------------
%%@doc 生存动态代码字符串 
%% @spec insert_code_table(ModuleName,CodeStr,ParamList,ParamList2,Index) -> return()
%% where
%%  return() = string()
%%@end
%% -----------------------------------------------------------------

insert_code_table(ModuleName,CodeStr,ParamList) ->
%%    ModuleDllName = ModuleName ++ ".dll",
    try ets:insert(?MODULE,{list_to_atom(ModuleName),CodeStr,ParamList}) of
        Val ->         
           ok
    catch
        _:_ ->
           {error,insert_table_fail}
    end.

%% -----------------------------------------------------------------
%%@doc 生存动态代码字符串 
%% @spec generate_dynamic_code(CodeStr) -> return()
%% where
%%  return() = atom()
%%@end
%% -----------------------------------------------------------------
generate_dynamic_code(CodeStr) ->
	try dynamic_compile:load_from_string(CodeStr) of
		{_,Mod} -> ok
	catch
		_:_ ->
			false
	end.


register_imp(ModuleInfo,StrRet,FunName,ParamInfo,Syn) ->
	
	ModuleName = get_module_name(ModuleInfo),
	case erl_vcall:fun_register(ModuleInfo,StrRet,FunName,ParamInfo) of
		{1,Index} when Index > 0 ->	

					 CodeStr = generate_code(ModuleName,ParamInfo,FunName,ModuleInfo,Syn),
					 ParamList = get_paramlist(ModuleName),
					 ParamList2 = [{ModuleInfo, FunName, ParamInfo,StrRet,Syn}],				
					 case generate_dynamic_code(CodeStr) of 
						 ok ->
							 insert_code_table(ModuleName,CodeStr,lists:merge(fun(A,B) -> true end,ParamList, ParamList2));
						 _ ->
							%% io:format("ModuleInfo:~w",[ModuleInfo]),
							io:format("~w ~n",[CodeStr]),
							 {error, generate_dynamic_code_fail}
					 end;					 
	
		 {1,_} -> 
			 {ok, fun_existed};
		 {0, ErrorCode} ->
			 {error, ErrorCode}
	 end.	
    
  


get_paramlist(ModuleName) ->
	case ets:lookup(?MODULE,list_to_atom(ModuleName)) of
		[{_,_,L}] ->
			L;
		_ ->
			[]
	end.


%% -----------------------------------------------------------------
%%@doc 创建ets表
%% @spec create_table() -> return()
%% where
%%  return() = atom()
%%@end
%% ----------------------------------------------------------------

create_table() ->
	try ets:new(?MODULE,[public,named_table]) of
	    Val -> true
	catch
		_:_ -> false
	end.

%% -----------------------------------------------------------------
%%@doc 获取模块名称
%% @spec get_module_name(FileName:string())-> return()
%% where
%%  return() = string()
%%@end
%% ----------------------------------------------------------------

get_module_name(FileName) ->
	case os:type() of
		{win32,_} ->
			Index = string:rstr(FileName, "\\"),
            Index2 = string:rstr(FileName,".dll"),				   
			if 
			   	(Index > 0) and (Index2 > Index)  ->
					
               		string:strip(string:sub_string(FileName, Index+1, Index2-1));
                Index > 0 ->
					string:strip(string:substr(FileName, Index+1));

                Index2 > 0 ->
                    string:strip(string:substr(FileName, 1, Index2-1));

                true ->
                    FileName
             end;	   
		
		{_,_} ->
			Index = string:rstr(FileName, "/"),
            Index2 = string:rstr(FileName,".so"),				   
			if 
				(Index > 0) and (Index2 > Index) ->
						string:strip(string:sub_string(FileName, Index+1, Index2-1));
                Index > 0 ->						   
						string:strip(string:substr(FileName, Index+1));					   
				Index2 > 0 ->
						string:strip(string:substr(FileName, 1, Index2-1));	
                true ->
                         FileName
            end 		
	end.
	
	

reload_module_imp(FileName) ->
	ModuleName = get_module_name(FileName),	
	ModuleNameLen = string:len(ModuleName),	
	
	if
		ModuleNameLen > 0 ->
	       Ret = erl_vcall:reload_dll(ModuleName),	
		   {OsType,_} = os:type(),
		   if
			   OsType =:= win32 ->
	       		   file:delete(ModuleName++".dll"),
	       		   file:copy(FileName,ModuleName++".dll");
			   true ->
				   file:delete(ModuleName++".so"),
	       		   file:copy(FileName,ModuleName++".so")
			end,
			   
	       ParamList = get_paramlist(ModuleName),
	       ets:delete(?MODULE, list_to_atom(ModuleName)),
		   if
			   (Ret =:= 1) and (erlang:length(ParamList) > 0) ->
				   register_imp(ParamList);
			   true ->
				   []
		   end,
		   {ok, reload_sucess};
		true ->
            {error,module_name_invalid}
	end.

%% -----------------------------------------------------------------
%%@doc 释放资源
%% @spec release_resource_imp()-> return()
%% where
%%  return() = tupple()
%%@end
%% ----------------------------------------------------------------	

release_resource_imp() ->
	ets:delete(?MODULE),
    try erl_vcall:release_dll() of
	    Val -> {ok,Val}
	catch
		_:_ -> {error,release_failed}
	end.

get_arr_dim(ArrStr)->
	
	case ArrStr of 
	"####" ->
		4;
	"###" ->
		3;
	"##"->
		2;
	"#" ->
        1;
	_ ->
		0
		
	end.


parse_arr_bind_args(RetInfo,ParamInfo,Index) ->
	
	TmpStr = string:strip(string:sub_string(RetInfo, Index+1, string:len(RetInfo))),
	
	{Index2,_} = string:to_integer(TmpStr),
	
	L = string:tokens(ParamInfo,","),	
	
	StrType = string:strip(lists:nth(Index2, L)),	
	
	Index3 = string:str(StrType,"#"),
	
	Dim = get_arr_dim(string:strip(string:substr(StrType,Index3,string:len(StrType)))),
	
	StrType2 = string:strip(string:substr(StrType, 1, Index3-1)),

    NType = get_param_type(string:strip(StrType2)),	

	Index4 = string:str(StrType2,"char"),
	
	if 
		Index4 > 0 ->			
			[21,Index2,Dim-1];
		true ->
			[NType,Index2,Dim]
	end.
   


parse_arr_ret_args(RetInfo,Index)->		
	
	RetArrInfo = string:strip(string:sub_string(RetInfo, 1, Index-1)),
	
	Index2 = string:str(RetArrInfo, "#"),	
	
	StrRet = string:strip(string:sub_string(RetArrInfo,1, Index2-1)),	
	
	Dim = case string:tokens(RetArrInfo, StrRet) of
		[R] ->
			get_arr_dim(R);
		R ->
			get_arr_dim(R)
	end,
	
	ArrLen = string:strip(string:sub_string(RetInfo,Index+1, string:len(RetInfo))),	
	
	ArrLenList = lists:map((fun(X) -> {Ret,_} = string:to_integer(X),Ret end),lists:reverse(string:tokens(ArrLen,";"),[])),
	
	NType = get_param_type(string:strip(StrRet)),
	
	Index4 = string:str(StrRet,"char"),
	
	if 
		Index4 > 0 ->			
			[21,Dim-1|ArrLenList];
		true ->
			[NType,Dim|ArrLenList]
	end.    


parse_arr_ret(RetInfo,ParamInfo,Index) -> 
	
	Index2 = string:str(RetInfo, "args"),
	
	if 
		Index2 > 0 ->
			
			parse_arr_bind_args(RetInfo,ParamInfo,Index);
		
		true ->
			
			parse_arr_ret_args(RetInfo,Index)
	
	end.



parse_ret_info(RetInfo, ParamInfo) ->
	TmpStr = string:strip(RetInfo),
	StrLen = string:len(TmpStr),
	Index = string:str(TmpStr, ";"),
	
	if 
		StrLen < 1 ->
			
			[0];
		Index < 1 ->
			
			[get_param_type(TmpStr)];
		    
		
		true ->
			
			parse_arr_ret(RetInfo,string:strip(ParamInfo),Index)
		
	end.

pre_process_paramInfo(ParamList,N) when N =:= length(ParamList)->
	
	ParamList;  
	
pre_process_paramInfo(ParamList,N)->
	
	Item = string:strip(lists:nth(N, ParamList)),
	
	case is_arr(Item) of
		
		false ->
			pre_process_paramInfo(ParamList,N+1);			
	    _-> 
			BindStr = get_bind_str(Item),
			BindList = get_param_bind_index(BindStr),
			[Bind1,Bind2,Bind3,Bind4] = BindList,		
			List1 = modify_list_by_value(ParamList,Bind1,N),
			List2 = modify_list_by_value(List1,Bind2,N),
			List3 = modify_list_by_value(List2,Bind3,N),
			List4 = modify_list_by_value(List3,Bind4,N),
			pre_process_paramInfo(List4,N+1)
	end.

modify_list_by_value(ParamList,Index,Value) ->
	
	case Index =:= 0 of
		true ->
			ParamList;		
		_->		
	Item = string:strip(lists:nth(Index, ParamList)),	
	ValueStr = integer_to_list(Value),
	Item2 = lists:append([Item,"$",ValueStr]),
	List1  = lists:sublist(ParamList, Index-1),
	List2  = lists:sublist(ParamList, Index+1,length(ParamList)-Index),
	lists:append([List1,[Item2],List2])
	end.
	
  
pre_process_paramInfo(ParamList)->
	
	Len = length(ParamList),
	
	if 
		Len < 2 ->
		  
		  ParamList;
		
        true ->
          pre_process_paramInfo(ParamList,1)
   end.


parse_paramInfo(ParamInfo) ->
	TmpStr = string:strip(ParamInfo),
    ParamList = string:tokens(TmpStr, ","),
	ParamList2 = pre_process_paramInfo(ParamList),
    parse_param(ParamList2).


get_param_bind_index(BindStr) -> 
	
	BindList = string:tokens(BindStr,";"),
	
	BindLen = length(BindList),

    BindList2 = lists:map((fun(X) -> {Ret,_} = string:to_integer(X),Ret end),BindList),

    if 
		BindLen =:= 0 ->
            [0,0,0,0];
        BindLen =:= 1 ->
            lists:append(BindList2,[0,0,0]);
           
        BindLen =:= 2 ->
			lists:append(BindList2,[0,0]);

        BindLen =:= 3-> 
            lists:append(BindList2,[0]);
        true ->
			{List2,_} =lists:split(4, BindList2),
            List2
      end.

get_bind_str(ParamInfo)->
	Index = string:str(ParamInfo,":"),	
	if 
		Index < 1 ->
			"";
		true->
			string:strip(string:substr(ParamInfo,1,Index-1))
	end.

get_type_str(ParamInfo)->
	Index1 = string:str(ParamInfo,":"),
	Index2 = string:rstr(ParamInfo,"#"),
	
	SubStr = string:strip(string:substr(ParamInfo, Index1+1, Index2)),
    SubStr2 = string:strip(string:substr(ParamInfo, Index2+1)),
	
	Index3 = string:str(SubStr2,";"),
	
	if
		Index3 > 0 ->
		  SubStr3 =	string:substr(SubStr2,1,Index3),
		  string:concat(SubStr, SubStr3);
		true ->
		  SubStr
	end.

get_len_str(ParamInfo) ->
	
	Index = string:rstr(ParamInfo,"#"),
	
	SubStr = string:strip(string:substr(ParamInfo, Index+1)),

    Index2 = string:str(SubStr,";"),

    string:substr(SubStr, Index2+1).
    
	
	

split_arr_info(ParamInfo) ->
	
	[get_bind_str(ParamInfo),get_type_str(ParamInfo),get_len_str(ParamInfo)].

   

get_arr_param_info(ParamInfo)->
	
   [BindStr,TypeInfo,LengthInfo] = split_arr_info(string:strip(ParamInfo)),   
    BindList = get_param_bind_index(BindStr),
    TypeList = get_arr_type(TypeInfo,string:len(LengthInfo)=:=0),
    LenList = get_arr_length(LengthInfo),  
    lists:append([TypeList,LenList,BindList]).


get_arr_length(LengthInfo) ->							 
	LenList = lists:map((fun(X) -> {Ret,_} = string:to_integer(X),Ret end),lists:reverse(string:tokens(LengthInfo,";"))),
	
	Len = length(LenList),
	
	if 
		Len =:= 0 ->
			[0,0,0,0];
		Len =:= 1 ->
			lists:append(LenList, [0,0,0]);
		Len =:= 2 ->
			lists:append(LenList, [0,0]);
		Len =:= 3 ->
			lists:append(LenList, [0]);
		Len =:= 4 ->
			{List2,_} =lists:split(4, LenList),
            List2
	end.

get_arr_type(TypeInfo,SetLength) ->	
	Index = string:str(TypeInfo,"#"),
	Index2 = string:rstr(TypeInfo, "#"),
	
	Dim = get_arr_dim(string:strip(string:sub_string(TypeInfo,Index,Index2))),
	StrType = string:strip(string:sub_string(TypeInfo,1, Index-1)),
	Param_style = param_style(string:strip(string:sub_string(TypeInfo, Index2)),SetLength),
	[return_type(StrType),Param_style,Dim].		


	   
     
is_virture_param(ParamInfo) ->	
	
	Index = string:str(ParamInfo,"$"),
	
	if 
		Index > 0 ->
			
			true;
		true ->
			false
		
	end.
	
	
parse_virture_param(ParamInfo) ->	
	
	Index = string:str(ParamInfo, "$"),
	
	SubStr = string:strip(string:substr(ParamInfo, 1, Index-1)),
	
	{BindValue,_} = string:to_integer(string:strip(string:substr(ParamInfo,Index+1))),
	
	[return_type(SubStr),BindValue].
   

	
get_param_type(ParamInfo) ->
	
		
    case is_arr(ParamInfo) of 
		
		true ->
			
			get_arr_param_info(ParamInfo);

		_ ->
			
			case is_virture_param(ParamInfo) of 
				
				true->
					
					parse_virture_param(ParamInfo);
				
			    _ ->
					
				
		    	return_type(ParamInfo)
					
			
			end 
			
	end.		



parse_param([]) ->
	[];
parse_param([H|L]) ->	
	[get_param_type(string:strip(H))|parse_param(L)].
	

param_style(ParamInfo,SetLength) -> 
	
	Index = string:str(ParamInfo,";"),
    if 
		Index < 1 -> 
			1;
		
		true ->
			
			case SetLength of
					
					true ->
                           3;
                    
		            _ ->
						2
		   end
	end.

	

is_arr(ParamInfo) ->
	Index = string:str(ParamInfo,"#"),
	
	if 
		Index > 0 ->
			true;
		true->
			false
		
	end.


return_type(TypeStr) ->
	
	Index = string:str(TypeStr,"*"),	
	if 
		Index > 0 ->
			14;
		
		true ->			
				
            case TypeStr of
                "void" ->
                    0;
                "int" ->
                    1;
                "float" ->
                    2;
                "double" ->
                    3;
                "long double" ->
                    4;
                "ubyte" ->
                    5;
                "unsigned char" ->
                    5;
                "bool" ->
                    5;
                "uint8" ->
                    5;
                "char" ->
                    6;		
                "byte" ->
                    6;
                "signed char" ->
                    6;
                "int8" ->
                    6;
                "unsigned short" ->
                    7;
                "wchar_t" ->
                    7;
                "uint16" ->
                    7;
                "short" ->
                    8;
                "int16" ->
                    8;
                "unsigned int" ->
                    9;
                "unsigned" ->
                    9;
                "uint32" ->
                    9;
                "int32" ->
                    10;
                "uint64" ->
                    11;                
                "int64" ->
                    12;                    
                "unsigned long" ->
                    16;
                "long" ->
                    17;
                "long int" ->
                    17;
                "signed long" ->
                    17;	
                
                "signed long long" ->
                    18;	
                
                "unsigned long long" ->
                    19;
                "long long" ->
                    18;
            
                _  ->
                14
            end
    end.



%% -----------------------------------------------------------------
%%@doc 填充参数列表
%% @spec fill_param_list(Num::number) -> return()
%% where
%%  return() = list()
%%@end
%% ----------------------------------------------------------------
fill_param_list(0) ->
	[];
fill_param_list(Num) ->
  ["A"++ erlang:integer_to_list(Num)| fill_param_list(Num -1)].

%% -----------------------------------------------------------------
%%@doc 创建模块信息
%% @spec createModuleInfo(ModuleName::string) -> return()
%% where
%%  return() = string()
%%@end
%% ----------------------------------------------------------------
createModuleInfo(ModuleName) ->
	"-module(" ++ ModuleName ++ ").\n".


%% -----------------------------------------------------------------
%%@doc 创建函数导出信息
%% @spec createModuleInfo(ModuleName::string) -> return()
%% where
%%  return() = string()
%%@end
%% ----------------------------------------------------------------
createExportInfo(FunName, ParamLength,Syn) -> 
	case Syn of 
		asyn ->
			FunName++"/" ++erlang:integer_to_list(ParamLength+1);
	    _ ->
			FunName++"/" ++erlang:integer_to_list(ParamLength)
    end.
	
 createFunAsyn(ModuleName,FunName, ParamLength)  ->
	
	ParamName = fill_param_list(ParamLength+1), 
	[P|L] = ParamName,
	FunName ++"(" ++ string:join(ParamName,",") ++ ") when is_pid(A" ++ erlang:integer_to_list(ParamLength+1) ++ ") ->\n"
    ++"FunName=\"" ++FunName ++ "\",\n"  
    ++"Paramters=["++ string:join(lists:nthtail(1,ParamName),",") ++"],\n"       
    ++"case erl_vcall:asyn_invoke(\""++ModuleName++"\",\""++FunName++"\","++P++",Paramters) of\n"
    ++"{1,Result} ->\n"
    ++"{ok,Result};\n"
    ++"{0,ErrorCode} ->\n"
    ++"{error,ErrorCode};\n"
    ++"_ ->\n"
    ++"ok\n"
    ++"end.\n".

%% -----------------------------------------------------------------
%%@doc 生成函数源码
%% @spec createFun(ModuleName::string(), FunName::string, ParamType:list()) -> return()
%% where
%%  return() = string()
%%@end
%% ----------------------------------------------------------------
createFun(ModuleName,FunName, ParamLength) ->	
	ParamName = fill_param_list(ParamLength),
	FunName ++"(" ++ string:join(ParamName,",") ++ ") ->\n"
    ++"FunName=\"" ++FunName ++ "\",\n"  
    ++"Paramters=["++ string:join(ParamName,",") ++"],\n"       
    ++"case erl_vcall:invoke(\""++ModuleName++"\",\""++FunName++"\",Paramters) of\n"
    ++"{1,Result} ->\n"
    ++"{ok,Result};\n"
    ++"{0,ErrorCode} ->\n"
    ++"{error,ErrorCode};\n"
    ++"_ ->\n"
    ++"ok\n"
    ++"end.\n".

%% -----------------------------------------------------------------
%%@doc 获取模块中存在的代码
%% @spec get_precode(ModuleName::string) -> return()
%% where
%%  return() = string()
%%@end
%% ---------------------------------------------------------------- 

get_precode(ModuleName) ->
	case ets:lookup(?MODULE, list_to_atom(ModuleName)) of
		[{_,Str,_}] ->
			Str;
		_ ->
			""
	end.

%% -----------------------------------------------------------------
%%@doc 判断代码字符串的合法性
%% @spec is_validCode(Code::string) -> return()
%% where
%%  return() = Number()
%%@end
%% ---------------------------------------------------------------- 
is_valid_code(Code) ->
	NstrLen = string:len(Code),
	if
		 NstrLen < 1 ->
			 0;
		 true ->
			 Index = string:str(Code,"-export(["),
			 if
				 Index < 1 ->
					 0;
				 true ->
					 Index2 =string:str(string:substr(Code,Index),"])"),
					 if
				         Index2 < 1 -> 
							 0;
                         true ->
                             1
                     end
              end
	 end.


get_param_length([]) ->
	0;

get_param_length([H|L]) ->
	
	case is_list(H) of
		
		true -> 
			
			Len = length(H),
			[_,Style|L2] = H,
			
			if 
				Len =:= 2 ->
					0+get_param_length(L);
				
				true ->
					case Style =:= 2 of  
						
						true ->
							
							0+get_param_length(L);
						
						_ ->
							
							1+get_param_length(L)
					 end
			  end;			
		_ ->
           1+get_param_length(L)
	end.
	
createFunImp(ModulePath,FunName,ParamLength, Syn) ->
	
	case Syn of 
		asyn ->
			createFunAsyn(ModulePath,FunName,ParamLength);
		_ ->
			createFun(ModulePath,FunName,ParamLength)
	end.


generate_code(ModuleName,ParamType,FunName,ModulePath,Syn)->	
    ParamLength = get_param_length(ParamType),
	ModuleInfo = createModuleInfo(ModuleName),
	ExportInfo = createExportInfo(FunName,ParamLength,Syn),
	FunStr = createFunImp(ModulePath,FunName,ParamLength, Syn),	
	PreCode = get_precode(ModuleName),	
    BValid = is_valid_code(PreCode),
	if 
		BValid =:= 0 ->
			ModuleInfo ++ "-export([" ++ExportInfo ++ "]).\n" ++ FunStr;
        true ->			
			Index1 = string:str(PreCode,"-export(["),
		    Index2 = string:str(PreCode,"])"),
			SubLen = string:len("-export(["),
			TmpStr = string:substr(PreCode, Index1 + SubLen, Index2 - Index1-SubLen),
			Len = string:len(string:strip(TmpStr)),
			if
				Len > 0 ->
					TmpStr2 = TmpStr ++ "," ++ ExportInfo,
				    string:substr(PreCode, 1, Index1 + SubLen-1) ++ TmpStr2 ++ string:substr(PreCode, Index2) ++ FunStr;
				true ->
					string:substr(PreCode, 1, Index1 + SubLen -1) ++ ExportInfo ++ string:substr(PreCode, Index2) ++ FunStr
			end	
			
	end.