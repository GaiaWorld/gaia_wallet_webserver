%%@doc 文件监控器
%%```
%%%配置表的格式为：{文件或目录, MFAI(I是检查时间间隔)}
%%%内存表的格式为：{文件或目录, [MFAI(I是检查时间间隔)], 最小检查时间间隔, 下次检查时间, 文件信息或文件信息列表, Pid}
%%'''
%%@end

-module(zm_file_monitor).

-description("file monitor").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/0]).
-export([get/1, set/2, set/3, delete/1, delete/2]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(TIMEOUT, 3000).
-define(INTERVAL_TIME, 3).
-define(DIR_DEPTH, 256).

%%%=======================RECORD=======================
-record(state, {ets}).

%%%=======================INCLUDE=======================
-include_lib("kernel/include/file.hrl").

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Function: start_link/0
%% Description: Starts a file monitor
%% Returns: {ok, Pid} | {error, Reason}
%% -----------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, ?MODULE, []).

%% -----------------------------------------------------------------
%%@doc 获取文件修改事件处理器
%% @spec get(File::list()) -> return()
%% where
%% return() =  none | {File, {M ,F, A, I}}
%%@end
%% -----------------------------------------------------------------
get(File) ->
	case ets:lookup(?MODULE, File) of
		[{File, MFAI, _, _, _, _}] -> {File, MFAI};
		[] -> none
	end.

%% -----------------------------------------------------------------
%%@doc 设置文件修改事件处理器
%% @spec set(File::list(),MFA::tuple()) -> ok
%%@end
%% -----------------------------------------------------------------
set(File, {_M, _F, _A} = MFA) when is_list(File) ->
	gen_server:call(?MODULE, {set, File, MFA}).

%% -----------------------------------------------------------------
%%@doc 设置文件修改事件处理器
%% @spec set(File::list(),MFA::tuple(),Interval::integer()) -> ok
%%@end
%% -----------------------------------------------------------------
set(File, {_M, _F, _A} = MFA, Interval) when is_list(File), is_integer(Interval), Interval > 0 ->
	gen_server:call(?MODULE, {set, File, MFA}).

%% -----------------------------------------------------------------
%%@doc 删除文件修改事件处理器
%% @spec delete(File::list()) -> return()
%% where
%% return() =  none | {ok, Old}
%%@end
%% -----------------------------------------------------------------
delete(File) ->
	gen_server:call(?MODULE, {delete, File}).

%% -----------------------------------------------------------------
%%@doc 删除文件修改事件处理器
%% @spec delete(File::list(),MFA::tuple()) -> return()
%% where
%% return() =  ok | none
%%@end
%% -----------------------------------------------------------------
delete(File, MFA) ->
	gen_server:call(?MODULE, {delete, File, MFA}).

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init(Ets) ->		
	lib_npc:start_link([]),	
    lib_npc:register("libuv","","start_monitor","",asyn),
	lib_npc:register("libuv","","add_dir","char#",syn),
	lib_npc:register("libuv","","remove_dir","char#",syn),
	lib_npc:register("libuv","","stop_monitor","",syn),	
	handle_monitor_info(Ets),
	{ok, #state{ets = ets:new(Ets, [set, public, named_table])}}.

%% -----------------------------------------------------------------
%% Function: handle_monitor_info/1
%% Description: starts a thread handle monitor information
%% Returns: Any
%% -----------------------------------------------------------------
handle_monitor_info(Ets) ->
	Pid = spawn(fun() -> 				  
				  loop(Ets)
				  end),
	libuv:start_monitor(Pid).

%% -----------------------------------------------------------------
%% Function: loop/1
%% Description: loop recieves monitor information and handle it
%% Returns: Any
%% -----------------------------------------------------------------
loop(Ets) ->
	receive 
		Data when is_list(Data) ->
			handle_monitor_info(Ets,Data),
			loop(Ets);
		_ ->
			ignore
	end.

%% -----------------------------------------------------------------
%% Function: handle_monitor_info/2
%% Description: implement handle monitor information
%% Returns: Any
%% -----------------------------------------------------------------

handle_monitor_info(Ets,Data) ->	
	case  rfc4627:decode(Data) of 		
		{ok, {_,[{_,FileName},{_,FilePath},{_,<<"modify">>}]},_} ->
			handle_modify(Ets, unicode:characters_to_list(FilePath), unicode:characters_to_list(FileName));
		{ok, {_,[{_,FileName},{_,FilePath},{_,<<"add">>}]},_} ->
			handle_add(Ets, unicode:characters_to_list(FilePath), unicode:characters_to_list(FileName));
		{ok, {_,[{_,FileName},{_,FilePath},{_,<<"remove">>}]},_} ->
		    handle_remove(Ets, unicode:characters_to_list(FilePath), unicode:characters_to_list(FileName));		
        {ok, {_,[{_,NewName},{_,OldName},{_,Path},{_,<<"rename">>}]},_} ->			
			handle_rename(Ets, unicode:characters_to_list(Path), unicode:characters_to_list(NewName),unicode:characters_to_list(OldName));
        _ ->
            ignore
    end.

%% -----------------------------------------------------------------
%% Function: handle_add_file/4
%% Description: handle add file
%% Returns: Any
%% -----------------------------------------------------------------

handle_add_file(Ets,Path, {File, _Size, regular, _MTime, _CTim} = E,MFAList) ->	
	case ets:lookup(Ets, Path++"/"++File) of
		[{_, _, _, _}] ->			
			ignore;
		[] ->			
			ets:insert(Ets, {Path++"/"++File, MFAList, regular, E}),
			monitor_spawn(MFAList,{monitors,[Path++"/"++File], [], [],[],[],[]})	
           
	end.			
%% -----------------------------------------------------------------
%% Function: handle_add_file/4
%% Description: handle add path
%% Returns: Any
%% -----------------------------------------------------------------
handle_add_path(Ets,Path, NewName,MFAList,{NewName,Size2,directory,Time2,CTime2} ) ->	
	case z_lib:list_file(Path++"/"++NewName, ?DIR_DEPTH) of
        {ok, L} ->
            LL1 = [ {NewName++"/"++ _File, Size,regular,Time,CTime} || {_File, Size, regular, Time, CTime}  <- L],
            LL2 = [ {NewName++"/"++ _File, Size,directory,Time,CTime} || {_File, Size, directory, Time, CTime}  <- L],	
            [ets:insert(Ets, {Path++"/"++File, MFAList, regular, E}) || {File, _, regular, _, _} = E <- LL1],
            [ets:insert(Ets, {Path++"/"++File, MFAList, directory, E}) || {File, _, directory, _, _} = E <- LL2],	
            case os:type() of
                {win32,_} ->
                    ignore;
                _ ->										
                    [libuv:add_dir(Path++"/"++File) || {File, _, _, _, _} <- LL2],
                    libuv:add_dir(Path++"/"++NewName)
            end,	
            case ets:lookup(Ets, Path++"/"++NewName) of
                [{_File, MFAList, _Type, _Info}] ->
                    ignore;
                [] ->	
                    ets:insert(Ets, {Path++"/"++NewName, MFAList, directory, {NewName,Size2,directory,Time2,CTime2}})			
            end,
            
            monitor_spawn(MFAList,{monitors,[Path++"/"++File || {File, _, _, _, _} <- LL1], [], [],[Path++"/"++NewName],[],[]});
        _  ->
              ignore
    end.         
	 
			

%% -----------------------------------------------------------------
%% Function: handle_add/3
%% Description: handle add information
%% Returns: Any
%% -----------------------------------------------------------------

handle_add(Ets,Path,NewName) ->
	case ets:lookup(Ets,Path) of
        [] ->
			ignore;
		[{_,MFAList,_,_}] ->    
			case file:read_file_info(Path++"/"++NewName) of
				{ok, #file_info{size = Size, type = regular, mtime = Time,ctime = CTime}} ->					
					handle_add_file(Ets,Path,{NewName,Size,regular,Time,CTime},MFAList);			
				{ok, #file_info{size = Size2,type = directory,mtime = Time2,ctime = CTime2}} ->					
					handle_add_path(Ets,Path,NewName,MFAList,{NewName,Size2,directory,Time2,CTime2});		
				{error,_} ->
					{none,none}
			end
	end.

%% -----------------------------------------------------------------
%% Function: handle_rename/4
%% Description: handle rename information
%% Returns: Any
%% -----------------------------------------------------------------
	
handle_rename(Ets,Path,NewName, OldName) ->	
	case ets:lookup(Ets,Path) of
		[] ->
			ignore;
		[{_,MFAList,_,_}] ->
			handle_remove(Ets,Path,OldName),
			case file:read_file_info(Path++"/"++NewName) of
				{ok, #file_info{size = Size, type = regular, mtime = Time,ctime = CTime}} ->					
					handle_add_file(Ets,Path,{NewName,Size,regular,Time,CTime},MFAList);			
				{ok, #file_info{size = Size2,type = directory,mtime = Time2,ctime = CTime2}} ->
					handle_remove(Ets,Path,OldName),
			        	handle_add_path(Ets,Path,NewName,MFAList,{NewName,Size2,directory,Time2,CTime2});
				{error,_} ->
					ignore
			end
	end.
			

%% -----------------------------------------------------------------
%% Function: handle_remove/3
%% Description: handle remove information
%% Returns: Any
%% -----------------------------------------------------------------
handle_remove(Ets,Path,NewName) ->
	case ets:lookup(Ets, Path) of
		[] ->
			ignore;
		[{_,MFAList,_,_}] ->
			case ets:lookup(Ets,Path++"/"++NewName) of
				[] ->
					ignore;		
				[{File,MFAList,regular, _Info}] ->						
					libuv:remove_dir(File),					
					ets:delete(Ets,File),
					monitor_spawn(MFAList,{monitors,[], [File], [],[],[],[]});			
				[{File,MFAList,directory,_}] ->
					libuv:remove_dir(File),					
					ets:delete(Ets,File),	
				  	handle_remove_path(Ets,MFAList,Path,NewName)					
			end
	end.

get_remove_file(_Ets,none,_RmDir,L) ->
  L;

get_remove_file(Ets,File,RmDir,L) ->
	Index = string:str(File, RmDir),
	case ets:lookup(Ets, File) of
		[{File,_MFAList,Type, _Info}] ->					
			case ets:next(Ets,File)	of 
				'$end_of_table' ->					
					if 
						Index =:= 1 ->
							get_remove_file(Ets,none,RmDir,[{File,Type}|L]);
						true ->
							get_remove_file(Ets,none,RmDir,L)
					end;
				File2 ->					
					if 						
						Index =:= 1 ->						   
							get_remove_file(Ets,File2 ,RmDir,[{File,Type}|L]);
					    true ->
                           get_remove_file(Ets,File2 ,RmDir,L) 
					end
			end;							
        _ ->
            L
     end.

handle_remove_path(Ets,MFAList, Path, NewName) ->
    RmDir = Path ++ "/" ++ NewName,
    ets:safe_fixtable(Ets, true),
	File = ets:first(Ets),
	L = get_remove_file(Ets,File,RmDir,[]),
	ets:safe_fixtable(Ets, false),	
	FileList = [File2 || {File2,regular} <- L],
	monitor_spawn(MFAList,{monitors,[], FileList, [],[],[RmDir],[]}).


%% -----------------------------------------------------------------
%% Function: handle_modify_file/4
%% Description: handle modify file
%% Returns: Any
%% -----------------------------------------------------------------
handle_modify_file(Ets,Path,NewName,Time) ->
	case ets:lookup(Ets,Path++"/"++NewName) of
		[] ->			
			handle_add(Ets,Path,NewName);		
		[{_File,_MFAList,_Type, {_,_,_,Time,_}}] ->				
			ignore;
		[{File,MFAList,_Type, {NewName, Size, regular, _MTime, CTime}}] ->
			ets:insert(Ets, {File, MFAList, regular, {NewName,Size,regular,Time,CTime}}),
			monitor_spawn(MFAList,{monitors,[], [], [File],[],[],[]})			
	end.

handle_modify_file2(Ets,Path,NewName) ->
	case ets:lookup(Ets,Path++"/"++NewName) of
		[] ->			
			ignore;			
		[{File,MFAList,_Type, {NewName, Size, regular, _MTime, CTime}}] ->
			case file:read_file_info(Path++"/"++NewName) of 
				{ok, #file_info{type = regular, mtime = _MTime}} ->
					ignore;
				{ok,#file_info{type = regular, mtime = _Time}} ->			
					ets:insert(Ets, {File, MFAList, regular, {NewName,Size,regular,_Time,CTime}}),
					monitor_spawn(MFAList,{monitors,[], [], [File],[],[],[]});
				{error,_} ->
					ignore
			end			
	end.
%% -----------------------------------------------------------------
%% Function: handle_modify_path/3
%% Description: handle modify path
%% Returns: Any
%% -----------------------------------------------------------------
handle_modify_path(Ets, Path,SubPath,_CTime) ->	
    FullPath = Path ++ "/" ++ SubPath,
    case z_lib:list_file(FullPath, ?DIR_DEPTH) of
        {ok, L} ->
            [handle_modify_file2(Ets,Path, SubPath++"/" ++ File) || {File, _, regular, _, _} = _E <- L];
        _ ->
            ignore 
    end.            
%% -----------------------------------------------------------------
%% Function: handle_modify/3
%% Description: handle modify information
%% Returns: Any
%% -----------------------------------------------------------------	
handle_modify(Ets,Path, NewName) ->
	case file:read_file_info(Path++"/"++NewName) of		
		{ok, #file_info{size = _Size, type = regular, mtime = Time}} ->			
			handle_modify_file(Ets,Path, NewName,Time);
		{ok, #file_info {type = directory,mtime = Time}} ->				
			handle_modify_path(Ets,Path,NewName,Time);
		_ ->			
			ignore
	end.

%% -----------------------------------------------------------------
%% Function: remove_dir/2
%% Description: remove monitor directory
%% Returns: Any
%% -----------------------------------------------------------------
remove_dir(_Ets,[]) ->
	ignore;

remove_dir(Ets,[H|T]) ->
	case ets:lookup(Ets, H) of
    [{_, _MFAList,  _, _}] ->
			ets:delete(Ets, H),
			[H | remove_dir(Ets,T) ];
		[] ->
			remove_dir(Ets,T)
	end.


set_impl(File,MFA, Ets) ->
	spawn(fun() -> 				  
				case ets:lookup(Ets, File) of
					[{_, _MFAList,  _Type, _Info}] ->				
							ignor;
					[] ->
						libuv:add_dir(File),
						case file:read_file_info(File) of
							{ok, #file_info{size = Size, type = regular, mtime = MTime, ctime = CTime}} ->				                   
									ets:insert(Ets,{File,[MFA],regular,{File, Size,regular,MTime, CTime}});				
            				{ok, #file_info{size = _Size, type = directory, mtime = MTime, ctime = CTime}} ->                                    
								case z_lib:list_file(File, ?DIR_DEPTH) of
									{ok,L} ->
										[ets:insert(Ets,{File++"/"++F, [MFA],regular,E})|| {F,_S,regular,_M,_C} = E <- L],
										PathList = [{File++"/"++F, [MFA],directory,E} || {F,_S,directory,_M,_C} = E <- L],
										[ets:insert(Ets, A) ||  A <- PathList],
										case os:type() of
											{win32,_} ->
												ignore;
											_ ->
												[libuv:add_dir(F2) || {F2,_,_,_} <- PathList]
										end,
										ets:insert(Ets,{File,[MFA],directory,{File, 0,directory,MTime, CTime}});
									_ ->
										ignore
								end;
							_ ->
								ignore
						end
				end
		  end).


%% -----------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State} |
%%		{reply, Reply, State, Timeout} |
%%		{noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, Reply, State} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_call({set, File, MFA}, _From, #state{ets = Ets} = State) ->
	set_impl(File,MFA,Ets),				
	{reply, ok, State};	


handle_call({delete, File}, _From, #state{ets = Ets} = State) ->
	case file:read_file_info(File) of
		{ok, #file_info{size = _Size, type = regular, mtime = _Time}} ->
			libuv:remove_dir(File),
			case ets:lookup(Ets,File) of
				[{_,MFAList,_,_}] ->
					ets:delete(Ets,File),
					{reply, {ok, {File, MFAList}}, State};
				[] ->
					{reply, none, State}
		    end;
	    {ok, #file_info {type = directory}} ->
			case z_lib:list_file(File, ?DIR_DEPTH) of
				{ok,L} ->
					LL = lists:sort([F || {F,_,regular,_,_} <- L]),
					libuv:remove_dir(File),
					case erlang:length(remove_dir(Ets,LL)) of
						0 ->
							{reply, none, State};
						_ ->
							{reply, {ok, {File, []}}, State}
					end;
				_ ->
					{reply, none, State}
			end;
		_ ->
			{reply, none, State}
	end;
	
handle_call({delete, File, MFA}, _From, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, File) of
		[{File, MFAList, Type,Info}] ->
			case lists:delete(MFA,  MFAList) of
				[] ->
					ets:delete(Ets, File),
					{reply, ok, State};
				MFAList ->
					{reply, none, State};
				MFAList1 ->				
					ets:insert(Ets, {File, MFAList1, Type, Info}),
					{reply, ok, State}
			end;
		[] ->
			{reply, none, State}
	end;

handle_call(_Request, _From, State) ->
	{noreply, State}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_cast(_Msg, State) ->
	{noreply, State}.

%% -----------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_info({timeout, _Ref, none}, #state{ets = _Ets} = State) ->
%%	file_monitor(Ets, zm_time:now_second()),
	{noreply, State, hibernate};

handle_info(_Info, State) ->
	{noreply, State}.


%% -----------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% -----------------------------------------------------------------
terminate(_Reason, State) ->
	State.

%% -----------------------------------------------------------------
%% Function: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% -----------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


%监控运行方法
monitor_spawn(MFAList, Arg) ->
	spawn(fun() -> [apply(M, F, [A, Arg]) || {M, F, A} <- MFAList] end).


