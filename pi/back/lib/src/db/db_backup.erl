%% @author luobin
%% @doc 备份与恢复存储层数据

-module(db_backup).

-include_lib("kernel/include/file.hrl").
-define(BLOCK_LEN_SIZE, 4).
-define(ZEROS_COUNT, 5).

%% ====================================================================
%% API functions
%% ====================================================================
-export([dump_data_gather/4, dump2file/4]).
-export([show_local_interval/1, get_local_interval/5]).
-export([restore/2]).

-record(state, {pid, name, ets, index, table, opts, path, mod, args, cache,
	interval, snapshot, dir, reads, write, wsize, lasttime, snapshot_pid}).

%% ====================================================================
%% Internal functions
%% ====================================================================
%% 
%% Function: restore/2 
%% Description: 还原备份数据 
%% Arguments: (DumpFile :: File, Timeout > 0)
%% File = Filename | iodata()
%% Filename = name_all()
%% Returns: {ok, Count} | _
%% 
restore(DumpFile, Timeout) when Timeout > 0 ->
	From=self(),
	Pid=spawn(fun() -> 
					Self=self(),
					case file:open(DumpFile, [read, raw, binary]) of
						{ok, F} ->
						    try
								case nodes() of
									[] ->
										load_loop(From, Self, F, 0, [], 0, node());
									_ ->
										zm_event:notify(?MODULE, load_ok, start),
							    		load_loop(From, Self, F, 0, [], 0, db_center:random_node())
								end
						    catch
							  _:Reason ->
								  close_file(F),
								  From ! {Self, {error, Reason, erlang:get_stacktrace()}}
						    end;
						{error, Reason} ->
							From ! {Self, {error, {open_dump_file_failed, Reason}}}
					end
			   end),
	receive
		{Pid, R} ->
			R
	after Timeout ->
		{error, timeout}
	end.



%% 
%% Function: dump2file/4
%% Description: dump数据到文件
%% Arguments: (DumpFile :: is_lists(), Table :: is_atom(), SendNum :: is_integer)
%% Returns: ok | _
%% 
dump2file(DumpFile, Table, SendNum,Timeout)
  when is_list(DumpFile), is_atom(Table), is_integer(SendNum), Timeout > 0 ->
	 case file:open(DumpFile, [append, raw, binary]) of
		{ok, F} ->
			dump_data_gather(self(), Table, SendNum, Timeout),
	 		save_loop(F, erlang:start_timer(Timeout, self(), stop), Timeout);
		 {error, Reason} ->
			{error, {open_dump_file_failed, Reason}}
	end.
	
%% Function: dump_data_gather/4
%% Description: 备份数据采集
%% Arguments: From :: Pid, Table :: is_atom(), SendNum :: is_integer()
%% Returns: {ok, Pid} || {error, Reason}
%% 
dump_data_gather(From, Table, SendNum, Timeout) ->
	case ets:lookup(zm_db_server, Table) of
		[{Table, _, _, running, TPid}] when is_pid(TPid) ->
			case zm_db_client:count(Table) of
				{ok, _Count} ->
					iterate_table(From, Table, SendNum, Timeout);
				{estimate, Count} ->
					{error, {not_security, Count}};
				Reason ->
					{error, Reason}
			end;
		_ ->
			{error, invalid_table}
	end.

%%
%% 获取本地指定表的只读增量列表，只支持文件表
%%
show_local_interval(Table) ->
	case ets:lookup(zm_db_server, Table) of
		[{Table, Opts, _, running, Pid}] when is_pid(Pid) ->
			case lists:member({type, file}, Opts) of
				true ->
					try 
						{_, _, _, {_, State}, _}=recon:get_state(Pid, 5000),
						get_interval_data(State)
					catch
						_:Reason ->
							{error, {Reason, erlang:get_stacktrace()}}
					end;
				false ->
					{error, invalid_table}
			end;
		_ ->
			{error, invalid_table}
	end.



%%
%% 获取本地指定表的指定增量数据，只支持文件表
%%
get_local_interval(Table, C, CPid, SleepTime, IgnoreError)
  when is_atom(Table), is_integer(C), is_pid(CPid), is_integer(SleepTime), C > 0, SleepTime >= 0, SleepTime =< 1000 ->
	case ets:lookup(zm_db_server, Table) of
		[{Table, Opts, _, running, Pid}] when is_pid(Pid) ->
			case lists:member({type, file}, Opts) of
				true ->
					Self=self(),
					{_, _, _, {_, State}, _}=recon:get_state(Pid, 5000),
					select_interval(Self, State, C, CPid, SleepTime, IgnoreError);
				false ->
					{error, invalid_table}
			end;
		_ ->
			{error, invalid_table}
	end.



select_interval(Self, State, C, CPid, SleepTime, IgnoreError) ->
	case sb_trees:to_list(get_read_interval(State)) of
		[] ->
			{error, interval_empty};
		L ->
			case lists:keyfind(C, 1, L) of
				{C, CPid} ->
					select_interval_(Self, C, CPid, 0, SleepTime, IgnoreError, []);
				{C, NewCPid} ->
					{error, {file_not_match, node(), C, CPid, NewCPid}};
				false ->
					{error, {invalid_interval, node(), C, CPid}}
			end
	end.	

get_interval_data(State) ->
	Local=node(),
	[begin
		 {Size, CTime, MTime}=get_interval_info(Pid),
		 {C, Local, Pid, Size, z_lib:datetime_to_second(CTime), z_lib:datetime_to_second(MTime)}
	 end || {C, Pid} <- sb_trees:to_list(get_read_interval(State))].

select_interval_(Self, C, CPid, Offset, SleepTime, IgnoreError, L) ->
	case zm_file:read(CPid, Offset, 4 * 1024 * 1024) of
		{ok, Bin} ->
			NewL=read_bin(C, CPid, Bin, Offset, IgnoreError, L),
			timer:sleep(SleepTime),
			select_interval_(Self, C, CPid, Offset + byte_size(Bin), SleepTime, IgnoreError, NewL);
		eof ->
			lists:reverse(L);
		E ->
			E
	end.

read_bin(C, CPid, Bin, Offset, IgnoreError, L) ->
	case Bin of
		<<Size:32, Block:Size/binary, OtherBin/binary>> ->
			read_bin(C, CPid, OtherBin, Offset + ?BLOCK_LEN_SIZE + Size, IgnoreError, [read_block(C, CPid, Offset, IgnoreError, Block)|L]);
		<<>> ->
			L;
		_ ->
			erlang:error({load_error,
						"read overage error", node(), C, CPid, Offset})
	end.

get_read_interval(#state{reads = Reads}) ->
	Reads.

get_interval_info(Pid) ->
	{ok, #file_info{size = Size, ctime = CTime, mtime = MTime}}=zm_file:info(Pid),
	{Size, CTime, MTime}.

read_block(C, CPid, Offset, IgnoreError, Block) ->
	case Block of
		<<0:16, _/binary>> ->
			empty_entry;
		<<KSize:16, K:KSize/binary, Vsn:32, Time:48,
			VSize:32, V:VSize/binary, _/binary>> ->
			try 
				read_entry(K, Vsn, Time, V)
			catch
				_:Reason ->
					[erlang:error({read_entry_error, Reason, node(), C, CPid, Offset})
						|| IgnoreError =/= true]
			end;
		_ when IgnoreError =/= true ->
			erlang:error({read_entry_error, invalid_entry, node(), C, CPid, Offset});
		_ ->
			invalid_entry
	end.

read_entry(K, 0, Time, _V) ->
	{binary_to_term(K), '$nil', 0, Time};
read_entry(K, Vsn, Time, V) ->
	{binary_to_term(K), binary_to_term(V), Vsn, Time}.





iterate_table(From, Table, SendNum, Timeout) ->
	case zm_db_client:iterate(Table, ascending, false) of
		{ok, Iterator} ->
			TableName=binary:list_to_bin(atom_to_list(Table)),
			TableNameSize=size(TableName),
			Pid = spawn(fun () -> iterate_table_loop(From, Table, Iterator, SendNum, erlang:start_timer(Timeout, self(), stop), Timeout) end),
			From ! {save, <<2#11111111:8, TableNameSize:16, TableName/binary>>, Pid},
			{ok, Pid};
		E ->
			E
	end.

iterate_table_loop(From, Table, Iterator, SendNum,TimerRef, Timeout) ->
	receive
		next ->
			select_table(From, Table, Iterator, SendNum, SendNum, [], Timeout);
		{timeout, TimerRef, stop} ->
			zm_log:warn(slave_node, iterate_table_loop, ?MODULE, "iterate_table_loop timeout", [
																								{table, Table},
																								{send_num, SendNum},
																								{timeout, Timeout},
																								{error,timeout}
																							   ]),
			{error,timeout}
	end.

select_table(From, Table, NewIterator, 0, OldSendNum, TableKeyList, Timeout) ->
	case reads(TableKeyList, [], Timeout) of
		{ok, Results} ->
			From ! {save, Results, self()},
			iterate_table_loop(From, Table, NewIterator, OldSendNum, erlang:start_timer(Timeout, self(), stop), Timeout);
		E ->
			E
	end;
select_table(From, Table, Iterator, SendNum, OldSendNum, TableKeyList, Timeout) ->
	case zm_db_client:iterate_next(Iterator) of
		{ok, {Key, _, _}, NewIterator} ->
			select_table(From, Table, NewIterator, SendNum - 1, OldSendNum, [{Table, Key} | TableKeyList], Timeout);
		over ->
			case TableKeyList of
				[] ->
					From ! {stop, Table};
				_ ->
					case reads(TableKeyList, [], Timeout) of
						{ok, Results} ->
							From ! {over, Results, self()};
						E ->
							E
					end
			end;
		E ->
			zm_log:warn(?MODULE, select_table, ?MODULE, "zm_db_client:iterate_next error", [
																							{table, Table},
																							{iterator, Iterator},
																							{error, E}
																						   ]),
			E
	end.

reads([], L, _TimeOut) ->
	{ok,L};
reads([{Table,Key}|T], L, Timeout) ->
	case zm_db_client:read(Table,Key,Timeout) of
		{ok,Value,Vsn,Time} ->
			reads(T, [{Key,Value,Vsn,Time}|L], Timeout);
		E ->
			zm_log:warn(slave_node, reads, ?MODULE, "reads table data error", [
																								{table, Table},
																								{key, Key},
																								{error,E}
																							   ]),
			E
	end.

save_loop(F, TimerRef, Timeout) ->
	receive 
		{save,Results, Pid} ->
			ok = save_file(F, Results),	%%接收到的数据发送给前台
			Pid ! next,
			save_loop(F, erlang:start_timer(Timeout, self(), stop), Timeout);
		{over, Results, _Pid} ->
			save_file(F, Results),
			close_file(F);
		{stop, Table} ->
			zm_log:info(local, dump, ?MODULE, "dump OK", [
																			  {table, Table}
																			  ]),
			close_file(F);
		{timeout, TimerRef, stop} ->
			close_file(F)
	end.

save_file(F, Results) ->
	case data_to_bin(Results) of
		Bin ->
			case file:write(F, Bin) of
				ok ->
					ok;
				{error, Reason} ->
					close_file(F),
					erlang:error({save_failed, Reason})
			end
	end.

close_file(F) ->
	case file:close(F) of
		ok ->
			ok;
		{error, Reason} ->
			erlang:error({stop_failed, Reason})
	end.

data_to_bin([]) ->
	<<>>;
data_to_bin(Results) when is_binary(Results) ->
	Results;
data_to_bin(Results) ->
	Bin=erlang:term_to_binary(Results),
	CheckSum=erlang:adler32(Bin),
	Size=size(Bin),
	<<Size:32, CheckSum:32, Bin/binary>>.

load_loop(From, Self, F, Offset, Table, Count, DbNode) ->
	case file:pread(F, Offset, 1) of
		{ok, <<2#11111111:8>>} ->
			case file:pread(F, Offset + 1, 2) of
				{ok, <<ByteSize:16>>} ->
					case file:pread(F, Offset + 3, ByteSize) of
						{ok, NewTable} ->
							load_data(From, Self, F, Offset + 3 + ByteSize, list_to_atom(binary:bin_to_list(NewTable)), 0, DbNode);
						{error, Reason} ->
							From ! {Self, {error, {read_table_name_failed, Reason}}}
					end;
				{error, Reason} ->
					From ! {Self, {error, {read_dump_file_head_failed, Reason}}}
			end;
		{ok, _} ->
			load_data(From, Self, F, Offset, Table, Count, DbNode);
		eof ->
			zm_event:notify(?MODULE, load_ok, stop),
			close_file(F),
			From ! {Self, ok};
		{error, Reason} ->
			From ! {Self, {error, {open_dump_file_failed, Reason}}}
	end.

load_data(From, Self, F, Offset, Table, Count, DbNode) ->
	case file:pread(F, Offset, 8) of
		{ok, <<ByteSize:32, CheckSum:32>>} ->
			DataOffset=Offset + 8,
			case file:pread(F, DataOffset, ByteSize) of
				{ok, Bin} ->
					case bin_to_data(CheckSum, Bin) of
						{ok, L} ->
							case rpc:call(DbNode, zm_db_client, restore, [[{Table, Key, Value, Vsn, Time} || {Key, Value, Vsn, Time} <- L]]) of
								ok ->
									NewCount=Count + length(L),
									zm_event:notify(?MODULE, load_ok, {Table, NewCount}), %通知加载指定数据成功和已加载记录的数量
									load_loop(From, Self, F, DataOffset + ByteSize, Table, NewCount, DbNode);
								E ->
									close_file(F),
									From ! {Self, E}
							end;
						E ->
							close_file(F),
							From ! {Self, E}
					end;
				eof ->
					close_file(F),
					From ! {Self, {error, {read_dump_file_failed, DataOffset, ByteSize}}};
				E ->
					close_file(F),
					From ! {Self, E}
			end;
		eof ->
			zm_event:notify(?MODULE, restore_ok, {Table, Count}),
			close_file(F),
			From ! {Self, ok};
		E ->
			close_file(F),
			From ! {Self, E}
	end.

bin_to_data(CheckSum, Bin) ->
	case erlang:adler32(Bin) of
		CheckSum ->
			{ok, erlang:binary_to_term(Bin)};
		RealCheckSum ->
			{error, {invalid_checksum, CheckSum, RealCheckSum}}
	end.