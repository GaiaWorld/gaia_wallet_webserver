%% @author lenovo
%% @doc @todo Add description to fs_port.


-module(fs_port).

%% ====================================================================
%% API functions
%% ====================================================================
-export([upload/5, list_dir/5]).

%%
%%上传指定文件并将文件保存到指定路径
%% 
upload(Args, _Session, _Attr, Info, Msg) ->
	Root=z_lib:get_value(Args, "root", "./"),
	Path=case z_lib:get_value(Msg, "path", "") of
			 "" ->
				Root;
			 P ->
				Dir=filename:join([Root, P]) ++ "/",
				case filelib:is_dir(Dir) of
					true ->
						Dir;
					false ->
						filelib:ensure_dir(Dir),
						Dir
				end
	end,
	case z_lib:get_value(Msg, "content", none) of
		{FileName, _, Content} ->
			case z_lib:get_value(Msg, "crc", none) of
				none ->
					save_file(Info, none, Path, FileName, Content);
				CRC ->
					save_file_by_checksum(Info, CRC, Path, FileName, Content)
			end;
		none ->
			zm_log:warn(dev, upload, ?MODULE, "upload failed, invalid file", [
																   {path, Path},
																   {file, none}
																  ]),
			{ok, [], Info, [{"result", "-1"}]}
	end.

%%
%%返加资源目录下指定路径下的所有文件
%% 
list_dir(Args, _Session, _Attr, Info, Msg) ->
	Root=z_lib:get_value(Args, "root", "./"),
	Path=case z_lib:get_value(Msg, "path", "") of
			 "" ->
				 filename:join(["../../../plugin", Root, ".res"]);
			 P ->
				 filename:join(["../../../plugin", Root, ".res", P])
	end,
	case file:list_dir(Path) of
		{ok, Files} ->
			zm_log:info(dev, upload, ?MODULE, "list dir ok", [
												   {path, Path}
												  ]),
			{ok, [], Info, lists:sort(list_file(Files, Path, "./", []))};
		{_, Reason} ->
			{ok, [], Info, [{"error", Reason}]}
	end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%%保存文件到指定路径
save_file(Info, CRC, Path, FileName, Content) ->
	File=filename:join([Path, FileName]),
	case file:write_file(File, Content) of
		ok ->
			zm_log:info(dev, upload, ?MODULE, "upload ok", [
												   {path, Path},
												   {file, FileName},
												   {crc, CRC}
												  ]),
			{ok, [], Info, [{"result", "0"}]};
		{error, Reason} ->
			zm_log:warn(dev, upload, ?MODULE, "write local failed", [
												   {path, Path},
												   {file, FileName},
												   {crc, CRC},
												   {reason, Reason}
												  ]),
			{ok, [], Info, [{"result", "-10"}]}
	end.

save_file_by_checksum(Info, CRC, Path, FileName, Content) ->
	case erlang:crc32(Content) of
		CRC ->
			save_file(Info, CRC, Path, FileName, Content);
		RealCRC ->
			zm_log:warn(dev, upload, ?MODULE, "upload failed, invalid crc", [
														   {path, Path},
														   {file, FileName},
														   {client_crc, CRC},
														   {real_crc, RealCRC}
														  ]),
			{ok, [], Info, [{"result", "-100"}]}
	end.

%%遍历指定路径下的所有文件
list_file([F|T], Path, Root, L) ->
	File=filename:join(Path, F),
	case filelib:is_dir(File) of
		true ->
			case file:list_dir(File) of
				{ok, Files} ->
					list_file(T, Path, Root, list_file(Files, File, filename:join(Root, F), []) ++ L);
				_ ->
					list_file(T, Path, Root, [Root|L])
			end;
		false ->
			list_file(T, Path, Root, [filename:join(Root, F)|L])
	end;
list_file([], _, _, L) ->
	L.