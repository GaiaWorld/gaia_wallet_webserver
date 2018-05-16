%%@doc http 分派模块
%%```
%%% 可以进行http分派，也可以进行session分派，
%%% 保留最后一个MFA的Info，合并http头，如果键相同，保留最后MFA的值
%%'''
%%@end


-module(zm_http_assign).

-description("http assign").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([assign/5]).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc assign msg with mfa list
%% @spec (Args::list(), Con, Info::list(), Headers, Body) -> return()
%%      | (Args::list(), Con, Attr, Info, Msg) -> return()
%% where
%%      return() = {ok, Info, Headers, Body} | {ok, Attr, Info, Msg}
%%@end
%% -----------------------------------------------------------------
assign(Args, Con, Info, Headers, Body) when is_list(Info) ->
	assign1(Args, Con, Info, Headers, Body, [], sb_trees:empty(), []);

assign(Args, Con, Attr, Info, Msg) ->
	assign2(Args, Con, Attr, Info, Msg, [], [], []).

%%%===================LOCAL FUNCTIONS==================
% http分派
assign1([], _Con, _ReqInfo, _ReqHeaders, _ReqBody, Info, Headers, Body) ->
	{ok, Info, Headers, Body};
assign1([{Name, M, F, A} | T], Con,
	ReqInfo, ReqHeaders, ReqBody, _Info, Headers, Body) when is_list(Name) ->
	{_, Info1, Headers1, Body1} = M:F(A, Con, ReqInfo, ReqHeaders, ReqBody),
	assign1(T, Con, ReqInfo, ReqHeaders, ReqBody, Info1,
		z_lib:merge_tree(Headers1, Headers), [{Name, Body1} | Body]);
assign1([{Name, M, F, A} | T], Con,
	ReqInfo, ReqHeaders, ReqBody, _Info, Headers, Body) when is_atom(Name) ->
	{_, Info1, Headers1, Body1} = M:F(A, Con, ReqInfo, ReqHeaders, ReqBody),
	assign1(T, Con, ReqInfo, ReqHeaders, ReqBody, Info1,
		z_lib:merge_tree(Headers1, Headers), [{atom_to_list(Name), Body1} | Body]);
assign1([_ | T], Con, ReqInfo, ReqHeaders, ReqBody, Info, Headers, Body) ->
	assign1(T, Con, ReqInfo, ReqHeaders, ReqBody, Info, Headers, Body).

% session分派
assign2([], _Con, _ReqAttr, _ReqInfo, _ReqMsg, Attr, Info, Msg) ->
	{ok, Attr, Info, Msg};
assign2([{Name, M, F, A} | T], Con,
	ReqAttr, ReqInfo, ReqMsg, Attr, _Info, Msg) when is_list(Name) ->
	{_, Attr1, Info1, Msg1} = M:F(A, Con, ReqAttr, ReqInfo, ReqMsg),
	assign2(T, Con, ReqAttr, ReqInfo, ReqMsg, Attr1 ++ Attr, Info1, [{Name, Msg1} | Msg]);
assign2([{Name, M, F, A} | T], Con,
	ReqAttr, ReqInfo, ReqMsg, Attr, _Info, Msg) when is_atom(Name) ->
	{_, Attr1, Info1, Msg1} = M:F(A, Con, ReqAttr, ReqInfo, ReqMsg),
	assign2(T, Con, ReqAttr, ReqInfo, ReqMsg, Attr1 ++ Attr, Info1,
		[{atom_to_list(Name), Msg1} | Msg]);
assign2([_ | T], Con, ReqAttr, ReqInfo, ReqMsg, Attr, Info, Msg) ->
	assign2(T, Con, ReqAttr, ReqInfo, ReqMsg, Attr, Info, Msg).
