%%% 根据通讯参数改写配置上指定的属性


-module(zm_write_attr_port).

-description("zm_write_attr_port").
-copyright({seasky, 'www.seasky.cn'}).
-author({zmyth, leo, 'zmythleo@gmail.com'}).
-vsn(1).

%%%=======================EXPORT=======================
-export([service/5]).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 根据通讯参数改写配置上指定的属性, 协议参数: [{AttrKey, Value} | ...]
%% @spec service(Args::list(), Con::tuple(), Attr::sb_trees(), Info::[{Key::atom(), Value::any()}], Msg::[{Key::atom(), Value::any()}]) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
service(AttrKeyRuleList, _Con, Attr, Info, Msg) ->
	% TODO
	erlang:throw({401, "forbidden table", [AttrKeyRuleList, Attr, Info, Msg]}).

%%%===================LOCAL FUNCTIONS==================
