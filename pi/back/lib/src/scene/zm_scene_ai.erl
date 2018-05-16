%%@doc AI模块
%%```
%%% AI模块可以在主进程上被World对象管理， 也可以单独启动一个zm_scene进程（frame由主进程推动）
%%% World对象要求实现get_pid(World)方法和get_ext(World, Key)方法。 Key一般是?MODULE
%%% get_ext(World, Key) 应该返回 {_, Args} | {_, Pid, Args}, {Ets, FrameTime} = Args
%%% 每ID对应AI对象是一个GSM
%%'''
%%@end


-module(zm_scene_ai).

-description("scene AI").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([cast/3, call/3, cast/2, call/2, frame/2]).
-export([member/3, insert/3, lookup/3, delete/3]).
-export([create/5, write_apply/5, do_cast/3, do_call/3, do_action/3, do_frame/3]).

%%%=======================DEFINE=======================
%%最大AI ID
-define(MAX_ID, max_id).
%%下次运行的时间
-define(NEXT_TIME, next_time).

%%参数模块的执行方法
-define(MF(MA, F), (element(1, MA)):F(MA).

%%场景直接执行的MFA
-define(APPLY, {?MODULE, write_apply, none}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 指定ID的AI发送Cast数据
%%```
%% 根据所在的进程，以及AI进程是否为单独的进程，来决定执行路径
%%'''
%% @spec cast(World, ID, Msg) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
cast(World, ID, Msg) ->
	apply1(cast_apply, do_cast, World, ID, Msg).

%% -----------------------------------------------------------------
%%@doc 指定ID的AI发送Call数据
%%```
%% 根据所在的进程，以及AI进程是否为单独的进程，来决定执行路径
%%'''
%% @spec call(World, ID, Msg) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
call(World, ID, Msg) ->
	apply1(call_apply, do_call, World, ID, Msg).

%% -----------------------------------------------------------------
%%@doc 用指定的MFA操作AI全局数据，返回执行结果
%%```
%% 根据所在的进程，以及AI进程是否为单独的进程，来决定执行路径
%%'''
%% @spec cast(World, MFA) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
cast(World, {_M, _F, _A} = MFA) ->
	apply2(cast_apply, do_action, World, MFA).

%% -----------------------------------------------------------------
%%@doc 用指定的MFA操作AI全局数据，返回执行结果
%%```
%% 根据所在的进程，以及AI进程是否为单独的进程，来决定执行路径
%%'''
%% @spec call(World, MFA) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
call(World, {_M, _F, _A} = MFA) ->
	apply2(call_apply, do_action, World, MFA).

%% -----------------------------------------------------------------
%%@doc 定时帧推
%%```
%% 根据所在的进程，以及AI进程是否为单独的进程，来决定执行路径
%%'''
%% @spec action(World, Now) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
frame(World, Now) ->
	apply2(cast_apply, do_frame, World, Now).

%% -----------------------------------------------------------------
%% Function: member/2
%% Description: 判断指定ID的AI对象是否存在
%% Returns: true | false
%% -----------------------------------------------------------------
member(ID, _World, Ets) ->
	ets:member(Ets, ID).

%% -----------------------------------------------------------------
%% Function: insert/4
%% Description: 创建AI对象，返回对象的ID
%% Returns: any()
%% -----------------------------------------------------------------
insert({0, M, A}, _World, Ets) ->
	Max = ets:lookup_element(Ets, ?MAX_ID, 2),
	ets:insert(Ets, {?MAX_ID, Max + 1}),
	ets:insert(Ets, {Max, M, A}),
	Max;
insert({ID, _M, _A} = I, _World, Ets) ->
	Max = ets:lookup_element(Ets, ?MAX_ID, 2),
	case ets:lookup_element(Ets, ?MAX_ID, 2) of
		Max when ID >= Max ->
			ets:insert(Ets, {?MAX_ID, ID + 1}),
			ets:insert(Ets, I),
			ID;
		_ ->
			ets:insert(Ets, I),
			ID
	end.

%% -----------------------------------------------------------------
%% Function: lookup/2
%% Description: 查找指定ID的AI对象
%% Returns: {ID, M, A} | none
%% -----------------------------------------------------------------
lookup(ID, _World, Ets) ->
	case ets:lookup(Ets, ID) of
		[I] -> I;
		_ -> none
	end.

%% -----------------------------------------------------------------
%% Function: delete/2
%% Description: 删除指定ID的AI对象
%% Returns: any()
%% -----------------------------------------------------------------
delete(ID, _World, Ets) ->
	ets:delete(Ets, ID).

%% -----------------------------------------------------------------
%%@doc 创建AI，独立进程由zm_scene调用，和主进程一起则直接调用
%% @spec create(AIList::list(), TypeName::{Type::any(), Name::any()}, Src::any(), FrameTime::integer(), Now::integer()) -> return()
%% where
%%  return() = {Close::fun(), Args::any()}
%%@end
%% -----------------------------------------------------------------
create(AIList, TypeName, _Src, FrameTime, Now) ->
	Ets = ets:new(?MODULE, [ordered_set]),
	ets:insert(Ets, {?MAX_ID, 1}),
	ets:insert(Ets, {?NEXT_TIME, Now + FrameTime}),
	init_(Ets, AIList, TypeName, Now),
	% 销毁回调函数
	Close = fun(_, Reason) ->
		[M:close(A, Reason) || {_, M, A} <- ets:tab2list(Ets)],
		ets:delete(Ets)
	end,
	{Close, {Ets, FrameTime}}.

%% -----------------------------------------------------------------
%%@doc 消息结构：{_, F, A}
%%```
%% 执行本模块指定的函数
%%'''
%% @spec write_apply(_, TypeName, Args, Msg, From) -> return()
%% where
%%  return() = any()
%% -----------------------------------------------------------------
write_apply(_, _TypeName, Args, {_, F, A}, _From) ->
	apply(?MODULE, F, [Args | A]).

%% -----------------------------------------------------------------
%%@doc 消息结构：{_, ID, Msg} | {_, ID, Msg, World}
%%```
%% 在本进程给指定ID的AI发送Cast数据
%%'''
%% @spec do_cast(World, Msg, From) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
do_cast({Ets, _}, {ID, Msg, World}, _From) ->
	do_cast1(Ets, World, ID, Msg);
do_cast(World, {ID, Msg}, _From) ->
	{_, {Ets, _}} = ?MF(World, get_ext), ?MODULE),
	do_cast1(Ets, World, ID, Msg).

do_cast1(Ets, World, ID, Msg) ->
	case ets:lookup(Ets, ID) of
		[{_, M, A}] ->
			case M:cast(A, {Msg, World, Ets, ID}) of
				{update, A1} ->
					try
						ets:update_element(Ets, ID, {3, A1})
					catch
						_:_ -> update_error
					end;
				delete ->
					ets:delete(Ets, ID);
				_ ->
					other
			end;
		_ ->
			none
	end.

%% -----------------------------------------------------------------
%%@doc 消息结构：{_, ID, Msg} | {_, ID, Msg, World}
%%```
%% 在本进程给指定ID的AI发送Call数据
%%'''
%% @spec do_call(World, Msg, From) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
do_call({Ets, _}, {ID, Msg, World}, From) ->
	do_call1(Ets, World, ID, Msg, From);
do_call(World, {ID, Msg}, From) ->
	{_, {Ets, _}} = ?MF(World, get_ext), ?MODULE),
	do_call1(Ets, World, ID, Msg, From).

do_call1(Ets, World, ID, Msg, From) ->
	case ets:lookup(Ets, ID) of
		[{_, M, A}] ->
			case M:call(A, {Msg, World, Ets, ID}, From) of
				{reply, Reply, update, A1} ->
					try
						ets:update_element(Ets, ID, {3, A1})
					catch
						_:_ -> update_error
					end,
					Reply;
				{reply, Reply, delete} ->
					ets:delete(Ets, ID),
					Reply;
				{reply, Reply} ->
					Reply;
				{noreply, update, A1} ->
					try
						ets:update_element(Ets, ID, {3, A1})
					catch
						_:_ -> update_error
					end,
					noreply;
				{noreply, delete} ->
					ets:delete(Ets, ID),
					noreply;
				noreply ->
					noreply;
				_ ->
					noreply
			end;
		_ ->
			{error, ai_not_found}
	end.

%% -----------------------------------------------------------------
%%@doc 消息结构：{_, MFA} | {_, MFA, World}
%%```
%% 在本进程执行指定的MFA
%%'''
%% @spec do_action(World, Msg, From) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
do_action({Ets, _}, {{M, F, A}, World}, _From) ->
	M:F(A, World, Ets);
do_action(World, {M, F, A}, _From) ->
	{_, {Ets, _}} = ?MF(World, get_ext), ?MODULE),
	M:F(A, World, Ets).

%% -----------------------------------------------------------------
%%@doc 消息结构：{MsgList, Now} | {_, Now, World}
%%```
%% 每帧消息，负责推动AI计算
%%'''
%% @spec do_frame(World, Msg, From) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
do_frame({Ets, FrameTime}, {Now, World}, _From) ->
	do_frame(Ets, World, Now, FrameTime);
do_frame(World, Now, _From) ->
	{_, {Ets, FrameTime}} = ?MF(World, get_ext), ?MODULE),
	do_frame(Ets, World, Now, FrameTime).

%%%===================LOCAL FUNCTIONS==================
% cast 或 call方法调用
apply1(Apply, Do, World, ID, Msg) ->
	Pid = ?MF(World, get_pid)),
	if
		Pid =:= self() ->
			case ?MF(World, get_ext), ?MODULE) of
				{_, Pid1, _} when is_pid(Pid1) ->
					zm_scene:Apply(Pid1, ?APPLY, {write_apply,
						Do, [{ID, Msg, World}, self()]}, 0);
				{_, Args} ->
					?MODULE:Do(Args, {ID, Msg, World}, self())
			end;
		true ->
			case ?MF(World, get_ext), ?MODULE) of
				{_, Pid1, _} when is_pid(Pid1) ->
					zm_scene:Apply(Pid1, ?APPLY, {write_apply,
						Do, [{ID, Msg, World}, self()]}, 0);
				_ ->
					zm_scene:Apply(Pid, ?APPLY, {write_apply,
						Do, [{ID, Msg}, self()]}, 0)
			end
	end.

% action 或 frame方法调用
apply2(Apply, Do, World, Msg) ->
	Pid = ?MF(World, get_pid)),
	if
		Pid =:= self() ->
			case ?MF(World, get_ext), ?MODULE) of
				{_, Pid1, _} when is_pid(Pid1) ->
					zm_scene:Apply(Pid1, ?APPLY, {write_apply,
						Do, [{Msg, World}, self()]}, 0);
				{_, Args} ->
					?MODULE:Do(Args, {Msg, World}, self())
			end;
		true ->
			case ?MF(World, get_ext), ?MODULE) of
				{_, Pid1, _} when is_pid(Pid1) ->
					zm_scene:Apply(Pid1, ?APPLY, {write_apply,
						Do, [{Msg, World}, self()]}, 0);
				_ ->
					zm_scene:Apply(Pid, ?APPLY, {write_apply,
						Do, [Msg, self()]}, 0)
			end
	end.

% 初始化AI列表
init_(Ets, [{M, F, A} | T], TypeName, Now) ->
	M:F(A, TypeName, Ets, Now),
	init_(Ets, T, TypeName, Now);
init_(_, _, _, _) ->
	ok.

% 检查帧时间是否到达
do_frame(Ets, World, Now, FrameTime) ->
	Msg = {frame, Now},
	case ets:lookup_element(Ets, ?NEXT_TIME, 2) of
		Time when Time =< Now ->
			ets:insert(Ets, {?NEXT_TIME, Now + FrameTime}),
			frame(Ets, ets:prev(Ets, ?MAX_ID), World, Msg, []);
		_ ->
			none
	end.

% 遍历AI
frame(Ets, ID, Args, Msg, ErrL) when is_integer(ID) ->
	case ets:lookup(Ets, ID) of
		[{_, M, A}] ->
			try M:cast(A, {Msg, Args, Ets, ID}) of
				{update, A1} ->
					try
						ets:update_element(Ets, ID, {3, A1})
					catch
						_:_ -> update_error
					end,
					frame(Ets, ets:prev(Ets, ID), Args, Msg, ErrL);
				delete ->
					ets:delete(Ets, ID),
					frame(Ets, ets:prev(Ets, ID), Args, Msg, ErrL);
				_ ->
					frame(Ets, ets:prev(Ets, ID), Args, Msg, ErrL)
			catch
				Error:Reason ->
					frame(Ets, ets:prev(Ets, ID), Args, Msg,
						[{Error, {Reason, {ID, M, A}}, erlang:get_stacktrace()} | ErrL])
			end;
		_ ->
			frame(Ets, ets:prev(Ets, ID), Args, Msg, ErrL)
	end;
frame(_Ets, _, _Args, _Msg, ErrL) ->
	case ErrL of
		[{E, _, ST} | _] ->
			erlang:raise(E, ErrL, ST);
		_ -> ok
	end.
