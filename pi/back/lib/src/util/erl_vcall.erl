-module(erl_vcall).

-export([init/0,fun_register/4,invoke/3,asyn_invoke/4,reload_dll/1,release_dll/0]). 


%% -----------------------------------------------------------------
%%@doc 初始化加载vcall 对应dll
%% @spec init() -> return()
%% where
%%  return() = any()
%%@end
%% ----------------------------------------------------------------	
  

init() ->  
    Path = case os:type() of 
			   {win32,_} ->
				   "./erl_vcall";
			   _ ->
				   "./liberl_vcall"
		   end,
	try  erlang:load_nif(Path, 0) of
		Val -> Val
    catch
		_:_ -> {error,throwexception}
	end.

%% -----------------------------------------------------------------
%%@doc 函数注册
%% @spec fun_register(_::string(),_::string(),_::Number(),_::list()) -> return()
%% where
%%  return() = tupple()
%%@end
%% ----------------------------------------------------------------	
  
fun_register(_,_,_,_) -> 
	
    io:format("this function is not defined!~n").

%% -----------------------------------------------------------------
%%@doc 函数调用
%% @spec invoke(_::string(),_::Number(),_::list()) -> return()
%% where
%%  return() = tupple()
%%@end
%% ----------------------------------------------------------------	
invoke(_,_,_) ->
   erlang:error(function_clause).


%% -----------------------------------------------------------------
%%@doc 重新加载dll
%% @spec reload_dll(_::string()) -> return()
%% where
%%  return() = Number()
%%@end
%% ----------------------------------------------------------------	
reload_dll(_) ->
	erlang:error(function_clause).

%% -----------------------------------------------------------------
%%@doc 释放资源
%% @spec release_dll() -> return()
%% where
%%  return() = Number()
%%@end
%% ----------------------------------------------------------------	
release_dll() ->
	erlang:error(function_clause).

asyn_invoke(_,_,_,_) ->
	erlang:error(function_clause).
	

   
