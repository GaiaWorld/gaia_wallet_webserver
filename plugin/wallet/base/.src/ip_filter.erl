%% Author: Administrator
%% Created: 2011-7-5
%% Description: TODO: Add description to ip_filter
-module(ip_filter).

%%
%% Include files
%%
-include("ip_filter.hrl").

%%
%% Exported Functions
%%
-export([filter_ipv4/2]).

%%
%% API Functions
%%

filter_ipv4(Rule_List, {_, _, _, _} = IP) when is_list(Rule_List) ->
    filter_ipv4_1(Rule_List, IP);
filter_ipv4(Rule_List, IP) when is_list(Rule_List), is_list(IP) ->
    L = string:tokens(IP, ?IPV4_STRING_SPLIT_CHAR),
    T = list_to_tuple([list_to_integer(X) || X <- L]),
    filter_ipv4_1(Rule_List, T).

%%
%% Local Functions
%%

filter_ipv4_1([{_, _, _, _} = Rule | T], IP) ->
    case rule_filter_ipv4(tuple_to_list(Rule), tuple_to_list(IP), []) of
        {ok, IP} = OK ->
            OK;
        {error, _} ->
            filter_ipv4_1(T, IP)
    end;
filter_ipv4_1([{{_, _, _, _}, {_, _, _, _} = NewIP} = Rule | T], IP) ->
    case rule_mapping_ipv4(Rule, IP) of
        {ok, NewIP} = OK ->
            OK;
        {error, _} ->
            filter_ipv4_1(T, IP)
    end;
filter_ipv4_1([], _) ->
    {error, invalid_ip}.

rule_filter_ipv4([?ANY | RT], [H | T], L) when is_integer(H), H >= 0 ->
    rule_filter_ipv4(RT, T, [H | L]);
rule_filter_ipv4([{Start, Tail} | RT], [H | T], L) when is_integer(H), H >= Start, H =< Tail ->
    rule_filter_ipv4(RT, T, [H | L]);
rule_filter_ipv4([H | RT], [H | T], L) when is_integer(H) ->
    rule_filter_ipv4(RT, T, [H | L]);
rule_filter_ipv4([], [], L) ->
    {ok, list_to_tuple(lists:reverse(L))};
rule_filter_ipv4(_, _, _L) ->
    {error, invalid_ip}.

rule_mapping_ipv4({Src, Dest}, IP) ->
    case rule_filter_ipv4(tuple_to_list(Src), tuple_to_list(IP), []) of
        {ok, IP} ->
            {ok, Dest};
        {error, _} ->
            {error, invalid_ip}
    end.