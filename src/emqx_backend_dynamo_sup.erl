%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 3月 2021 下午4:33
%%%-------------------------------------------------------------------

-module(emqx_backend_dynamo_sup).
-include("../include/emqx_backend_dynamo.hrl").

-behaviour(supervisor).
-export([start_link/1]).
-export([init/1]).


start_link(Pools) ->
  supervisor:start_link({local, emqx_backend_dynamo_sup}, emqx_backend_dynamo_sup, [Pools]).

init([Pools]) ->
  SupFlags = #{strategy => one_for_one, intensity => 10, period => 100},
  PoolSpecs = [pool_spec(Pool, Opts) || {Pool, Opts} <- Pools],
  {ok, {SupFlags, PoolSpecs}}.

pool_spec(Pool, Opts) ->
  ecpool:pool_spec({emqx_backend_dynamo, Pool}, emqx_backend_dynamo:pool_name(Pool), emqx_backend_dynamo_cli, Opts).

