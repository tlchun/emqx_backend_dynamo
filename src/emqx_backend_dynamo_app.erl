%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 3月 2021 下午4:31
%%%-------------------------------------------------------------------

-module(emqx_backend_dynamo_app).
-include("../include/emqx_backend_dynamo.hrl").


-behaviour(application).

-emqx_plugin(backend).

-export([start/2, stop/1]).


start(_StartType, _StartArgs) ->
  Pools = application:get_env(emqx_backend_dynamo, pools, []),
  Region = application:get_env(emqx_backend_dynamo, region, "us-west-2"),
  application:set_env(erlcloud, aws_region, Region),
  {ok, Sup} = emqx_backend_dynamo_sup:start_link(Pools),
  emqx_backend_dynamo:register_metrics(),
  emqx_backend_dynamo:load(),
  {ok, Sup}.

stop(_State) -> emqx_backend_dynamo:unload().
