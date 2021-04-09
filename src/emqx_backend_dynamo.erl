%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 3月 2021 下午4:31
%%%-------------------------------------------------------------------
-module(emqx_backend_dynamo).

-include("../include/emqx_backend_dynamo.hrl").
-include("../include/emqx.hrl").


-export([pool_name/1]).

-export([register_metrics/0, load/0, unload/0]).

-export([on_client_connected/3,
  on_client_disconnected/4,
  on_subscribe_lookup/3,
  on_message_publish/2,
  on_message_store/2,
  on_message_fetch_for_queue/4,
  on_retain_lookup/4,
  on_acked_delete/4,
  on_message_retain/2,
  on_retain_delete/2,
  on_message_acked_for_queue/3]).


register_metrics() ->
  [emqx_metrics:new(MetricName)
    || MetricName
    <- ['backend.dynamodb.client_connected',
      'backend.dynamodb.client_disconnected',
      'backend.dynamodb.message_publish',
      'backend.dynamodb.message_store',
      'backend.dynamodb.subscribe_lookup',
      'backend.dynamodb.message_fetch_for_queue',
      'backend.dynamodb.retain_lookup',
      'backend.dynamodb.acked_delete',
      'backend.dynamodb.message_retain',
      'backend.dynamodb.retain_delete',
      'backend.dynamodb.message_acked_for_queue']].

pool_name(Pool) ->
  list_to_atom(lists:concat([emqx_backend_dynamo,
    '_',
    Pool])).

load() ->
  HookList =
    parse_hook(application:get_env(emqx_backend_dynamo,
      hooks,
      [])),
  lists:foreach(fun ({Hook, Action, Pool, Filter}) ->
    case proplists:get_value(<<"function">>, Action) of
      undefined -> ok;
      Fun -> load_(Hook, b2a(Fun), {Filter, Pool})
    end
                end,
    HookList),
  io:format("~s is loaded.~n", [emqx_backend_dynamo]),
  ok.

load_(Hook, Fun, Params) ->
  case Hook of
    'client.connected' ->
      emqx:hook(Hook,
        fun emqx_backend_dynamo:Fun/3,
        [Params]);
    'client.disconnected' ->
      emqx:hook(Hook,
        fun emqx_backend_dynamo:Fun/4,
        [Params]);
    'session.subscribed' ->
      emqx:hook(Hook,
        fun emqx_backend_dynamo:Fun/4,
        [Params]);
    'session.unsubscribed' ->
      emqx:hook(Hook,
        fun emqx_backend_dynamo:Fun/4,
        [Params]);
    'message.publish' ->
      emqx:hook(Hook,
        fun emqx_backend_dynamo:Fun/2,
        [Params]);
    'message.acked' ->
      emqx:hook(Hook, fun emqx_backend_dynamo:Fun/3, [Params])
  end.

unload() ->
  HookList =
    parse_hook(application:get_env(emqx_backend_dynamo,
      hooks,
      [])),
  lists:foreach(fun ({Hook, Action, _Pool, _Filter}) ->
    case proplists:get_value(<<"function">>, Action) of
      undefined -> ok;
      Fun -> unload_(Hook, b2a(Fun))
    end
                end,
    HookList),
  io:format("~s is unloaded.~n", [emqx_backend_dynamo]),
  ok.

unload_(Hook, Fun) ->
  case Hook of
    'client.connected' ->
      emqx:unhook(Hook, fun emqx_backend_dynamo:Fun/3);
    'client.disconnected' ->
      emqx:unhook(Hook, fun emqx_backend_dynamo:Fun/4);
    'session.subscribed' ->
      emqx:unhook(Hook, fun emqx_backend_dynamo:Fun/4);
    'session.unsubscribed' ->
      emqx:unhook(Hook, fun emqx_backend_dynamo:Fun/4);
    'message.publish' ->
      emqx:unhook(Hook, fun emqx_backend_dynamo:Fun/2);
    'message.acked' ->
      emqx:unhook(Hook, fun emqx_backend_dynamo:Fun/3)
  end.

on_client_connected(#{clientid := ClientId}, _ConnInfo,
    {Filter, Pool}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.dynamodb.client_connected'),
    emqx_backend_dynamo_cli:client_connected(Pool,
      #{clientid =>
      ClientId})
              end,
    undefined,
    Filter).

on_subscribe_lookup(#{clientid := ClientId}, _ConnInfo,
    {Filter, Pool}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.dynamodb.subscribe_lookup'),
    case emqx_backend_dynamo_cli:subscribe_lookup(Pool,
      #{clientid
      =>
      ClientId})
    of
      [] -> ok;
      TopicTable ->
        self() ! {subscribe, TopicTable},
        ok
    end
              end,
    undefined,
    Filter).

on_client_disconnected(#{clientid := ClientId}, _Reason,
    _ConnInfo, {Filter, Pool}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.dynamodb.client_disconnected'),
    emqx_backend_dynamo_cli:client_disconnected(Pool,
      #{clientid
      =>
      ClientId})
              end,
    undefined,
    Filter).

on_message_publish(Msg = #message{flags =
#{retain := true},
  payload = <<>>},
    _Rule) ->
  {ok, Msg};
on_message_publish(Msg = #message{qos = Qos},
    {_Filter, _Pool})
  when Qos =:= 0 ->
  {ok, Msg};
on_message_publish(Msg0 = #message{topic = Topic},
    {Filter, Pool}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.dynamodb.message_publish'),
    Msg = emqx_backend_dynamo_cli:message_publish(Pool,
      Msg0),
    {ok, Msg}
              end,
    Msg0,
    Topic,
    Filter).

on_message_store(Msg = #message{flags =
#{retain := true},
  payload = <<>>},
    _Rule) ->
  {ok, Msg};
on_message_store(Msg0 = #message{topic = Topic},
    {Filter, Pool}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.dynamodb.message_store'),
    Msg = emqx_backend_dynamo_cli:message_store(Pool, Msg0),
    {ok, Msg}
              end,
    Msg0,
    Topic,
    Filter).

on_message_fetch_for_queue(_Credential, Topic, Opts,
    {Filter, Pool}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.dynamodb.message_fetch_for_queue'),
    case maps:get(qos, Opts, 0) > 0 andalso
      maps:get(is_new, Opts, true)
    of
      true ->
        MsgInfo = #{topic => Topic},
        MsgList =
          emqx_backend_dynamo_cli:message_fetch_for_queue(Pool,
            MsgInfo),
        [self() ! {deliver, Topic, Msg}
          || Msg <- MsgList];
      false -> ok
    end
              end,
    Topic,
    Filter).

on_retain_lookup(_Client, Topic, _Opts,
    {Filter, Pool}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.dynamodb.retain_lookup'),
    MsgList = emqx_backend_dynamo_cli:lookup_retain(Pool,
      #{topic
      =>
      Topic}),
    [self() !
      {deliver,
        Topic,
        emqx_message:set_header(retained, true, Msg)}
      || Msg <- MsgList]
              end,
    Topic,
    Filter).

on_acked_delete(#{clientid := ClientId}, Topic, _Opts,
    {Filter, Pool}) ->
  with_filter(fun () ->
    Msg = #{clientid => ClientId, topic => Topic},
    emqx_metrics:inc('backend.dynamodb.acked_delete'),
    emqx_backend_dynamo_cli:acked_delete(Pool, Msg)
              end,
    Topic,
    Filter).

on_message_retain(Msg = #message{flags =
#{retain := false}},
    _Rule) ->
  {ok, Msg};
on_message_retain(Msg = #message{flags =
#{retain := true},
  payload = <<>>},
    _Rule) ->
  {ok, Msg};
on_message_retain(Msg0 = #message{flags =
#{retain := true},
  topic = Topic, headers = Headers0},
    {Filter, Pool}) ->
  Headers = case erlang:is_map(Headers0) of
              true -> Headers0;
              false -> #{}
            end,
  case maps:find(retained, Headers) of
    {ok, true} -> {ok, Msg0};
    _ ->
      with_filter(fun () ->
        emqx_metrics:inc('backend.dynamodb.message_retain'),
        Msg =
          emqx_backend_dynamo_cli:message_retain(Pool,
            Msg0),
        {ok, Msg}
                  end,
        Msg0,
        Topic,
        Filter)
  end;
on_message_retain(Msg, _Rule) -> {ok, Msg}.

on_retain_delete(Msg0 = #message{flags =
#{retain := true},
  topic = Topic, payload = <<>>},
    {Filter, Pool}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.dynamodb.retain_delete'),
    Msg = emqx_backend_dynamo_cli:delete_retain(Pool, Msg0),
    {ok, Msg}
              end,
    Msg0,
    Topic,
    Filter);
on_retain_delete(Msg, _Rule) -> {ok, Msg}.

on_message_acked_for_queue(#{clientid := ClientId},
    #message{topic = Topic, id = MsgId},
    {Filter, Pool}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.dynamodb.message_acked_for_queue'),
    emqx_backend_dynamo_cli:message_acked_for_queue(Pool,
      #{clientid
      =>
      ClientId,
        topic
        =>
        Topic,
        msgid
        =>
        MsgId})
              end,
    Topic,
    Filter).

parse_hook(Hooks) -> parse_hook(Hooks, []).

parse_hook([], Acc) -> Acc;
parse_hook([{Hook, Item} | Hooks], Acc) ->
  Params = emqx_json:decode(Item),
  Action = proplists:get_value(<<"action">>, Params),
  Pool = proplists:get_value(<<"pool">>, Params),
  Filter = proplists:get_value(<<"topic">>, Params),
  parse_hook(Hooks,
    [{l2a(Hook), Action, pool_name(b2a(Pool)), Filter}
      | Acc]).

with_filter(Fun, _, undefined) ->
  Fun(),
  ok;
with_filter(Fun, Topic, Filter) ->
  case emqx_topic:match(Topic, Filter) of
    true ->
      Fun(),
      ok;
    false -> ok
  end.

with_filter(Fun, _, _, undefined) -> Fun();
with_filter(Fun, Msg, Topic, Filter) ->
  case emqx_topic:match(Topic, Filter) of
    true -> Fun();
    false -> {ok, Msg}
  end.

l2a(L) -> erlang:list_to_atom(L).

b2a(B) -> erlang:binary_to_atom(B, utf8).
