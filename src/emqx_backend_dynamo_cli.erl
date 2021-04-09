%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 3月 2021 下午4:32
%%%-------------------------------------------------------------------
-module(emqx_backend_dynamo_cli).

-export([logger_header/0]).

-behaviour(ecpool_worker).
-include("../include/emqx_backend_dynamo.hrl").
-include("../include/emqx.hrl").
-include("../include/logger.hrl").


-export([client_connected/2,
  subscribe_lookup/2,
  client_disconnected/2,
  message_fetch_for_queue/2,
  lookup_retain/2,
  acked_delete/2,
  message_publish/2,
  message_store/2,
  message_retain/2,
  delete_retain/2,
  message_acked_for_queue/2]).

-export([dynamo_insert/3,
  dynamo_update/5,
  dynamo_scan/2,
  dynamo_get/3,
  dynamo_delete/3,
  dynamo_query/4]).

-export([connect/1]).


client_connected(Pool, #{clientid := ClientId}) ->
  Items = [{<<"clientid">>, ClientId},
    {<<"connect_state">>, 1},
    {<<"node">>, a2b(node())},
    {<<"online_at">>, erlang:system_time(millisecond)},
    {<<"offline_at">>, 0}],
  dynamo_insert(Pool, <<"mqtt_client">>, Items).

client_disconnected(Pool, #{clientid := ClientId}) ->
  Key = [{<<"clientid">>, ClientId}],
  Expression = <<"SET connect_state = :val1, offline_at "
  "= :val2">>,
  Opts = [{expression_attribute_values,
    [{<<":val1">>, 0},
      {<<":val2">>, erlang:system_time(millisecond)}]},
    {return_values, updated_new}],
  dynamo_update(Pool,
    <<"mqtt_client">>,
    Key,
    Expression,
    Opts),
  ok.

subscribe_lookup(Pool, #{clientid := ClientId}) ->
  Expression = <<"clientid = :clientid">>,
  Opts = [{expression_attribute_values,
    [{<<":clientid">>, ClientId}]}],
  case dynamo_query(Pool,
    <<"mqtt_sub">>,
    Expression,
    Opts)
  of
    {ok, []} -> [];
    {ok, Items} ->
      [{proplists:get_value(<<"topic">>, Item),
        #{qos => proplists:get_value(<<"qos">>, Item)}}
        || Item <- Items];
    {error, Error} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header ++
              "Lookup subscription error: ~p",
              [Error]}
          end,
            mfa =>
            {emqx_backend_dynamo_cli, subscribe_lookup, 2},
            line => 67})
      end,
      []
  end.

message_publish(Pool,
    Msg = #message{id = MsgId, topic = Topic}) ->
  message_to_dynamo(Pool, Msg),
  TopicMsgKey = [{<<"topic">>, Topic}],
  Expression = <<"ADD MsgId :msgid">>,
  Opts = [{expression_attribute_values,
    [{<<":msgid">>, {ss, [to_b62(MsgId)]}}]},
    {return_values, none}],
  dynamo_update(Pool,
    <<"mqtt_topic_msg_map">>,
    TopicMsgKey,
    Expression,
    Opts),
  Msg.

message_store(Pool, Msg) ->
  message_to_dynamo(Pool, Msg),
  Msg.

message_to_dynamo(Pool, Msg) ->
  MsgItem = feed_var([<<"msgid">>,
    <<"topic">>,
    <<"sender">>,
    <<"qos">>,
    <<"retain">>,
    <<"payload">>,
    <<"arrived">>],
    Msg),
  dynamo_insert(Pool, <<"mqtt_msg">>, MsgItem).

message_fetch_for_queue(Pool, #{topic := Topic}) ->
  TopicMsgIds = message_ids(Pool,
    <<"mqtt_topic_msg_map">>,
    <<"topic = :topic">>,
    Topic),
  lookup_msg(Pool, TopicMsgIds).

lookup_msg(Pool, MsgIds) ->
  Keys = [{<<"msgid">>, MsgId} || MsgId <- MsgIds],
  case dynamo_get(Pool, <<"mqtt_msg">>, Keys) of
    {ok, []} -> [];
    {ok, Msgs} -> [record_to_msg(Msg) || Msg <- Msgs];
    {error, Error} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header ++
              "Lookup msgs error: ~p",
              [Error]}
          end,
            mfa => {emqx_backend_dynamo_cli, lookup_msg, 2},
            line => 98})
      end,
      []
  end.

message_ids(Pool, TableName, Expression, Value) ->
  Opts = [{expression_attribute_values,
    [{<<":topic">>, Value}]}],
  case dynamo_query(Pool, TableName, Expression, Opts) of
    {ok, []} -> [];
    {ok, Items} ->
      MsgIdGroups = [proplists:get_value(<<"MsgId">>, Item)
        || Item <- Items],
      lists:flatten(MsgIdGroups);
    {error, Error} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header ++
              "Lookup subscription error: ~p",
              [Error]}
          end,
            mfa => {emqx_backend_dynamo_cli, message_ids, 4},
            line => 111})
      end,
      []
  end.

lookup_retain(Pool, #{topic := Topic}) ->
  Key = [{<<"topic">>, Topic}],
  case dynamo_get(Pool, <<"mqtt_retain">>, Key) of
    {ok, []} -> [];
    {ok, Msgs} -> [record_to_msg(Msg) || Msg <- Msgs];
    {error, Error} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header ++
              "Lookup subscription error: ~p",
              [Error]}
          end,
            mfa => {emqx_backend_dynamo_cli, lookup_retain, 2},
            line => 122})
      end,
      []
  end.

acked_delete(Pool,
    #{clientid := ClientId, topic := Topic}) ->
  Key = [{<<"topic">>, Topic},
    {<<"clientid">>, ClientId}],
  dynamo_delete(Pool, <<"mqtt_acked">>, Key),
  ok.

message_acked_for_queue(Pool,
    #{clientid := ClientId, msgid := MsgId0,
      topic := Topic}) ->
  MsgId = to_b62(MsgId0),
  MsgKey = [{<<"msgid">>, MsgId}],
  TopicMsgKey = [{<<"topic">>, Topic}],
  Expression = <<"Delete MsgId :msgid">>,
  Opts = [{expression_attribute_values,
    [{<<":msgid">>, {ss, [MsgId]}}]},
    {return_values, none}],
  AckedItem = [{<<"clientid">>, ClientId},
    {<<"topic">>, Topic},
    {<<"msgid">>, MsgId}],
  dynamo_insert(Pool, <<"mqtt_acked">>, AckedItem),
  dynamo_delete(Pool, <<"mqtt_msg">>, MsgKey),
  dynamo_update(Pool,
    <<"mqtt_topic_msg_map">>,
    TopicMsgKey,
    Expression,
    Opts),
  ok.

message_retain(Pool, Msg) ->
  MsgItem = feed_var([<<"msgid">>,
    <<"topic">>,
    <<"sender">>,
    <<"qos">>,
    <<"retain">>,
    <<"payload">>,
    <<"arrived">>],
    Msg),
  dynamo_insert(Pool, <<"mqtt_retain">>, MsgItem),
  Msg.

delete_retain(Pool, Msg) ->
  Key = [<<"topic">>],
  dynamo_delete(Pool,
    <<"mqtt_retain">>,
    feed_var(Key, Msg)),
  Msg.

feed_var(Params, Msg) -> feed_var(Params, Msg, []).

feed_var([], _Msg, Acc) -> lists:reverse(Acc);
feed_var([<<"topic">> | Params],
    Msg = #message{topic = Topic}, Acc) ->
  feed_var(Params, Msg, [{<<"topic">>, Topic} | Acc]);
feed_var([<<"topic">> | Params], Vals, Acc)
  when is_list(Vals) ->
  feed_var(Params,
    Vals,
    [{<<"topic">>, proplists:get_value(topic, Vals, null)}
      | Acc]);
feed_var([<<"msgid">> | Params],
    Msg = #message{id = MsgId}, Acc) ->
  feed_var(Params,
    Msg,
    [{<<"msgid">>, to_b62(MsgId)} | Acc]);
feed_var([<<"msgid">> | Params], Vals, Acc)
  when is_list(Vals) ->
  feed_var(Params,
    Vals,
    [{<<"msgid">>,
      to_b62(proplists:get_value(msgid, Vals, null))}
      | Acc]);
feed_var([<<"sender">> | Params],
    Msg = #message{from = From}, Acc) ->
  ClientId = to_binary(From),
  feed_var(Params, Msg, [{<<"sender">>, ClientId} | Acc]);
feed_var([<<"sender">> | Params], Vals, Acc)
  when is_list(Vals) ->
  ClientId = to_binary(proplists:get_value(clientid,
    Vals,
    null)),
  feed_var(Params,
    Vals,
    [{<<"sender">>, ClientId} | Acc]);
feed_var([<<"clientid">> | Params],
    Msg = #message{from = From}, Acc) ->
  ClientId = to_binary(From),
  feed_var(Params,
    Msg,
    [{<<"clientid">>, ClientId} | Acc]);
feed_var([<<"clientid">> | Params], Vals, Acc)
  when is_list(Vals) ->
  ClientId = to_binary(proplists:get_value(clientid,
    Vals,
    null)),
  feed_var(Params,
    Vals,
    [{<<"clientid">>, ClientId} | Acc]);
feed_var([<<"qos">> | Params],
    Msg = #message{qos = Qos}, Acc) ->
  feed_var(Params, Msg, [{<<"qos">>, Qos} | Acc]);
feed_var([<<"qos">> | Params], Vals, Acc)
  when is_list(Vals) ->
  feed_var(Params,
    Vals,
    [{<<"qos">>, proplists:get_value(qos, Vals, null)}
      | Acc]);
feed_var([<<"retain">> | Params],
    Msg = #message{flags = #{retain := Retain}}, Acc) ->
  feed_var(Params,
    Msg,
    [{<<"retain">>, i(Retain)} | Acc]);
feed_var([<<"payload">> | Params],
    Msg = #message{payload = Payload}, Acc) ->
  feed_var(Params, Msg, [{<<"payload">>, Payload} | Acc]);
feed_var([<<"arrived">> | Params],
    Msg = #message{timestamp = Ts}, Acc) ->
  feed_var(Params, Msg, [{<<"arrived">>, Ts} | Acc]);
feed_var([_Other | Params], Msg, Acc) ->
  feed_var(Params, Msg, [null | Acc]).

i(true) -> 1;
i(false) -> 0.

a2b(A) -> erlang:atom_to_binary(A, utf8).

to_binary(From) when is_atom(From) -> a2b(From);
to_binary(From) when is_binary(From) -> From.

record_to_msg(Record) ->
  record_to_msg(Record, #message{headers = #{}}).

record_to_msg([], Msg) -> Msg;
record_to_msg([{<<"id">>, Id} | Record], Msg) ->
  record_to_msg(Record,
    emqx_message:set_header(dynamo_id, Id, Msg));
record_to_msg([{<<"msgid">>, MsgId} | Record], Msg) ->
  record_to_msg(Record,
    Msg#message{id = from_b62(MsgId)});
record_to_msg([{<<"topic">>, Topic} | Record], Msg) ->
  record_to_msg(Record, Msg#message{topic = Topic});
record_to_msg([{<<"sender">>, Sender} | Record], Msg) ->
  record_to_msg(Record, Msg#message{from = Sender});
record_to_msg([{<<"qos">>, Qos} | Record], Msg) ->
  record_to_msg(Record, Msg#message{qos = Qos});
record_to_msg([{<<"retain">>, R} | Record], Msg) ->
  record_to_msg(Record,
    Msg#message{flags = #{retain => b(R)}});
record_to_msg([{<<"payload">>, Payload} | Record],
    Msg) ->
  record_to_msg(Record, Msg#message{payload = Payload});
record_to_msg([{<<"arrived">>, Arrived} | Record],
    Msg) ->
  record_to_msg(Record, Msg#message{timestamp = Arrived});
record_to_msg([_ | Record], Msg) ->
  record_to_msg(Record, Msg).

b(0) -> false;
b(1) -> true.

to_b62(MsgId) -> emqx_guid:to_base62(MsgId).

from_b62(MsgId) -> emqx_guid:from_base62(MsgId).

connect(Opts) ->
  emqx_backend_dynamo_client:start_link(Opts).

dynamo_insert(Pool, Table, Items) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header ++
          "dynamo_insert_update Table:~p, Items:~p",
          [Table, Items]}
      end,
        mfa => {emqx_backend_dynamo_cli, dynamo_insert, 3},
        line => 274})
  end,
  case ecpool:with_client(Pool,
    fun (C) ->
      emqx_backend_dynamo_client:insert_item(C,
        Table,
        Items)
    end)
  of
    {ok, _Return} -> ok;
    {error, Term} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header ++
              "Failed to ~p, Error : ~p",
              [Table, Term]}
          end,
            mfa => {emqx_backend_dynamo_cli, dynamo_insert, 3},
            line => 279})
      end,
      ok
  end.

dynamo_update(Pool, Table, Key, UpdatesOrExpression,
    Opts) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header ++
          "dynamo_insert_update Table:~p, Key:~p",
          [Table, Key]}
      end,
        mfa => {emqx_backend_dynamo_cli, dynamo_update, 5},
        line => 284})
  end,
  case ecpool:with_client(Pool,
    fun (C) ->
      emqx_backend_dynamo_client:update_item(C,
        Table,
        Key,
        UpdatesOrExpression,
        Opts)
    end)
  of
    {ok, _Return} -> ok;
    {error, Term} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header ++
              "Failed to update ~p, Error : ~p",
              [Table, Term]}
          end,
            mfa => {emqx_backend_dynamo_cli, dynamo_update, 5},
            line => 289})
      end,
      ok
  end.

dynamo_scan(Pool, Table) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header ++ "dynamo_scan table: ~p",
          [Table]}
      end,
        mfa => {emqx_backend_dynamo_cli, dynamo_scan, 2},
        line => 294})
  end,
  case ecpool:with_client(Pool,
    fun (C) ->
      emqx_backend_dynamo_client:scan_items(C,
        Table)
    end)
  of
    {ok, _Return} -> ok;
    {error, Term} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header ++
              "Failed to update ~p, Error : ~p",
              [Table, Term]}
          end,
            mfa => {emqx_backend_dynamo_cli, dynamo_scan, 2},
            line => 299})
      end,
      ok
  end.

dynamo_get(_Pool, Table, []) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header ++ "dynamo_scan table: ~p",
          [Table]}
      end,
        mfa => {emqx_backend_dynamo_cli, dynamo_get, 3},
        line => 304})
  end,
  {ok, []};
dynamo_get(Pool, Table, Keys) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header ++ "dynamo_scan table: ~p",
          [Table]}
      end,
        mfa => {emqx_backend_dynamo_cli, dynamo_get, 3},
        line => 307})
  end,
  case ecpool:with_client(Pool,
    fun (C) ->
      emqx_backend_dynamo_client:get_items(C,
        Table,
        Keys)
    end)
  of
    {ok, Return} -> {ok, Return};
    {error, Term} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header ++
              "Failed to get ~p, Error : ~p, Key : ~p",
              [Table, Term, Keys]}
          end,
            mfa => {emqx_backend_dynamo_cli, dynamo_get, 3},
            line => 312})
      end,
      {error, Term}
  end.

dynamo_delete(Pool, Table, Key) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header ++
          "dynamo_delete Table:~p, Key:~p",
          [Table, Key]}
      end,
        mfa => {emqx_backend_dynamo_cli, dynamo_delete, 3},
        line => 317})
  end,
  case ecpool:with_client(Pool,
    fun (C) ->
      emqx_backend_dynamo_client:delete_item(C,
        Table,
        Key)
    end)
  of
    {ok, _} -> ok;
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header ++
              "Failed to delete Table ~p: Reason: ~p",
              [Table, Reason]}
          end,
            mfa => {emqx_backend_dynamo_cli, dynamo_delete, 3},
            line => 322})
      end,
      false
  end.

dynamo_query(Pool, Table, KeyConditionsOrExpression,
    Opts) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header ++ "dynamo_query Table:~p",
          [Table]}
      end,
        mfa => {emqx_backend_dynamo_cli, dynamo_query, 4},
        line => 327})
  end,
  case ecpool:with_client(Pool,
    fun (C) ->
      emqx_backend_dynamo_client:query_items(C,
        Table,
        KeyConditionsOrExpression,
        Opts)
    end)
  of
    {ok, Return} -> {ok, Return};
    {error, Term} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header ++
              "Failed to query ~p, Error : ~p",
              [Table, Term]}
          end,
            mfa => {emqx_backend_dynamo_cli, dynamo_query, 4},
            line => 332})
      end,
      {error, Term}
  end.

logger_header() -> "".
