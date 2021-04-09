%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 3月 2021 下午4:33
%%%-------------------------------------------------------------------
-module(emqx_backend_dynamo_client).
-export([logger_header/0]).
-include("../include/logger.hrl").
-include("../include/erlcloud_aws.hrl").

-behaviour(gen_server).

-export([start_link/1,
  aws_config/1,
  scan_items/2,
  insert_item/3,
  delete_items/3,
  delete_item/3,
  query_items/4,
  get_items/3,
  update_item/5,
  list_tables/1,
  list_tables/2]).

-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3,
  format_status/2]).

-vsn("4.2.5").

aws_config(Pid) -> gen_server:call(Pid, get_aws_config).

list_tables(Pid) -> gen_server:call(Pid, list_tables).

list_tables(Pid, Timeout) ->
  gen_server:call(Pid, list_tables, Timeout).

insert_item(Pid, Table, Item) ->
  gen_server:call(Pid,
    {insert_item, Table, Item},
    infinity).

update_item(Pid, Table, Key, UpdatesOrExpression,
    Opts) ->
  gen_server:call(Pid,
    {update_item, Table, Key, UpdatesOrExpression, Opts},
    infinity).

delete_items(Pid, Table, Keys) ->
  gen_server:call(Pid,
    {delete_items, Table, Keys},
    infinity).

delete_item(Pid, Table, Key) ->
  gen_server:call(Pid,
    {delete_item, Table, Key},
    infinity).

query_items(Pid, Table, KeyConditionsOrExpression,
    Opts) ->
  gen_server:call(Pid,
    {query_items, Table, KeyConditionsOrExpression, Opts},
    infinity).

scan_items(Pid, Table) ->
  gen_server:call(Pid, {scan_items, Table}, infinity).

get_items(Pid, Table, Keys) ->
  gen_server:call(Pid,
    {get_items, Table, Keys},
    infinity).

start_link(Options) when is_map(Options) ->
  start_link(maps:to_list(Options));
start_link(Options) ->
  gen_server:start_link(emqx_backend_dynamo_client,
    Options,
    []).

-spec init(Options :: list()) -> {ok, aws_config()}.

init(Options) ->
  process_flag(trap_exit, true),
  GetD = fun (K, D) -> proplists:get_value(K, Options, D)
         end,
  Get = fun (K) -> proplists:get_value(K, Options) end,
  DDBHost = GetD(host, "localhost"),
  DDBPort = GetD(port, 8000),
  DDBScheme = GetD(scheme, "http://"),
  AccessKeyID = Get(aws_access_key_id),
  SecretAccessKey = Get(aws_secret_access_key),
  erlcloud_ddb2:configure(AccessKeyID,
    SecretAccessKey,
    DDBHost,
    DDBPort,
    DDBScheme),
  {ok, erlcloud_aws:default_config()}.

handle_call(get_aws_config, _From, AwsConfig = State) ->
  {reply, AwsConfig, State};
handle_call(list_tables, _From, State) ->
  Return = erlcloud_ddb2:list_tables(),
  {reply, Return, State};
handle_call({insert_item, Table, Item}, _From, State) ->
  Return = erlcloud_ddb2:put_item(Table, Item),
  {reply, Return, State};
handle_call({update_item,
  Table,
  Key,
  UpdatesOrExpression,
  Opts},
    _From, State) ->
  Return = erlcloud_ddb2:update_item(Table,
    Key,
    UpdatesOrExpression,
    Opts),
  {reply, Return, State};
handle_call({delete_items, Table, Keys}, _From,
    State) ->
  Return = erlcloud_ddb_util:delete_all(Table, Keys),
  {reply, Return, State};
handle_call({delete_item, Table, Key}, _From, State) ->
  Return = erlcloud_ddb2:delete_item(Table, Key),
  {reply, Return, State};
handle_call({query_items,
  Table,
  KeyConditionsOrExpression,
  Opts},
    _From, State) ->
  Return = erlcloud_ddb_util:q_all(Table,
    KeyConditionsOrExpression,
    Opts),
  {reply, Return, State};
handle_call({get_items, Table, Keys}, _From, State) ->
  Return = erlcloud_ddb_util:get_all(Table, Keys),
  {reply, Return, State};
handle_call({scan_items, Table}, _From, State) ->
  Return = erlcloud_ddb2:scan(Table),
  {reply, Return, State};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Request, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

-spec format_status(Opt :: normal | terminate,
    Status :: list()) -> Status :: term().

format_status(_Opt, Status) -> Status.

logger_header() -> "".
