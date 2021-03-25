%% =============================================================================
%%  wamp_service_peer.erl -
%%
%%  Copyright (c) 2016-2021 Leapsight. All rights reserved.
%%
%%  Licensed under the Apache License, Version 2.0 (the "License");
%%  you may not use this file except in compliance with the License.
%%  You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%%  Unless required by applicable law or agreed to in writing, software
%%  distributed under the License is distributed on an "AS IS" BASIS,
%%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%  See the License for the specific language governing permissions and
%%  limitations under the License.
%% =============================================================================

-module(wamp_service_peer).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-define(TIMEOUT, 5000).

-define(DEFAULT_REGISTER_OPTS, #{
    disclose_caller => true,
    invoke => roundrobin
}).

-define(DEFAULT_SUBSCRIBE_OPTS, #{
    match => exact,
    get_retained => false
}).

-define(DEFAULT_CALL_OPTS, #{
    timeout => 15000,
    disclose_me => true
}).

-define(DEFAULT_PUBLISH_OPTS, #{
    acknowledge => false,
    disclose_me => true,
    exclude_me => true,
    retain => false
}).

-type router()  ::  #{
    hostname := binary(),
    port := integer(),
    realm := binary(),
    encoding => erlbin | json | msgpack,
    reconnect => boolean(),
    reconnect_max_retries => integer(),
    reconnect_backoff_min => integer(),
    reconnect_backoff_max => integer(),
    reconnect_backoff_type => jitter | normal
}.

-type handler()             ::  {module(), atom(), [integer()]}
                                | {function(), [integer()]}.

-type registrations()           ::  #{
    Uri :: binary() => #{
        handler => handler(),
        options => map()
    }
}.

-type registration_state()           ::  #{
    RegId :: integer() => #{
        uri := binary(),
        handler := handler()
    },
    Uri :: binary() => RegId :: integer()
}.

-type subscriptions()               ::  #{
    Uri :: binary() => #{
        handler => handler(),
        options => map()
    }
}.

-type subscription_state()           ::  #{
    SubsId :: integer() => #{
        uri := binary(),
        handler := handler()
    },
    Uri :: binary() => SubsId :: integer()
}.

-record(state, {
    router                      ::  router(),
    roles                       ::  [wamp_role()],
    connection                  ::  pid() | undefined,
    session_id                  ::  integer() | undefined,
    router_details              ::  map() | list() | undefined,
    max_retries                 ::  integer(),
    retry_count = 0             ::  integer(),
    backoff                     ::  backoff:backoff() | undefined,
    cb_conf                     ::  map(),
    registrations = #{}         ::  registrations(),
    registration_state = #{}    ::  registration_state(),
    subscriptions = #{}         ::  subscriptions(),
    subscription_state = #{}    ::  subscription_state()
}).

-type wamp_role()               ::  caller | callee | subscriber | publisher.

-type wamp_result()             ::  {
                                        ok,
                                        Args :: list(),
                                        KWArgs :: map(),
                                        Details :: map()
                                    }.

-type wamp_error()              ::  {
                                        error,
                                        Uri :: binary(),
                                        Args :: list(),
                                        KWArgs :: map(),
                                        Details :: map()
                                    }.

%% API
-export([start_link/3]).
-export([handle_invocation/2]).
-export([handle_event/2]).

-export([call/5]).
-export([publish/5]).

-export([register/4]).
-export([register/5]).
-export([unregister/2]).
-export([unregister/3]).

-export([subscribe/4]).
-export([subscribe/5]).
-export([unsubscribe/2]).
-export([unsubscribe/3]).

-export([info/1]).


%% GEN_SERVER CALLBACKS
-export([init/1]).
-export([handle_call/3]).
-export([handle_continue/2]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).





%% =============================================================================
%% API
%% =============================================================================



start_link(Config, PeerName, WorkerName) ->
    gen_server:start_link(
        {local, WorkerName}, ?MODULE, [PeerName, WorkerName, Config], []
    ).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec register(
    Peername :: atom() | {atom(), term()} | pid(),
    Uri :: binary(),
    Opts :: map(),
    Handler :: handler()) -> any().

register(Peername, Uri, Opts, Handler) ->
    register(Peername, Uri, Opts, Handler, ?TIMEOUT).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec register(
    Peername :: atom() | {atom(), term()} | pid(),
    Uri :: binary(),
    Opts :: map(),
    Handler :: handler(),
    Timeout :: integer()) -> any().

register(Peername, Uri, Opts, Handler, Timeout) when is_atom(Peername) ->
    register({Peername, Uri}, Uri, Opts, Handler, Timeout);

register({Peername, Term}, Uri, Opts, Handler, Timeout)
when is_atom(Peername) ->
    WorkerPid = pick_worker(Peername, Term),
    register(WorkerPid, Uri, Opts, Handler, Timeout);

register(WorkerPid, Uri, Opts, Handler, Timeout) when is_pid(WorkerPid) ->
    gen_server:call(WorkerPid, {register, Uri, Opts, Handler}, Timeout).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec unregister(
    Peername :: atom() | {atom(), term()} | pid(),
    Uri :: binary()) -> any().

unregister(Peername, Uri) ->
    unregister(Peername, Uri, ?TIMEOUT).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec unregister(
    Peername :: atom() | {atom(), term()} | pid(),
    Uri :: binary() | integer(),
    Timeout :: integer()) -> any().

unregister(Peername, Uri, Timeout) when is_atom(Peername) ->
    unregister({Peername, Uri}, Uri, Timeout);

unregister({Peername, Term}, Uri, Timeout) when is_atom(Peername) ->
    WorkerPid = pick_worker(Peername, Term),
    unregister(WorkerPid, Uri, Timeout);

unregister(WorkerPid, Uri, Timeout)
when is_pid(WorkerPid) andalso is_binary(Uri) ->
    gen_server:call(WorkerPid, {unregister, Uri}, Timeout).



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec subscribe(
    Peername :: atom() | {atom(), term()} | pid(),
    Uri :: binary(),
    Opts :: map(),
    Handler :: handler()) -> any().

subscribe(Peername, Uri, Opts, Handler) ->
    subscribe(Peername, Uri, Opts, Handler, ?TIMEOUT).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec subscribe(
    Peername :: atom() | {atom(), term()} | pid(),
    Uri :: binary(),
    Opts :: map(),
    Handler :: handler(),
    Timeout :: integer()) -> any().

subscribe(Peername, Uri, Opts, Handler, Timeout) when is_atom(Peername) ->
    subscribe({Peername, Uri}, Uri, Opts, Handler, Timeout);

subscribe({Peername, Term}, Uri, Opts, Handler, Timeout)
when is_atom(Peername) ->
    WorkerPid = pick_worker(Peername, Term),
    subscribe(WorkerPid, Uri, Opts, Handler, Timeout);

subscribe(WorkerPid, Uri, Opts, Handler, Timeout) when is_pid(WorkerPid) ->
    gen_server:call(WorkerPid, {subscribe, Uri, Opts, Handler}, Timeout).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec unsubscribe(
    Peername :: atom() | {atom(), term()} | pid(),
    Uri :: binary() | integer()) -> any().

unsubscribe(Peername, Uri) ->
    unsubscribe(Peername, Uri, ?TIMEOUT).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec unsubscribe(
    Peername :: atom() | {atom(), term()} | pid(),
    Uri :: integer(),
    Timeout :: integer()) -> any().

unsubscribe(Peername, Uri, Timeout) when is_atom(Peername) ->
    unsubscribe({Peername, Uri}, Uri, Timeout);

unsubscribe({Peername, Term}, Uri, Timeout) when is_atom(Peername) ->
    WorkerPid = pick_worker(Peername, Term),
    unsubscribe(WorkerPid, Uri, Timeout);

unsubscribe(WorkerPid, Uri, Timeout) when is_pid(WorkerPid) ->
    gen_server:call(WorkerPid, {unsubscribe, Uri}, Timeout).



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec call(
    Peername :: atom() | {atom(), term()} | pid(),
    Uri :: binary(),
    Args :: list(),
    KWArgs :: map(),
    Opts :: map()) -> wamp_result() | wamp_error() | no_return().

call(Peername, Uri, Args, KWArgs, Opts) when is_atom(Peername) ->
    call({Peername, Uri}, Uri, Args, KWArgs, Opts);

call({Peername, Term}, Uri, Args, KWArgs, Opts) when is_atom(Peername) ->
    WorkerPid = pick_worker(Peername, Term),
    call(WorkerPid, Uri, Args, KWArgs, Opts);

call(WorkerPid, Uri, Args, KWArgs, Opts) when is_pid(WorkerPid) ->
    Timeout = maps:get(timeout, Opts, 5000),
    Conn = gen_server:call(WorkerPid, connection, 5000),

    try

        is_pid(Conn) orelse error(no_connection),
        case awre:call(Conn, maps:to_list(Opts), Uri, Args, KWArgs, Timeout) of
            {ok, RDetails, RArgs, RKWArgs} ->
                {ok, RArgs, RKWArgs, RDetails};
            {error, RDetails, RUri, RArgs, RKWArgs} ->
                {error, RUri, RArgs, RKWArgs, RDetails}
        end

    catch
        exit:{timeout, _} ->
            EKWArgs = #{
                <<"procedure_uri">> => Uri,
                <<"timeout">> => Timeout
            },
            {error, <<"wamp.error.timeout">>, [], EKWArgs, #{}};

        Class:Reason:Stacktrace ->
            ?LOG_ERROR(#{
                message => "Error while sending WAMP call request",
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace,
                procedure_uri => Uri,
                args => Args,
                kwargs => KWArgs,
                options => Opts
            }),
            erlang:raise(Class, Reason, Stacktrace)
    end.


%% -----------------------------------------------------------------------------
%% @doc Notice that acknowledge option is not supporte by awre
%% @end
%% -----------------------------------------------------------------------------
-spec publish(
    Peername :: atom() | {atom(), term()} | pid(),
    Topic :: binary(),
    Args :: [any()],
    KWArgs :: map(),
    Opts :: map()) ->
    ok | wamp_error() | no_return().

publish(Peername, Uri, Args, KWArgs, Opts) when is_atom(Peername) ->
    publish({Peername, Uri}, Uri, Args, KWArgs, Opts);

publish({Peername, Term}, Uri, Args, KWArgs, Opts) when is_atom(Peername) ->
    WorkerPid = pick_worker(Peername, Term),
    publish(WorkerPid, Uri, Args, KWArgs, Opts);

publish(WorkerPid, Uri, Args, KWArgs, Opts) when
is_pid(WorkerPid) andalso
is_binary(Uri) andalso
is_map(Opts) andalso
is_list(Args) andalso
is_map(KWArgs) ->
    Timeout = maps:get(timeout, Opts, 5000),
    Conn = gen_server:call(WorkerPid, connection, 5000),

    try

        is_pid(Conn) orelse error(no_connection),
        awre:publish(Conn, maps:to_list(Opts), Uri, Args, KWArgs)

    catch
        exit:{timeout, _} ->
            RKWArgs = #{
                <<"topic_uri">> => Uri,
                <<"timeout">> => Timeout
            },
            {error, <<"wamp.error.timeout">>, [], RKWArgs, #{}};

        Class:Reason:Stacktrace ->
            ?LOG_ERROR(#{
                message => "Error while sending WAMP publish request",
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace,
                topic_uri => Uri,
                args => Args,
                kwargs => KWArgs,
                options => Opts
            }),
            erlang:raise(Class, Reason, Stacktrace)
    end.


info(Peername) when is_atom(Peername) ->
    gproc_pool:defined_workers(Peername).



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



init([PeerName, WorkerName, Config]) ->
    process_flag(trap_exit, true),

    Router = get_router(Config),

    State =  #state{
        router = Router,
        backoff = init_backoff(Router),
        max_retries = maps:get(max_retries, Router, 10),
        registrations = process_registrations(Config),
        subscriptions = process_subscriptions(Config)
    },

    %% We add the worker to the gproc pool
    true = gproc:reg({n, l, WorkerName}),
    _ = gproc_pool:add_worker(PeerName, WorkerName),
    _ = gproc_pool:connect_worker(PeerName, WorkerName),

    {ok, State, {continue, connect}}.



handle_continue(connect, State) ->
    {ok, NewState} = maybe_reconnect(State),
    {noreply, NewState}.


handle_call(connection, _From, State) ->
    {reply, State#state.connection, State};

handle_call({register, Uri, Options, Handler0}, _From, State) ->
    try
        Handler1 = validate_handler(Handler0),
        case do_register(Uri, Options, Handler1, State) of
            {ok, RegId, NewState} ->
                {reply, {ok, RegId}, NewState};
            {error, Reason, NewState} ->
                {reply, {error, Reason}, NewState}
        end
    catch
        throw:invalid_handler ->
            {error, invalid_handler, State}
    end;


handle_call({unregister, Uri}, _From, State) ->
    case do_unregister(Uri, State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, Reason, NewState} ->
            {reply, {error, Reason}, NewState}
    end;

handle_call({subscribe, Uri, Options, Handler0}, _From, State) ->
    try
        Handler1 = validate_handler(Handler0),
        case do_subscribe(Uri, Options, Handler1, State) of
            {ok, RegId, NewState} ->
                {reply, {ok, RegId}, NewState};
            {error, _, NewState} = Error ->
                {reply, Error, NewState}
        end
    catch
        throw:invalid_handler ->
            {error, invalid_handler, State}
    end;

handle_call({unsubscribe, Uri}, _From, State) ->
    case do_unsubscribe(Uri, State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, Reason, NewState} ->
            {reply, {error, Reason}, NewState}
    end;

handle_call(_, _From, State) ->
    {reply, {error, unknown_call}, State}.


handle_cast(_, State) ->
    {noreply, State}.


handle_info({awre, {invocation, _, _, _, _, _} = Invocation},  State) ->
    %% TODO use a pool or resource limiter
    %% invocation of the rpc handler
    spawn(fun() -> handle_invocation(Invocation, State) end), % TODO: handle load regulation?
    {noreply, State};

handle_info({awre, {event, _, _, _, _, _} = Publication}, State) ->
    %% TODO use a pool or resource limiter
    %% invocation of the sub handler
    spawn(fun() -> handle_event(Publication, State) end),
    % TODO: handle load regulation?
    {noreply, State};

handle_info({'EXIT', Pid, Reason}, #state{connection = Pid} = State0)->
    ?LOG_ERROR(#{
        message => "WAMP connection down",
        reason => Reason
    }),
    State1 = State0#state{
        registration_state = #{},
        subscription_state = #{},
        connection = undefined
    },
    {ok, State2} = maybe_reconnect(State1),
    {noreply, State2};

handle_info(Msg, State) ->
    ?LOG_WARNING(#{
        message => "Received info message",
        reason => Msg
    }),
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




%% =============================================================================
%% PRIVATE
%% =============================================================================



handle_invocation({invocation, ReqId, RegId, Details, Args, KWArgs}, State) ->
    Conn = State#state.connection,
    #{RegId := #{handler := Handler}} = State#state.registration_state,

    ?LOG_DEBUG(#{
        message => "Handling invocation",
        request_id => ReqId,
        registration_id => RegId,
        handler => Handler,
        args => Args,
        kwargs => KWArgs
    }),

    try

        case apply_callback(Handler, Args, KWArgs, Details) of
            {ok, RArgs, RKWArgs, RDetails}
            when is_list(RArgs), is_map(RKWArgs), is_map(RDetails) ->
                ok = awre:yield(Conn, ReqId, RDetails, RArgs, RKWArgs);
            {error, RUri, RArgs, RKWArgs, RDetails}
            when is_binary(RUri), is_list(RArgs), is_map(RKWArgs),
                is_map(RDetails) ->
                %% AWRE drops EARgs and EDEtails!!!
                ok = awre:error(Conn, ReqId, RKWArgs, RUri)
        end

    catch
        throw:{badarity, N, Arities}  ->
            EKWArgs = #{
                code => badarity,
                message => <<"The call was made passing the wrong number of positional arguments.">>,
                description => iolist_to_binary(
                    io_lib:format(
                        <<
                            "The call was made passing ~b positional arguments, when the procedure expects ~s arguments."
                        >>,
                        [N, format_arity('or', Arities)]
                    )
                )
            },
            EUri = <<"wamp.error.invalid_argument">>,
            awre:error(Conn, ReqId, EKWArgs, EUri);

        Class:Reason:Stacktrace ->
            ?LOG_ERROR(#{
                message => "Error while handling WAMP invocation",
                request_id => ReqId,
                registration_id => RegId,
                handler => Handler,
                args => Args,
                kwargs => KWArgs,
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            EKWArgs = #{
                code => internal_error,
                message => <<"Internal error.">>,
                description => <<"There was an internal error, please contact the administrator.">>
            },
            EUri = <<"com.magenta.error.internal_error">>,
            awre:error(Conn, ReqId, EKWArgs, EUri)
    end.


%% @private
handle_event({event, SubscriptionId, PubId, Details, Args, KWArgs}, State) ->
    Callbacks = State#state.subscription_state,
    Conn = State#state.connection,

    #{SubscriptionId := #{handler := Handler}} = Callbacks,

    ?LOG_DEBUG(#{
        mesage => "Handling event",
        subscription_id => SubscriptionId,
        publication_id => PubId,
        details => Details,
        handler => Handler,
        args => Args,
        kwargs => KWArgs
    }),

    try

        apply_callback(Handler, Args, KWArgs, Details)

    catch
        throw:{badarity, N, Arities}  ->
            RKWArgs = #{
                code => badarity,
                message => <<"The event contains the wrong number of positional arguments.">>,
                description => iolist_to_binary(
                    io_lib:format(
                        <<
                            "The event contains ~b arguments, when the subcription expects ~s arguments"
                        >>,
                        [N, format_arity('or', Arities)]
                    )
                )
            },
            RUri = <<"wamp.error.invalid_argument">>,
            awre:error(Conn, PubId, RKWArgs, RUri);
        Class:Reason:Stacktrace ->
            %% @TODO review error handling and URIs
            ?LOG_DEBUG(#{
                mesage => "Error while handling event",
                subscription_id => SubscriptionId,
                publication_id => PubId,
                details => Details,
                handler => Handler,
                args => Args,
                kwargs => KWArgs,
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            RKWArgs = #{
                code => internal_error,
                message => <<"Internal error.">>,
                description => <<"There was an internal error, please contact the administrator.">>
            },
            RUri = <<"com.magenta.error.internal_error">>,
            awre:error(Conn, PubId, RKWArgs, RUri)
    end.





apply_callback({Mod, Fun, Arities}, Args, KWArgs, Details) ->
    Arity = length(Args),
    lists:member(Arity, Arities)
        orelse throw({badarity, Arity, Arities}),

    HandlerArgs = to_handler_args(Args, KWArgs, Details),
    apply(Mod, Fun, HandlerArgs);

apply_callback({Fun, Arities}, Args, KWArgs, Details) when is_function(Fun) ->
    Arity = length(Args),
    lists:member(Arity, Arities)
        orelse throw({badarity, Arity, Arities}),

    HandlerArgs = to_handler_args(Args, KWArgs, Details),
    apply(Fun, HandlerArgs).


%% @private
register_all(#state{} = State) ->
    Regs = State#state.registrations,

    maps:fold(
        fun(Uri, #{options := Opts, handler := Handler}, Acc) ->
            case do_register(Uri, Opts, Handler, Acc) of
                {ok, _, NewState} ->
                    NewState;
                {error, Reason, _NewState} ->
                    ?LOG_INFO(#{
                        message => "Error while registering procedure",
                        procedure_uri => Uri,
                        options => Opts
                    }),
                    error({init_failure, Reason})
            end
        end,
        State,
        Regs
    ).


%% @private
subscribe_all(State) ->
    Regs = State#state.subscriptions,

    maps:fold(
        fun(Uri, #{options := Opts, handler := Handler}, Acc) ->
            case do_subscribe(Uri, Opts, Handler, Acc) of
                {ok, _, NewState} ->
                    NewState;
                {error, Reason} ->
                    ?LOG_INFO(#{
                        message => "Error while subscribing",
                        topic_uri => Uri,
                        options => Opts
                    }),
                    error({init_failure, Reason})
            end
        end,
        State,
        Regs
    ).


%% @private
do_register(Uri, Opts, Handler, #state{} = State) ->
    Conn = State#state.connection,
    RegState0 = State#state.registration_state,

    case maps:find(Uri, RegState0) of
        {ok, RegId} ->
            {error, {already_registered, RegId}, State};
        error ->
            case awre:register(Conn, maps:to_list(Opts), Uri) of
                {ok, RegId} ->
                    ?LOG_INFO(#{
                        message => "Successfully registered procedure",
                        procedure_uri => Uri,
                        handler => Handler,
                        options => Opts
                    }),


                    RegState1 = maps:put(Uri, RegId, RegState0),
                    Callback = #{uri => Uri, handler => Handler},
                    RegState2 = maps:put(RegId, Callback, RegState1),
                    NewState = State#state{registration_state = RegState2},

                    {ok, RegId, NewState};

                {error, Reason} ->
                    {error, Reason, State}
            end
    end.


%% @private
do_unregister(Uri, #state{} = State) when is_binary(Uri) ->
    case maps:find(Uri, State#state.registration_state) of
        {ok, Id} ->
            do_unregister(Id, State);
        error ->
            {error, {unknown_registration, Uri}, State}
    end;

do_unregister(Id, #state{} = State) when is_integer(Id) ->
    Conn = State#state.connection,
    SubsState0 = State#state.registration_state,
    case maps:take(Id, SubsState0) of
        {#{uri := Uri}, SubsState1} ->
            ok = awre:unregister(Conn, Id),
            SubsState = maps:remove(Uri, SubsState1),
            {ok, State#state{registration_state = SubsState}};
        error ->
            {error, {unknown_registration, Id}, State}
    end.


%% @private
do_subscribe(Uri, Opts, Handler, #state{} = State) ->
    Conn = State#state.connection,

    case awre:subscribe(Conn, maps:to_list(Opts), Uri) of
        {ok, SubsId} ->
            ?LOG_INFO(#{
                message => "Successfully subscribed",
                topic_uri => Uri,
                handler => Handler,
                options => Opts
            }),

            RegState0 = State#state.subscription_state,
            RegState1 = maps:put(Uri, SubsId, RegState0),
            Callback = #{uri => Uri, handler => Handler},
            RegState2 = maps:put(SubsId, Callback, RegState1),
            NewState = State#state{subscription_state = RegState2},

            {ok, SubsId, NewState};

        {error, Reason} ->
            {error, Reason, State}
    end.


%% @private
do_unsubscribe(Uri, #state{} = State) when is_binary(Uri) ->
    case maps:find(Uri, State#state.subscription_state) of
        {ok, Id} ->
            do_unsubscribe(Id, State);
        error ->
            {error, {unknown_subscription, Uri}, State}
    end;

do_unsubscribe(Id, #state{} = State) ->
    Conn = State#state.connection,
    SubsState0 = State#state.subscription_state,
    case maps:take(Id, SubsState0) of
        {#{uri := Uri}, SubsState1} ->
            ok = awre:unsubscribe(Conn, Id),
            SubsState = maps:remove(Uri, SubsState1),
            {ok, State#state{subscription_state = SubsState}};
        error ->
            {error, {unknown_subscription, Id}, State}
    end.


%% @private
connect(#state{router = Router} = State0) ->
    #{
        hostname := Host,
        port := Port,
        realm := Realm,
        encoding := Encoding
    } = Router,

    {ok, Conn} = awre:start_client(),
    link(Conn),

    try

        {ok, SessionId, Details} = awre:connect(
            Conn, Host, Port, Realm, Encoding
        ),
        State1 = State0#state{
            connection = Conn,
            session_id = SessionId,
            router_details = Details
        },
        State2 = on_connect(State1),
        {ok, State2}

    catch
        Class:Reason:Stacktrace ->
            ?LOG_ERROR(#{
                message => "Failed to connect to WAMP Router",
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {error, Reason}
    end.



%% @private
on_connect(State0) ->
    ?LOG_ERROR("Connected ~p", [State0#state.connection]),
    State1 = register_all(State0),
    subscribe_all(State1).


%% @private
maybe_reconnect(#state{backoff = undefined}) ->
    ?LOG_ERROR(#{
        message => "Failed to connect to WAMP Router",
        reconnection_enabled => false
    }),
    exit(wamp_connection_error);

maybe_reconnect(#state{max_retries = N, retry_count = M}) when N < M ->
    ?LOG_ERROR(#{
        message => "Failed to connect to WAMP Router after max retries reached",
        reconnection_enabled => true,
        max_retries => N
    }),
    exit(wamp_connection_error);

maybe_reconnect(#state{backoff = B0, retry_count = N} = State0) ->
    case connect(State0) of
        {ok, State1} ->
            {_, B1} = backoff:succeed(B0),
            State2 = State1#state{backoff = B1},
            {ok, State2};

        {error, _} ->
            {Time, B1} = backoff:fail(B0),

            ?LOG_INFO(#{
                message => "Failed to connect to WAMP Router, will retry",
                reconnection_enabled => true,
                max_retries => State0#state.max_retries,
                retry_count => N,
                backoff_time => Time
            }),

            ok = timer:sleep(Time),

            State1 = State0#state{backoff = B1, retry_count = N + 1},
            maybe_reconnect(State1)
    end.


%% @private
to_handler_args(Args, KWArgs, Details)
when is_list(Args) andalso is_map(KWArgs) andalso is_map(Details) ->
    Args ++ [KWArgs, Details];

to_handler_args(undefined, KWArgs, Details) ->
    to_handler_args([], KWArgs, Details);

to_handler_args(Args, undefined, Details) ->
    to_handler_args(Args, #{}, Details);

to_handler_args(Args, KWArgs, undefined) ->
    to_handler_args(Args, KWArgs, #{});

to_handler_args(Arg, KWArgs, Details) when not is_list(Arg) ->
    to_handler_args([Arg], KWArgs, Details).


get_router(#{router := RouterName}) ->
    wamp_service_config:get([routers, RouterName]);

get_router(_) ->
    Default = wamp_service_config:get([defaults, router]),
    wamp_service_config:get([routers, Default]).


init_backoff(Router) ->
    case maps:get(reconnect, Router, false) of
        true ->
            Min = maps:get(
                reconnect_backoff_min, Router, 10
            ),
            Max = maps:get(
                reconnect_backoff_max, Router, 120000
            ),
            Type = maps:get(reconnect_backoff_type, Router, jitter),
            backoff:type(backoff:init(Min, Max), Type);
        false ->
            undefined
    end.


process_registrations(#{roles := #{callee := #{registrations := Map}}}) ->
    DefaultOpts = wamp_service_config:get(
        [defaults, callee], ?DEFAULT_REGISTER_OPTS
    ),
    process_callback_handlers(Map, DefaultOpts);

process_registrations(_) ->
    #{}.


process_subscriptions(#{roles := #{subscriber := #{subscriptions := Map}}}) ->
    DefaultOpts = wamp_service_config:get(
        [defaults, subscriber], ?DEFAULT_SUBSCRIBE_OPTS
    ),
    process_callback_handlers(Map, DefaultOpts);

process_subscriptions(_) ->
    #{}.


process_callback_handlers(Map, DefaultOpts) ->
    maps:map(
        fun
            (K, _) when not is_binary(K) ->
                error({invalid_uri, K});

            (_, #{handler := Handler0} = R0) ->
                Handler1 = validate_handler(Handler0),
                R1 = maps:put(handler, Handler1, R0),
                case maps:find(options, R1) of
                    {ok, Opts} ->
                        maps:put(options, maps:merge(DefaultOpts, Opts), R1);
                    error ->
                        maps:put(options, DefaultOpts, R1)
                end;

            (K, _) ->
                error({missing_handler, K})
        end,
        Map
    ).


validate_handler({M, F} = Handler) when is_atom(M) andalso is_atom(F) ->
    Exports = sofs:to_external(
        sofs:relation_to_family(
            sofs:restriction(
                sofs:relation(
                    M:module_info(exports)),
                    sofs:set([F]
                )
            )
        )
    ),
    case Exports of
        [] ->
            ?LOG_ERROR(#{
                message => "Invalid handler",
                handler => Handler
            }),
            throw(invalid_handler);

        [{F, Arities0}] ->
            %% All wamp handlers should have at least 2 args
            %% (KWArgs and Details)
            Arities = lists:filter(fun(X) -> X >= 2 end, Arities0),
            length(Arities) > 0 orelse throw(invalid_handler),
            {M, F, [X - 2 || X <- Arities]}
    end;

validate_handler(Fun) when is_function(Fun) ->
    {arity, N} = erlang:fun_info(Fun, arity),
    %% All wamp handlers should have at least 2 args
    %% (KWArgs and Details)
    N >= 2 orelse throw(invalid_handler),
    {Fun, [N - 2]};

validate_handler(Handler) ->
    ?LOG_ERROR("Invalid handler ~p", [Handler]),
    throw(invalid_handler).


pick_worker(Peername, Term) ->
    case gproc:get_value({p,l,{gproc_pool, default}}, shared) of
        {_, Type} when Type == round_robin; Type == random ->
            do_pick_worker(Peername);
        {_, Type} when Type == hash
            orelse (Type == direct andalso is_integer(Term)) ->
            do_pick_worker(Peername, Term);
        {_, Type} when Type == hash ->
            do_pick_worker(Peername, Term);
        {_, _} ->
            error({badarg, {Peername, Term}})
    end.



do_pick_worker(Peername) ->
    case gproc_pool:pick(Peername) of
        {n, l, [gproc_pool, Peername, _, _]} = Id ->
            log_and_return(Id);
        false ->
            undefined
    end.

do_pick_worker(Peername, Term) ->
    case gproc_pool:pick(Peername, Term) of
        {n, l, [gproc_pool, Peername, _, _]} = Id ->
            log_and_return(Id);
        false ->
            undefined
    end.


log_and_return({n, l, [gproc_pool, Peername, _, Name]} = Id) ->
    Pid = gproc_pool:whereis_worker(Peername, Name),
    _ = gproc_pool:log(Id),
    Pid.


format_arity(_, [N]) ->
    io_lib:format("exactly ~B", [N]);

format_arity('or', L0) ->
    {L1, [N]} = lists:split(length(L0) - 1, L0),
    Str = string:join([io_lib:format("~B", [X]) || X <- L1], [$,]),
    io_lib:format("either ~s or ~B", [Str, N]).

