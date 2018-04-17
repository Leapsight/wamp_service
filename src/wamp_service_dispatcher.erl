% =============================================================================
%% Copyright (C) NGINEO LIMITED 2011 - 2016. All rights reserved.
%% =============================================================================
-module(wamp_service_dispatcher).

-behaviour(gen_server).

-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init(Opts) ->
    process_flag(trap_exit, true),
    Host = proplists:get_value(hostname, Opts),
    Port = proplists:get_value(port, Opts),
    Realm = proplists:get_value(realm, Opts),
    Encoding = proplists:get_value(encoding, Opts),
    Retries = proplists:get_value(retries, Opts, 10),
    Backoff = proplists:get_value(backoff, Opts, 100),
    CbConf = normalize_cb_conf(proplists:get_value(callbacks, Opts, #{})),
    State = #{
      host => Host,
      port => Port,
      realm => Realm,
      encoding => Encoding,
      retries => Retries,
      backoff => Backoff,
      cb_conf => CbConf,
      callbacks => #{},
      inverted_ref => #{}
     },
    case wamp_service_utils:connect(Host, Port, Realm, Encoding) of
        {ok, {Conn, SessionId}} ->
            State1 = State#{conn => Conn, session => SessionId},
            State2 = register_callbacks(State1),
            {ok, State2};
        error ->
            exit(wamp_connection_error)
    end.


%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) ->
%%                {reply, Reply, State} |
%%                {reply, Reply, State, Timeout} |
%%                {noreply, State} |
%%                {noreply, State, Timeout} |
%%                {stop, Reason, Reply, State} |
%%                {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({register, Uri, Callback}, _From, State = #{cb_conf := CbConf}) ->
    try
        case maps:get(Uri, CbConf, undefined) of
            undefined ->
                State1 = add_callback(Uri, Callback, State),
                {reply, ok, State1};
            _ ->
                {reply, {error, "URI already registered, unregister it first"}, State}
        end
    catch
        Class:_ ->
            {reply, {error, Class}, State}
    end;
handle_call({unregister, Uri}, _From, State) ->
    try
        State1 = remove_callback(Uri, State),
        {reply, ok, State1}
    catch
        Class:_ ->
            {reply, {error, Class}, State}
    end;
handle_call(status, _From, State) ->
    try
        {reply, State, State}
    catch
        Class:_ ->
            {reply, {error, Class}, State}
    end.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({awre, {invocation, _, _, _, _, _} = Invocation},  State) ->
    %% invocation of the rpc handler
    spawn(fun() -> handle_invocation(Invocation, State) end), %% TODO: handle load regulation?
    {noreply, State};
handle_info({awre, {event, _, _, _, _, _} = Publication}, State) ->
    %% invocation of the sub handler
    spawn(fun() -> handle_event(Publication, State) end), %% TODO: handle load regulation?
    {noreply, State};
handle_info(Msg, State) ->
    lager:error("Service down messsage=~p", [Msg]),
    #{host := Host, port:= Port, realm := Realm,
      encoding := Encoding, retries := Retries, backoff := Backoff} = State,
    {ok, {Conn, SessionId}} = wamp_service_utils:reconnect(Host, Port, Realm, Encoding, Backoff, Retries, 0),
    State1 = State#{conn => Conn, session => SessionId},
    State2 = register_callbacks(State1),
    {noreply, State2}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.


%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% =============================================================================
%% PRIVATE
%% =============================================================================
handle_invocation({invocation, RequestId, RegistrationId, Details, Args, ArgsKw},
                  #{conn := Conn, callbacks := Callbacks}) ->
    #{RegistrationId := #{handler := Handler, scopes := Scopes}} = Callbacks,
    try
        _ = lager:debug("handle invocation request_id=~p registration_id=~p handler=~p args=~p args_kw=~p, scope=~p",
                        [RequestId, RegistrationId, Handler, Args, ArgsKw, Scopes]),
        handle_security(ArgsKw, Scopes),
        set_locale(ArgsKw),
        Res = exec_callback(Handler, wamp_service_utils:args(Args) ++ [wamp_service_utils:options(ArgsKw)]),
        handle_result(Conn, RequestId, Details, Res, ArgsKw)
    catch
        throw:not_found -> % do not log not found errors
            handle_invocation_error(Conn, RequestId, Handler, throw, not_found);
        Class:Reason ->
            Args1 = obfuscate_pass(Args),
            lager:error("handle invocation class=~p reason=~p call handler=~p args=~p args_kw=~p stacktrace=~p",
                        [Class, Reason, Handler, Args1, ArgsKw, erlang:get_stacktrace()]),
            handle_invocation_error(Conn, RequestId, Handler, Class, Reason)
    end.

%% @private
handle_event({event, SubscriptionId, PublicationId, _Details, Args, ArgsKw},
             #{callbacks := Callbacks}) ->
    #{SubscriptionId := #{handler := Handler}} = Callbacks,
    try
        _ = lager:debug("handle event subscription_id=~p publication_id=~p handler=~p args=~p args_kw=~p",
                        [SubscriptionId, PublicationId, Handler, Args, ArgsKw]),
        exec_callback(Handler, wamp_service_utils:args(Args) ++ [wamp_service_utils:options(ArgsKw)])
    catch
        %% @TODO review error handling and URIs
        Class:Reason ->
            _ = lager:error("Error ~p:~p subscription handler=~p args=~p args_kw=~p stacktrace=~p",
                            [Class, Reason, Handler, Args, ArgsKw, erlang:get_stacktrace()])
    end.

%% @private
handle_result(Conn, RequestId, Details, Res, ArgsKw) ->
    case Res of
        undefined ->
            ok = awre:yield(Conn, RequestId, Details, [], ArgsKw);
        notfound ->
            throw(not_found);
        {error, _, _} = Error ->
            throw(Error);
        _ ->
            ok = awre:yield(Conn, RequestId, Details, [Res], ArgsKw)
    end.

%% @private
handle_invocation_error(Conn, RequestId, Handler, Class, Reason) ->
    case {Class, Reason} of
        %% @TODO review error handling and URIs
        {throw, unauthorized} ->
            Error = #{code => unauthorized, message => _(<<"Unauthorized user.">>),
                      description => _(<<"The user does not have the required permissions to access the resource.">>)},
            awre:error(Conn, RequestId, Error, <<"wamp.error.unauthorized">>);
        {throw, not_found} ->
            Error = #{code => not_found, message => _(<<"Resource not found.">>),
                      description => _(<<"The resource you are trying to retrieve does not exist.">>)},
            awre:error(Conn, RequestId, Error, <<"com.magenta.error.not_found">>);
        {_, {error, Key, Error}} ->
            awre:error(Conn, RequestId, Error, Key);
        {error, #{code := authorization_error} = Error} ->
            awre:error(Conn, RequestId, Error, <<"wamp.error.not_authorized">>);
        {error, #{code := service_error} = Error} ->
            awre:error(Conn, RequestId, Error, <<"com.magenta.error.internal_error">>);
        {error, #{code := _} = Error} ->
            awre:error(Conn, RequestId, Error, <<"wamp.error.invalid_argument">>);
        {Class, Reason} ->
            _ = lager:error("handle invocation error: handler=~p, class=~p, reason=~p, stack=~p",
                            [Handler, Class, Reason, erlang:get_stacktrace()]),
            Error = #{code => unknown_error, message => _(<<"Unknown error.">>),
                      description => _(<<"There was an unknown error, please contact the administrator.">>)},
            awre:error(Conn, RequestId, Error, <<"com.magenta.error.unknown_error">>)
    end.

exec_callback({Mod, Fun}, Args) ->
    apply(Mod, Fun, Args);
exec_callback(Fun, Args) when is_function(Fun) ->
    apply(Fun, Args).

handle_security(_, []) ->
    true;
handle_security(#{<<"security">> := #{<<"groups">> := Groups}}, ProcScope) ->
    Any = lists:any(fun(S) -> lists:member(S, ProcScope) end, Groups),
    Any orelse throw(unauthorized);
handle_security(_, _) ->
    throw(unauthorized).

register_callbacks(State = #{cb_conf := CbConf}) ->
    maps:fold(fun (Uri, Cb, St) ->
                      add_callback(Uri, Cb, St)
              end, State#{cb_conf => #{}, callbacks => #{} ,inverted_ref => #{}}, CbConf).

normalize_cb_conf(CbConf = #{}) ->
    CbConf;
normalize_cb_conf(CbConf) when is_list(CbConf) ->
    lists:foldl(fun
                    ({subscription, Uri, MF}, Acc)  ->
                       Acc#{Uri => {subscription, MF}};
                    ({procedure, Uri, MF}, Acc)  ->
                       Acc#{Uri => {procedure, MF, []}};
                    ({procedure, Uri, MF, Scopes}, Acc) ->
                       Acc#{Uri => {procedure, MF, Scopes}}
               end,
                #{}, CbConf).

add_callback(Uri, Callback = {procedure, Fun, Scopes},
             State = #{cb_conf := CbConf, callbacks := Callbacks,
                       inverted_ref := InvertedRef, conn := Conn}) ->
    _ = lager:info("registering procedure uri=~p ... ", [Uri]),
    ok = validate_handler(Fun),
    {ok, RegistrationId} = awre:register(Conn, [{invoke, roundrobin}], Uri),
    _ = lager:info("registered reg_id=~p.", [RegistrationId]),
    State#{
      cb_conf => CbConf#{Uri => Callback},
      callbacks => Callbacks#{RegistrationId => #{uri => Uri, handler => Fun, scopes => Scopes}},
      inverted_ref => InvertedRef#{Uri => RegistrationId}
     };
add_callback(Uri, Callback = {subscription,  Fun},
             State = #{cb_conf := CbConf, callbacks := Callbacks,
                       inverted_ref := InvertedRef, conn := Conn}) ->
    _ = lager:info("registering subscription uri=~p ... ", [Uri]),
    ok = validate_handler(Fun),
    {ok, SubscriptionId} = awre:subscribe(Conn, [], Uri),
    _ = lager:info("registered subs_id=~p.", [SubscriptionId]),
    State#{
      cb_conf => CbConf#{Uri => Callback},
      callbacks => Callbacks#{SubscriptionId => #{uri => Uri, handler => Fun}},
      inverted_ref => InvertedRef#{Uri => SubscriptionId}
     }.

remove_callback(Uri, State = #{inverted_ref := InvertedRef}) ->
    _ = lager:debug("deregistering procedure uri=~p ... ", [Uri]),
    case maps:get(Uri, InvertedRef, undefined) of
        undefined ->
            _ = lager:warning("Attemping to remove invalid registration uri=~p", [Uri]),
            State;
        _ ->
            #{inverted_ref := InvertedRef = #{Uri := Id}, callbacks := Callbacks,
              cb_conf := CbConf = #{Uri := Conf}, conn := Conn} = State,
            do_remove_callback(Conn, Id, Conf),
            State#{inverted_ref => maps:remove(Uri, InvertedRef),
                   callbacks => maps:remove(Id, Callbacks),
                   cb_conf => maps:remove(Uri, CbConf)}
    end.

do_remove_callback(Conn, Id, {procedure, _, _}) ->
    _ = lager:debug("deregistering procedure id=~p ... ", [Id]),
    awre:unregister(Conn, Id);
do_remove_callback(Conn, Id, {subscription, _}) ->
    _ = lager:debug("deregistering subscription id=~p ... ", [Id]),
    awre:unsubscribe(Conn, Id).

validate_handler(Fun) when is_function(Fun) ->
    ok;
validate_handler(Handler = {M, F}) ->
    Exports = M:module_info(exports),
    case lists:keyfind(F, 1, Exports) of
        false ->
            _ = lager:error("Invalid handler ~p", [Handler]),
            error(invalid_handler, "The handler you're trying to register does not exist.");
        _ ->
            ok
    end;
validate_handler(Handler) ->
    _ = lager:error("Invalid handler ~p", [Handler]),
    error(invalid_handler, <<"The handler you're trying to register is invalid",
                             "(should be either Fun | {Mod, FunName}).">>).

set_locale(ArgsKw) ->
    Sec = maps:get(<<"security">>, wamp_service_utils:options(ArgsKw), #{}),
    Locale = maps:get(<<"locale">>, Sec, <<"es_AR">>),
    Locale1 = re:replace(Locale, <<"-">>, <<"_">>,  [{return, binary}]),
    erlang:put(locale, Locale1).

obfuscate_pass(Args) ->
    lists:map(fun(Arg) when is_map(Arg) ->
                      maps:without([<<"password">>, <<"old_password">>, <<"new_password">>], Arg);
                 (Arg) ->
                      Arg
              end, Args).
