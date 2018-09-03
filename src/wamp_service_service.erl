%% =============================================================================
%% Copyright (C) NGINEO LIMITED 2011 - 2016. All rights reserved.
%% =============================================================================
-module(wamp_service_service).

-behaviour(gen_server).

-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


start_link(Opts) ->
    gen_server:start_link({local, wamp_caller} ,?MODULE, Opts, []).

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init(Opts) ->
    Host = proplists:get_value(hostname, Opts),
    Port = proplists:get_value(port, Opts),
    Realm = proplists:get_value(realm, Opts),
    Encoding = proplists:get_value(encoding, Opts),
    {ok, Conn} = awre:start_client(),
    link(Conn),
    {ok, SessionId, _RouterDetails} = awre:connect(Conn, Host, Port, Realm, Encoding),
    State1 = #{conn => Conn, session => SessionId},
    {ok, State1}.

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
handle_call(Msg= {call, _, _, _, _}, From, State) ->
    _ = do_call(Msg, From, State),
    {noreply, State};
handle_call(Msg = {publish, _, _, _}, _From, State) ->
    ok = do_publish(Msg, State),
    {reply, ok, State}.
%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(_Msg, State) ->
    {stop, error, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, #{conn := Conn} ) ->
    awre:stop_client(Conn),
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

do_call({call, Uri, Args, Opts, Timeout}, From, #{conn := Conn} = State) ->
    spawn(fun() ->
            Opts1 = set_trace_id(Opts),
            try
                Res = awre:call(Conn, [], Uri, Args, Opts1, Timeout),
                gen_server:reply(From, Res)
            catch
                Class:Reason ->
                    handle_call_error(Class, Reason, Uri, Args, Opts1)
            end
          end).


do_publish({publish, Topic, Args, Opts}, #{conn := Conn}) ->
    Opts1 = set_trace_id(Opts),
    spawn(fun() ->
            awre:publish(Conn, [], Topic, Args, Opts1)
          end).


handle_call_error(Class, Reason, Uri, Args, Opts) ->
    _ = lager:error("handle call class=~p, reason=~p, uri=~p,  args=~p, args_kw=~p, stacktrace=~p",
                    [Class, Reason, Uri, Args, Opts, erlang:get_stacktrace()]),
    case {Class, Reason} of
        {exit, {timeout, _}} ->
            Details = #{code => timeout, message => _(<<"Service timeout.">>),
                        description => _(<<"There was a timeout resolving the operation.">>)},
            {error, #{}, <<"com.magenta.error.timeout">>, #{}, Details};
        {error, #{code := _} = Error} ->
            Error;
        {_, _} ->
            Details = #{code => internal_error, message => _(<<"Internal error.">>),
                        description => _(<<"There was an internal error, please contact the administrator.">>)},
            {error, #{}, <<"com.magenta.error.internal">>, #{}, Details}
    end.


-spec trace_id(map()) -> binary().
trace_id(Opts) ->
    maps:get(<<"trace_id">>, Opts, undefined).

-spec set_trace_id(map()) -> map().
set_trace_id(Opts) ->
    case trace_id(Opts) of
        undefined ->
            TraceId = wamp_service_trace_id:generate(),
            maps:put(<<"trace_id">>, TraceId, Opts);
        _ ->
            Opts
    end.
