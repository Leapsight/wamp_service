%%% File        : worker_test.erl
%%% Author      : Federico Repond
%%% Description : 
%%% Created     : 29 Apr 2017 by Federico Repond
-module(wamp_service_handler).

-behaviour(gen_server).

-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	terminate/2, code_change/3]).

-define(SERVER, ?MODULE).


start_link(Opts) ->
	gen_server:start_link(?MODULE, [Opts], []).


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
init([Opts]) ->
	%% init worker pool
	PoolName = proplists:get_value(pool_name, Opts),
	Capacity = proplists:get_value(pool_capacity, Opts),
	Size = proplists:get_value(pool_size, Opts),
	sidejob:new_sharded_resource(PoolName, wamp_service_worker, Capacity, Size),
	%% connect to wamp broker
	Host = proplists:get_value(hostname, Opts),
	Port = proplists:get_value(port, Opts),
	Realm = proplists:get_value(realm, Opts),
	Encoding = proplists:get_value(encoding, Opts),
	{ok, Con} = awre:start_client(), 
	{ok, SessionId, _RouterDetails} = awre:connect(Con, Host, Port, Realm, Encoding),
	lager:info("done (~p).", [SessionId]),
	%% and register procedure
	Callbacks = register_callbacks(Con, Opts),
	{ok, #{con => Con, session => SessionId, callbacks => Callbacks, pool_name => PoolName}}.


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
handle_call(_, _, State) ->
	{noreply,State}.


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
handle_info({awre, {invocation, RequestId, RegistrationId, _Details, _Args, _ArgumentsKw} = Invocation}, 
			#{callbacks := Callbacks, con := Con, pool_name := PoolName} = State) ->
	lager:debug("Been called ~p ... will just handle it ...", [Invocation]),
	%% invocation of the rpc handler
	#{RegistrationId := #{uri := Uri}} = Callbacks,
	Res = sidejob:cast({PoolName, RequestId}, {Invocation, State}),
	case Res of
		overload ->
			lager:error("Service overload <~p>.", [Uri]),
			awre:error(Con, RequestId, "overload", "worker pool exhausted", <<"com.leapsight.iota.error.overload">>),
			{noreply, State};
		_ ->
			{noreply, State}
	end;
handle_info({awre, {event, SubscriptionId, PublicationId, _Details, _Args, _ArgumentsKw} = Publication}, 
			#{callbacks := Callbacks, con := Con, pool_name := PoolName} = State) ->
	lager:debug("Publication ~p ... will just handle it ...", [Publication]),
	%% invocation of the sub handler
	#{SubscriptionId := #{uri := Uri}} = Callbacks,
	Res = sidejob:cast({PoolName, SubscriptionId}, {Publication, State}),
	case Res of
		overload ->
			lager:error("Service overload <~p>.", [Uri]),
			awre:error(Con, PublicationId, "overload", "worker pool exhausted", <<"com.leapsight.iota.error.overload">>),
			{noreply, State};
		_ ->
			{noreply, State}
	end.


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


%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
register_callbacks(Con, Opts) ->
	Callbacks = proplists:get_value(callbacks, Opts),
	lists:foldl(fun ({Type, Uri, MF}, Acc) -> 
		lager:info("register ~p ... ", [Uri]),
		case Type of
			callee ->
				{ok, RegistrationId} = awre:register(Con, [{invoke, roundrobin}], Uri),
				lager:info("registered (~p).", [RegistrationId]),
				Acc#{RegistrationId => #{uri => Uri, handler => MF}};
			subscriber ->
				{ok, SubscriptionId} = awre:subscribe(Con, [], Uri),
				lager:info("registered (~p).", [SubscriptionId]),
				Acc#{SubscriptionId => #{uri => Uri, handler => MF}}
		end
	end, #{}, Callbacks).