%%%-------------------------------------------------------------------
%% @doc wamp public API
%% @end
%%%-------------------------------------------------------------------

-module(wamp_service_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    Res = wamp_service_sup:start_link(),
    {ok, DispatcherSpec} = application:get_env(wamp_service, callee_dispatcher),
    {ok, ServiceSpec} = application:get_env(wamp_service, caller_service),
    {workers, DWorkers} = lists:keyfind(workers, 1, DispatcherSpec),
    {workers, SWorkers} = lists:keyfind(workers, 1, ServiceSpec),
    wpool:start_sup_pool(callee_dispatcher, [{workers, DWorkers}, {worker, {wamp_service_dispatcher, DispatcherSpec}}]),
    wpool:start_sup_pool(caller_service, [{workers, SWorkers}, {worker, {wamp_service_service, ServiceSpec}}]),
    register_services(),
    Res.

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
register_services() ->
    {Service, Host} = service_and_host(),
    Service = service_name(Service),
    Ping = <<"com.magenta.", Host/binary, ".ping">>,
    Ping2 = <<"com.magenta.", Service/binary, ".ping">>,
    LogLevel = <<"com.magenta.", Host/binary, ".log_level">>,
    wamp_service:register(procedure, Ping, fun wamp_service_instr:ping/1, [<<"admin">>]),
    wamp_service:register(procedure, LogLevel, fun wamp_service_instr:log_level/2, [<<"admin">>]).

service_and_host() ->
    [Service, Host] = binary:split(atom_to_binary(node(), utf8), [<<"@">>]),
    {Service, Host}.

service_name(Service) ->
    [WampService, _] = binary:split(Service, [<<"_">>]),
    WampService.