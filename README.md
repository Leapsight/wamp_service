# wamp_service

__TODO: still needs generalization and refactoring especially opts.__

A boilerplate WAMP micro service infrastructure for developing basic micro service with WAMP support. This micro service registers to procedures: `com.example.add2` and `com.leapsight.echo`. The first one is intended to be used with crossbar example application.

This allows register procedures and subscriptions in a declarative way and abstract actual wamp complexity from service implementation. If more complex feature of the WAMP protocol is needed it should be handled by the service.

## Configuration
The micro service has several configurations in `sys.config`:

```erlang
 %% service conf
 {wamp_service,
  [
   {callee_dispatcher,
    [
     %% wamp opts
     {hostname, "localhost"},
     {port, 18082},
     {realm, <<"com.magenta.test">>},
     {encoding, json},
     {reconnect, true},
     %% service callbacks
     {callbacks,
      [
       {procedure, <<"com.example.add2">>, {wamp_service_example, add}},
       {procedure, <<"com.example.echo">>, {wamp_service_example, echo}},
       {procedure, <<"com.example.multiple">>, {wamp_service_example, multiple_results}},
       {procedure, <<"com.example.circular">>, {wamp_service_example, circular}},
       {procedure, <<"com.example.circular_service_error">>, {wamp_service_example, circular_service_error}},
       {procedure, <<"com.example.unknown_error">>, {wamp_service_example, unknown_error}},
       {procedure, <<"com.example.notfound_error">>, {wamp_service_example, notfound_error}},
       {procedure, <<"com.example.validation_error">>, {wamp_service_example, validation_error}},
       {procedure, <<"com.example.service_error">>, {wamp_service_example, service_error}},
       {procedure, <<"com.example.authorization_error">>, {wamp_service_example, authorization_error}},
       {procedure, <<"com.example.timeout">>, {wamp_service_example, timeout}},
       {subscription, <<"com.example.onhello">>, {wamp_service_example, onhello}},
       {subscription, <<"com.example.onadd">>, {wamp_service_example, onadd}}
      ]}
    ]
   },
   {caller_service,
    [
     %% wamp opts
     {hostname, "localhost"},
     {port, 18082},
     {realm, <<"com.magenta.test">>},
     {encoding, json},
     {reconnect, true}
    ]
   }
  ]},
```

The __worker args__ are the usual connection options plus __service callbacks__ definitions, for each callback it will be added a procedure or subscription with the given URI and the handler given by the tuple `{module, function}. Finally the _reconnect options_ are the attempts to retry to reconnect and initial exponential backoff.

## Build

    $ rebar3 compile

## Test

In order to test you must start a wamp broker, for example [bondy](https://gitlab.com/leapsight/bondy) for testing.

Or using docker

    $ docker run --rm -it -p 18080:18080 -p 18081:18081 -p 18082:18082 --name bondy registry.gitlab.com/leapsight/bondy:latest

Start the erlang shell:

    $ rebar3 auto

In the Erlang shell start the micro service:

    application:start(wamp_service).

To test the micro service and published procedures on the same shell or a new one:

    wamp_service:call(<<"com.example.echo">>, ["Hello wamp!"], #{<<"security">> => #{<<"groups">> => [<<"admin">>]}}).
    wamp_service:call(<<"com.example.add2">>, [1, 1], #{}).
    wamp_service:call(<<"com.example.error">>, [], #{}). % error test
    wamp_service:publish(<<"com.example.onhello">>, [<<"Hello wamp!">>], #{}).
    wamp_service:publish(<<"com.example.onadd">>, [1, 2], #{}).

The `call` function return either the result or an error result, see `maybe_call` for variants
automatically raising an `error()`.

You can also register or unregister procedure or subscription dynamically in the following way:

    wamp_service:unregister(<<"com.example.echo">>).
    wamp_service:register(procedure, <<"com.example.echo">>, fun(X, _Opts) -> X end).

## Developing a new Service

In order to create a new service you should use the rebar3 template [basic_service_template](https://gitlab.com/leapsight-lojack/basic_service_template).

## Volume test

```erlang
application:start(wamp_service).
lists:foreach(fun(N) ->
                spawn(fun() ->
                        T1 = erlang:system_time(millisecond),
                        N1 = N + 1,
                        {ok, N1} = wamp_service:call(<<"com.example.add2">>, [N, 1], #{<<"trace_id">> => N}),
                        io:format("~p -> ~p~n", [N, erlang:system_time(millisecond) - T1])
                      end)
             end, lists:seq(1, 1000)).
```
