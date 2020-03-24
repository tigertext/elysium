-module(elysium_prometheus).
-author("leo").

%% API
-export([setup/0]).
-export([report_metrics/3]).


-define(DEFAULT_REGISTRY, default).
-define(DEFAULT_MACHINE_NAME, try config:get_env(machine_name, "anonymous_machine") catch _:_ -> "anonymous_machine" end).


setup() ->
    is_prometheus_started() andalso do_setup().

is_prometheus_started() ->
    [AppName || {AppName, _, _} <- application:which_applications(), AppName == prometheus] == [prometheus].

do_setup() ->
    Get_Connection_Duration_Buckets =
        case application:get_env(elysium, prometheus_default_cassandra_get_connection_duration_buckets, [10, 100, 500, 1000, 2500, 5000, 10000, 50000, 100000, 250000, 500000, 1000000]) of
            {ok, V1} -> V1;
            V2 -> V2
        end,
    prometheus_histogram:declare([
        {name, cassandra_get_connection_duration_microseconds},
        {registry, ?DEFAULT_REGISTRY},
        {labels, [machine_name, destination_keyspace, destination_table, command_type]},
        {buckets, Get_Connection_Duration_Buckets},
        {help, "The time to get cassandra connection from connection pool in microseconds."}]),
    Exec_Cmd_Duration_Buckets =
        case application:get_env(elysium, prometheus_default_cassandra_exec_cmd_duration_buckets, [10, 100, 500, 1000, 2500, 5000, 10000, 50000, 100000, 250000, 500000, 1000000]) of
            {ok, V3} -> V3;
            V4 -> V4
        end,
    prometheus_histogram:declare([
        {name, cassandra_exec_cmd_duration_microseconds},
        {registry, ?DEFAULT_REGISTRY},
        {labels, [machine_name, destination_keyspace, destination_table, command_type]},
        {buckets, Exec_Cmd_Duration_Buckets},
        {help, "The time to execute cassandra commands against DB in microseconds."}]).

report_metrics(_Metric_Name, undefined, _Duration) ->
    ok;
report_metrics(Metric_Name, {Cmd_Type, KeySpace, Table}, Duration) ->
    if_collect_cassandra_metrics() andalso prometheus_histogram:observe(Metric_Name, [?DEFAULT_MACHINE_NAME, KeySpace, Table, Cmd_Type], Duration).

if_collect_cassandra_metrics() ->
    try config:get_env(prometheus_collect_cassandra_metrics, false)
    catch _:_ -> false
    end.