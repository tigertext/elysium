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

report_metrics(Metric_Name, {Fun, Args}, Duration) ->
    case if_collect_cassandra_metrics() of
        false ->
            ok;
        true ->
            case get_cmd_details(Fun, Args) of
                undefined ->
                    ok;
                {KeySpace, Table, Cmd_Type} ->
                    prometheus_histogram:observe(Metric_Name, [?DEFAULT_MACHINE_NAME, KeySpace, Table, Cmd_Type], Duration)
            end
    end.

if_collect_cassandra_metrics() ->
    try config:get_env(prometheus_collect_cassandra_metrics, false)
    catch _:_ -> false
    end.

get_cmd_details(run_perform, [Query]) ->
    do_get_cmd_details(Query);
get_cmd_details(execute_prepared_query_internal, [Query, _Values]) ->
    do_get_cmd_details(Query);
get_cmd_details(run_insert, [Query, _Values]) ->
    do_get_cmd_details(Query);
%% following Funs are actually not used.
%% add to avoid any crash
get_cmd_details(run_prepare, _) ->
    undefined;
get_cmd_details(run_execute, _) ->
    undefined.

do_get_cmd_details(Query) ->
    case Query of
        ["SELECT ", _, " FROM ", Key | _] ->
            [KeySpace, Table] = string:tokens(Key, "."),
            {"SELECT", KeySpace, Table};
        _ ->
            case string:tokens(lists:flatten(Query), " ") of
                ["INSERT", "INTO", Key | _] ->
                    [KeySpace, Table] = string:tokens(Key, "."),
                    {"INSERT", KeySpace, Table};
                ["SELECT", "count(*)", "FROM", Key | _] ->
                    [KeySpace, Table] = string:tokens(Key, "."),
                    {"SELECT", KeySpace, Table};
                ["SELECT", _, "FROM", Key | _] ->
                    [KeySpace, Table] = string:tokens(Key, "."),
                    {"SELECT", KeySpace, Table};
                ["UPDATE", Key | _] ->
                    [KeySpace, Table] = string:tokens(Key, "."),
                    {"UPDATE", KeySpace, Table};
                ["DELETE", "FROM", Key | _] ->
                    [KeySpace, Table] = string:tokens(Key, "."),
                    {"DELETE", KeySpace, Table};
                ["CREATE", "TABLE", "IF", "NOT", "EXISTS", Key | _] ->
                    [KeySpace, Table] = string:tokens(Key, "."),
                    {"CREATE", KeySpace, Table};
                ["CREATE", "TABLE", Key | _] ->
                    [KeySpace, Table] = string:tokens(Key, "."),
                    {"CREATE", KeySpace, Table};
                ["CREATE", "COLUMNFAMILY", Key | _] ->
                    [KeySpace, Table] = string:tokens(Key, "."),
                    {"CREATE", KeySpace, Table};
                _ ->
                    lager:warning("Unrecgonized Cassandra Query ~p when report to prometheus", [Query]),
                    undefined
            end
    end.