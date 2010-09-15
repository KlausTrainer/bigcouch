% Copyright 2010 Cloudant
% 
% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(fabric_doc_open).

-export([go/3]).

-include("fabric.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

go(DbName, Id, Options) ->
    Workers = fabric_util:submit_jobs(mem3:shards(DbName,Id), open_doc,
        [Id, [deleted|Options]]),
    SuppressDeletedDoc = not lists:member(deleted, Options),
    R = couch_util:get_value(r, Options, couch_config:get("cluster","r","2")),
    Acc0 = {length(Workers), list_to_integer(R), []},
    case fabric_util:recv(Workers, #shard.ref, fun handle_message/3, Acc0) of
    {ok, {ok, #doc{deleted=true}}} when SuppressDeletedDoc ->
        {not_found, deleted};
    {ok, Else} ->
        Else;
    Error ->
        Error
    end.

handle_message({ok, Doc}, _Worker0, {_WaitingCount, 1, _GroupedReplies0}) ->
    {stop, {ok, Doc}};
handle_message(NewReply, Worker0, {WaitingCount, R, GroupedReplies0}) ->
    GroupedReplies =
    case NewReply of
    {rexi_DOWN, _RexiMonPid, _ServerPid, _Reason} ->
        GroupedReplies0;
    {rexi_EXIT, _Reason} ->
        GroupedReplies0;
    _ ->
        [{Worker0, NewReply} | GroupedReplies0]
    end,
    N = list_to_integer(couch_config:get("cluster","n","3")),
    case length(GroupedReplies) of
    NumReplies when NumReplies > N div 2 ->
        % check for quorum
        case find_r_equal_versions(N div 2 + 1, GroupedReplies) of
        false when WaitingCount =:= 1 ->
            % last message arrived, but still no quorum
            {Worker, Reply} = get_highest_version(GroupedReplies),
            case Reply of
            {not_found, missing} ->
                ok;
            _ ->
                spawn(fun() ->
                          read_quorum_repairer(GroupedReplies, {Worker, Reply})
                      end)
            end,
            {stop, Reply};
        false ->
            {ok, {WaitingCount-1, R, GroupedReplies}};
        {Worker, Reply} ->
            % we got a quorum
            case agree(GroupedReplies) of
            false when Reply =/= {not_found, missing} ->
                read_quorum_repairer(GroupedReplies, {Worker, Reply});
            true ->
                ok
            end,
            {stop, Reply}
        end;
    NumReplies when NumReplies >= R ->
        case find_r_equal_versions(R, GroupedReplies) of
        {_Worker, Reply} ->
            % we have reached agreement
            {stop, Reply};
        false when WaitingCount =:= 1->
            % last message arrived: version with highest rev number wins
            {Worker, Reply} = get_highest_version(GroupedReplies),
            case Reply of
            {not_found, missing} ->
                ok;
            _ ->
                spawn(fun() ->
                          read_quorum_repairer(GroupedReplies, {Worker, Reply})
                      end)
            end,
            {stop, Reply};
        false ->
            {ok, {WaitingCount-1, R, GroupedReplies}}
        end;
    _ when WaitingCount =:= 1 ->
        {stop, {not_found, missing}};
    _ ->
        {ok, {WaitingCount-1, R, GroupedReplies}}
    end.

-spec agree({#shard{}, {ok, #doc{}}|{not_found, missing}}) -> boolean().
agree([H|T]) ->
    {_Worker0, {Tag0, Doc0}} = H,
    lists:all(
        fun({_Worker, {Tag, Doc}}) ->
            % for the sake of efficiency do this
            % instead of comparing whole replies
            case Tag of
            not_found when Tag0 =:= not_found ->
                true;
            ok when Tag0 =:= ok ->
                % here we actually got a #doc{}
                Revs0 = Doc0#doc.revs,
                case Doc#doc.revs of
                Revs0 -> true;
                _ -> false
                end;
            _ ->
                false
            end
        end, T).

-spec find_r_equal_versions(integer(), {#shard{}, {ok, #doc{}}|{not_found, missing}}) -> {#shard{}, {ok, #doc{}}} | {nil, {not_found, missing}}.
find_r_equal_versions(R, GroupedReplies) ->
    case lists:dropwhile(
             fun({_Worker0, {Tag0, Doc0}}) ->
                 case lists:filter(
                     fun({_Worker, {Tag, Doc}}) ->
                         % for the sake of efficiency do this
                         % instead of comparing whole replies
                         case Tag of
                         not_found when Tag0 =:= not_found ->
                             true;
                         ok when Tag0 =:= ok ->
                             % here we actually got a #doc{}
                             Revs0 = Doc0#doc.revs,
                             case Doc#doc.revs of
                             Revs0 -> true;
                             _ -> false
                             end;
                         _ ->
                             false
                         end
                     end, GroupedReplies) of
                 L when length(L) < R -> true;
                 _ -> false
                 end
             end, GroupedReplies) of
    [] ->
        false;
    [{Worker, Reply}|_] ->
        case Reply of
        {not_found, missing} -> {nil, Reply};
        {ok, Doc} -> {Worker, {ok, Doc}}
        end
    end.

-spec get_highest_version([{#shard{}, {ok, #doc{}}|{not_found, missing}}]) -> {#shard{}, {ok, #doc{}}} | {nil, {not_found, missing}}.
get_highest_version([H|T]) ->
    get_highest_version(H, T).

get_highest_version(HighestVersion, []) ->
    case HighestVersion of
    {_Worker, {not_found, missing}} -> {nil, {not_found, missing}};
    HighestVersion -> HighestVersion
    end;
get_highest_version(HighestVersion0, [H|T]) ->
    HighestVersion =
    case H of
    {_Worker, {ok, #doc{revs=Revs}}} ->
        {_Worker0, {ok, #doc{revs=Revs0}}} = HighestVersion0,
        case Revs > Revs0 of
        true -> H;
        false -> HighestVersion0
        end;
    {_Worker, {not_found, missing}} ->
        HighestVersion0
    end,
    get_highest_version(HighestVersion, T).

-spec read_quorum_repairer([{#shard{}, {ok, #doc{}}|{not_found, missing}}], {#shard{}, {ok, #doc{}}}) -> none().
read_quorum_repairer(GroupedReplies0, {_Worker0, {ok, Doc0}}) ->
    GroupedReplies =
    lists:filter(
        fun({_Worker, Reply}) ->
            case Reply of
            {ok, Doc0} -> false;
            _ -> true
            end
        end, GroupedReplies0),
    WorkerDocsZip =
    lists:map(
        fun({Worker, Reply}) ->
            Docs =
            case Reply of
            {ok, Doc} ->
                {Seq0, [_|T]} = Doc0#doc.revs,
                {Seq, _} = Doc#doc.revs,
                case Seq0 of
                0 when Doc#doc.deleted =/= true ->
                    [Doc#doc{deleted=true}, Doc0];
                0 ->
                    [Doc0];
                _ when Seq =< Seq0 andalso Doc#doc.deleted =/= true ->
                    [Doc#doc{deleted=true}, Doc0#doc{revs={Seq0-1, T}}];
                _ ->
                    [Doc0#doc{revs={Seq0-1, T}}]
                end;
            {not_found, missing} ->
                {Seq0, [_|T]} = Doc0#doc.revs,
                case Seq0 of
                0 ->
                    [Doc0];
                _ ->
                    [Doc0#doc{revs={Seq0-1, T}}]
                end
            end,
            {Worker, Docs}
        end, GroupedReplies),
    Options = [merge_conflicts, {user_ctx,{user_ctx,null, [<<"_reader">>,<<"_writer">>,<<"_admin">>], undefined}}],
    lists:foreach(
        fun({#shard{name=Name, node=Node}, Docs}) ->
            rexi:cast(Node, {fabric_rpc, update_docs, [Name, Docs, Options]})
        end, WorkerDocsZip).
