-module(mg_skd_pulse).

-include_lib("mg_scheduler/include/pulse.hrl").

%% API
-export_type([beat/0]).
-export_type([handler/0]).

-callback handle_beat(handler(), beat() | any()) -> ok.

%%
%% API
%%
-type beat() ::
    % Scheduler handling
    #mg_skd_task_add_error{}
    | #mg_skd_search_success{}
    | #mg_skd_search_error{}
    | #mg_skd_task_error{}
    | #mg_skd_new_tasks{}
    | #mg_skd_task_started{}
    | #mg_skd_task_finished{}
    | #mg_skd_quota_reserved{}.

-type handler() :: mg_skd_utils:mod_opts() | undefined.
