workspace {

    model {
        user = person "Generic user"

        infra = softwareSystem "Infrastructure" {

            eventstream = container "Kafka events stream" {

            }

            riak = container "Riak ring" {
                riak -> riak "Black magic"
            }
        }

        backoffice = softwareSystem "Backoffice" {

            backoffice_services = container "Backoffice services" {
                this -> eventstream "Reads" "kafka, thrift"
            }

            support_services = container "Support" {}

            unknown = container "Unknown service" {}
        }

        business = softwareSystem "Business processing services" {

             processor = container "You-name-it processor" {}
        }

        gateway = softwareSystem "Web API" {

            web = container "JsonAPI border services" {
                user -> this "Calls operation" "http, json"
                this -> processor "Executes RPC" "http, thrift"
            }
        }

        machinegun = softwareSystem "Machinegun cluster" {

            mg_automaton = container "Automaton Thrift API" "Thrift service for managing machines in known namespaces" "erlang, http, thrift" {
                processor -> this "Executes RPC" "http, thrift"
                support_services -> this "Executes RPC" "http, thrift"
            }

            mg_es_kafka = container "Machines Business Event Sink" "Machines business events publisher via kafka topic on per-namcespace basis and thirft serialization of its data" "erlang, kafka, thrift" {
                this -> eventstream "Produces" "thrift"
            }

            mg_es_lf_kafka = container "Lifecycle Event Sink" "Machines lifecycle events publisher via kafka topic and thirft serialization of data" "erlang, kafka, thrift" {
                this -> eventstream "Produces" "thrift"
            }

            container "Cluster management" {

                discovery = component "Discovery/distribution service" {
                    this -> this "Discovers" "erlang"
                }

                group_with_leadership_in_cluster = component "Squads in cluster" "Cluster allows group of processes know about each other and elect leader" {
                    this -> discovery "Relies upon"
                }

                singleton_in_cluster_guarantee = component "Singleton in cluster" "Cluster's guarnatee for entity as singleton" {
                    this -> discovery "Relies upon"
                }
            }

            container "Quota management" {

                quota_worker = component "Quota manager" {}
            }

            mg_machines = container "Namespaced events machines" {
                description "Each instance of child component is dedicated to parent namespace only"

                group "Storages" {

                    mg_events_storage = component "Machine events storage" "Buckets per machine's namespace" "erlang" {
                        this -> riak "Reads and writes business events" "erlang, protobuff"
                    }

                    mg_machines_storage = component "Machines state storage" "Stores machines status, context and auxiliary data necessary to load machine and make it continue its work" "erlang" {
                        this -> riak "Reads and writes state" "erlang, protobuff"
                    }

                    mg_notification_storage = component "Notification storage with special write API" "erlang" {
                        this -> riak "Reads and writes messages" "erlang, protobuff"
                    }
                }

                group "Schedulers" {

                    mg_scanner = component "Scanner for interrupted, timers queues and notification delivery scheduler" "" "erlang" {
                        this -> group_with_leadership_in_cluster "Each scan occurrs only once"
                        this -> mg_machines_storage "Scans periodicall"
                        this -> mg_notification_storage "Scans periodically"
                        this -> this "Inquires schedulers of same kind in cluster"
                    }

                    mg_scheduler = component "Scheduler" "" "erlang" {
                        this -> quota_worker "Reserves resource partition"
                        mg_scanner -> this "Disseminate tasks"
                    }

                    mg_task_runner = component "Tasks runner" "" "erlang" {
                        mg_scheduler -> this "Starts new task" "erlang"
                    }
                }

                group "Machinery" {

                    mg_processor = component "Machine processor implementation" "" "erlang, http, thrift" {
                        description "In case of external processor it uses hackney client with thrift API"

                        this -> processor "Executes machine state processing RPC" "http, thrift"
                    }

                    mg_manager = component "Machines workers manager-supervisor" "" "erlang" {
                        mg_task_runner -> this "Sends signal during task execution" "erlang"
                        mg_automaton -> this "Calls machines" "erlang"
                        this -> singleton_in_cluster_guarantee "Relies for instancing and addressing workers"
                    }

                    mg_worker = component "Machine worker" "" "erlang" {
                        mg_manager -> this "Starts process and passes machine calls"
                    }

                    mg_machine = component "Machine" "" "erlang" {
                        mg_worker -> this "Handles calls with implementation"
                        this -> mg_machines_storage "Reads and writes" "erlang"
                        this -> mg_notification_storage "Writes" "erlang"
                    }

                    mg_event_machine = component "Event Machine" "" "erlang" {
                        mg_machine -> this "Processes state"
                        this -> mg_processor "Calls state processor"
                        this -> mg_events_storage "Reads and writes" "erlang"
                        this -> mg_es_kafka "Publishes business events" "erlang"
                    }
                }
            }

            mg_pulse = container "Pulse handler" {
                mg_scanner -> this "Pulse beats with lifecycle" "erlang"
                mg_scheduler -> this "Pulse beats with lifecycle" "erlang"
                mg_task_runner -> this "Pulse beats with lifecycle" "erlang"

                mg_machines_storage -> this "Pulse beats with lifecycle" "erlang"
                mg_events_storage -> this "Pulse beats with lifecycle" "erlang"
                mg_notification_storage -> this "Pulse beats with lifecycle" "erlang"

                mg_manager -> this "Pulse beats with lifecycle" "erlang"
                mg_worker -> this "Pulse beats with lifecycle" "erlang"
                mg_machine -> this "Pulse beats with lifecycle" "erlang"
                mg_event_machine -> this "Pulse beats with lifecycle" "erlang"
                mg_processor -> this "Pulse beats with lifecycle" "erlang"

                this -> mg_es_lf_kafka "Publishes lifecycle events" "erlang"
            }
        }
    }

    views {

        theme default
    }

}
