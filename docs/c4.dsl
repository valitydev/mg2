workspace {

    model {
        user = person "Generic user"

        infra = softwareSystem "Infrastructure" {

            eventstream = container "Kafka events stream" {}
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

            mg_pulse = container "Pulse handler" {

            }

            mg_automaton = container "Automaton Thrift API" "Thrift service for managing machines in known namespaces" "erlang, http, thrift" {
                processor -> this "Executes RPC" "http, thrift"
                support_services -> this "Executes RPC" "http, thrift"
            }

            mg_es_thrift = container "Event Sink Thrift API" "Thrift service for reading all events of all machine in strictly ordered fashion" "erlang, http, thrift" {
                unknown -> this "Executes RPC" "http, thrift"
            }

            mg_es_machine = container "Event sink collector machine" {
                description "This process starts as a machine with internal processor. Other than that it is a machine with a namespace and its very own options."
                mg_es_thrift -> this "Queries" "erlang"
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

                mg_processor = component "Machine processor implementation" "" "erlang, http, thrift" {
                    description "In case of external processor it uses hackney client with thrift API"

                    this -> processor "Executes machine state processing RPC" "http, thrift"
                }

                mg_manager = component "Machines workers manager-supervisor" "" "erlang" {
                    mg_automaton -> this "Calls machines" "erlang"
                    this -> singleton_in_cluster_guarantee "Relies for instancing and addressing workers"
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
                        this -> mg_manager "Sends signal during task execution" "erlang"
                    }
                }

                mg_worker = component "Actual machine process" "" "erlang" {
                    mg_manager -> this "Starts process and passes gen_server calls"
                    this -> mg_es_lf_kafka "Publishes lifecycle events" "erlang"
                    this -> mg_processor "Processes state" "erlang"
                    this -> mg_es_kafka "Publishes business events" "erlang"
                    this -> mg_es_machine "Publishes business events" "erlang"
                    this -> mg_events_storage "Reads and writes" "erlang"
                    this -> mg_machines_storage "Reads and writes" "erlang"
                }
            }
        }
    }

    views {

        theme default
    }

}
