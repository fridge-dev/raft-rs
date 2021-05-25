**Example use case of a single cluster exposing a strictly serializable key-value store**

![arch](./kv-over-raft-application-architecture.jpg)

A cluster is made up of many nodes participating in a single instance of raft together. Each node
will expose 3 endpoints, where an endpoint is a unique IP/port combination. The endpoints are:
* {raft_endpoint} - Serves raft RPCs, called internally by lib.
* {app_be_endpoint} - Backend: Serves KV requests, called by Frontend. Depends on raft lib intra-process.
* {app_fe_endpoint} - Frontend: Serves client facing requests. Calls Backend. Can optionally be in
                      separate process.

Here is the use case I'm imagining:
1. Generic raft library that uses RPC/HTTP on {raft_endpoint} to coordinate with peers. Simply
    replicates blobs.
2. There is an application specific backend RPC server on {app_be_endpoint}. It validates/accepts
    transitions with app-specific logic to provide a strictly serializable key-value store.
    It calls into raft lib. It does not follow redirects.
3. Application is a web service over {app_fe_endpoint} that exposes the API to external service
    clients. It does basic frontend stuff (authN, authZ, throttling, etc) and then forwards
    request to the correct {app_be_endpoint}. Internally, it will follow redirects from the
    {app_be_endpoint}. It is not fully cluster aware, it just needs to remember the most recent
    leader and keep using that until the backend redirects. It can discover the initial leader
    at application startup by trying to call the localhost {app_be_endpoint}. Since this layer
    follows redirects, a client can make request to any host's {app_fe_endpoint} and the
    Application will take care of routing to the correct {app_be_endpoint} leader. This way,
    the cluster could actually just sit behind a load balancer and clients call the load balancer
    or clients call any host in the cluster and it just works.
4. Some client needs KV store, so they call Application on {app_fe_endpoint} from 3. Their
    connection will never need to be redirected.

--------------------

Copying notes from old gRPC server module:

3 different gRPC servers:

Raft data plane ({raft_endpoint}) - this is the primitive building block.
* AppendEntries(...)
* RequestVote(...)

Raft control plane ({raft_endpoint}) - used during raft deployment/cycling (infrequent). They ultimately delegate
towards raft data plane.
* ObserveCluster(...) - for adding self to cluster
* LeaveCluster(...) - for removing self from cluster
* UpdateCluster(...) - for existing members to become aware of added/removed members

Public facing KV store ({app_endpoint}) - supports redirects. Ultimately results in raft data plane calls.
* Get(key)
* Put(key, value, options...)
* Delete(key, options...)
