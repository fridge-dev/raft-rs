use raft;
use std::error::Error;
use std::net::Ipv4Addr;
use tokio::time::Duration;
use std::collections::HashMap;
use bytes::Bytes;

#[tokio::test]
async fn leader_election() -> Result<(), Box<dyn Error>> {
    let num_members = 5;
    let mut clients = HashMap::with_capacity(num_members);
    for i in 0..num_members {
        let client_config = config(i, 5);
        let client_id = client_config.cluster_info.my_replica_id.clone();
        let client = raft::create_raft_client(client_config).await?;
        clients.insert(client_id, client);
    }
    let clients = clients;

    sleep_sec(15).await;

    // Try to find leader
    let (_, any_client) = clients.iter().next().unwrap();
    let output = any_client.replication_log.start_replication(raft::StartReplicationInput {
        data: Bytes::default(),
    }).await;

    let leader = match output {
        Ok(ok) => {
            println!("Omg, got lucky and found leader. Replied: {:?}", ok);
            any_client
        }
        Err(raft::StartReplicationError::LeaderRedirect { leader_id, .. }) => {
            println!("Redirected to {:?}", leader_id);
            clients.get(&leader_id).unwrap().clone()
        }
        Err(e) => {
            panic!("Failed to find leader: {:?}", e);
        }
    };

    // Confirm leader
    let output = leader.replication_log.start_replication(raft::StartReplicationInput {
        data: Bytes::default(),
    }).await;
    if let Err(raft::StartReplicationError::LeaderRedirect { .. }) = output {
        panic!("wtf double redirect")
    }

    sleep_sec(20).await;

    Ok(())
}

fn config(id: usize, num_members: usize) -> raft::RaftClientConfig {
    let mut cluster_members = Vec::with_capacity(num_members);
    for i in 0..num_members {
        cluster_members.push(member_info(i));
    }

    raft::RaftClientConfig {
        log_directory: "/tmp/".to_string(),
        cluster_info: raft::ClusterInfo {
            my_replica_id: repl_id(id),
            cluster_members,
        },
        leader_heartbeat_duration: Duration::from_millis(3000),
        follower_min_timeout: Duration::from_millis(5000),
        follower_max_timeout: Duration::from_millis(15000),
    }
}

fn member_info(id: usize) -> raft::MemberInfo {
    raft::MemberInfo {
        replica_id: repl_id(id),
        replica_ip_addr: Ipv4Addr::from([127, 0, 0, 1]),
        replica_port: 3000 + id as u16,
    }
}

fn repl_id(id: usize) -> String {
    format!("replica-{}", id + 1)
}

async fn sleep_sec(secs: u64) {
    println!("Sleep {} sec", secs);
    tokio::time::sleep(Duration::from_secs(secs)).await;
    println!("Awake!");
}