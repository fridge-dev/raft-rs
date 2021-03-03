use raft;
use std::error::Error;
use std::net::Ipv4Addr;
use tokio::time::Duration;

#[tokio::test]
async fn leader_election() -> Result<(), Box<dyn Error>> {
    println!("Starting...");
    let _client1 = raft::create_raft_client(config(1, 5)).await?;
    let _client2 = raft::create_raft_client(config(2, 5)).await?;
    let _client3 = raft::create_raft_client(config(3, 5)).await?;
    let _client4 = raft::create_raft_client(config(4, 5)).await?;
    let _client5 = raft::create_raft_client(config(5, 5)).await?;

    println!("Sleep 30 sec.");
    tokio::time::sleep(Duration::from_secs(30)).await;

    Ok(())
}

fn config(id: u8, num_members: usize) -> raft::RaftClientConfig {
    let mut cluster_members = Vec::with_capacity(num_members);
    for i in 0..num_members {
        cluster_members.push(member_info(i as u8 + 1));
    }

    raft::RaftClientConfig {
        log_directory: "/tmp/".to_string(),
        cluster_info: raft::ClusterInfo {
            my_replica_id: format!("replica-{}", id),
            cluster_members,
        },
        leader_heartbeat_duration: Duration::from_millis(3000),
        follower_min_timeout: Duration::from_millis(5000),
        follower_max_timeout: Duration::from_millis(15000),
    }
}

fn member_info(id: u8) -> raft::MemberInfo {
    raft::MemberInfo {
        replica_id: format!("replica-{}", id),
        replica_ip_addr: Ipv4Addr::from([127, 0, 0, 1]),
        replica_port: 3000 + id as u16,
    }
}
