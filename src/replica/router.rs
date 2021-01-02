// When would node need to write to one of its own clusters?
pub trait RaftClient {
    // Will attempt to discover leader to trigger a replication of data
    fn send_request(/* app specific types?, no, same as append_entries() */);
}
