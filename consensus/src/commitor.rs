use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::u128;

use crate::config::Stake;
use crate::error::ConsensusResult;
use crate::{Block, Committee, ConsensusMessage, SeqNumber};
use crypto::Digest;
use log::{debug, info, warn};
use store::Store;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::{Receiver, Sender};

async fn try_to_commit(digests: Vec<Digest>, mut store: Store) -> ConsensusResult<()> {
    for digest in digests {
        if let Some(bytes) = store.read(digest.to_vec()).await? {
            let block: Block = bincode::deserialize(&bytes)?;
            if !block.payload.is_empty() {
                info!("Committed {}", block);

                #[cfg(feature = "benchmark")]
                for x in &block.payload {
                    info!(
                        "Committed B{}({}) timestamp {}",
                        block.height,
                        base64::encode(x),
                        block.timestamp,
                    );
                }
            }
        }
    }
    Ok(())
}

enum CommitMessage {
    AddInstance(u128, SeqNumber),
    UpdateExec(u128),
    ArbcOutput(u128, SeqNumber, Option<Digest>),
}

pub struct Commitor {
    tx_channel: Sender<CommitMessage>,
}

impl Commitor {
    pub fn new(
        tx_core: Sender<ConsensusMessage>,
        committee: Committee,
        store: Store,
        tx_clean: Sender<(Vec<Digest>, u128, SeqNumber)>,
    ) -> Self {
        let (tx_channel, mut rx_channel): (_, Receiver<CommitMessage>) = channel(1000);
        let mut used = HashSet::new();
        let mut instance = BinaryHeap::new();
        let (mut exec_ts, mut pass_ts): (u128, u128) = (0, 0);
        let mut commit_map: HashMap<(u128, SeqNumber), Option<Digest>> = HashMap::new();
        let mut commit_instance = BinaryHeap::new();

        tokio::spawn(async move {
            while let Some(message) = rx_channel.recv().await {
                match message {
                    CommitMessage::AddInstance(ts, h) => {
                        if used.insert((ts, h)) {
                            instance.push(Reverse((ts, h)));
                        }
                    }
                    CommitMessage::UpdateExec(exec) => {
                        if exec >= exec_ts {
                            exec_ts = exec;
                        }
                        // try to commit
                        let (mut c_ts, mut c_h) = (0, 0);
                        let mut commits: Vec<Digest> = Vec::new();
                        while let Some(val) = instance.peek() {
                            let (ts, h) = val.clone().0;
                            let flag = commit_map.contains_key(&(ts, h));
                            debug!("try to commit min_ts {} min_h {}, exec_ts {}", ts, h, exec);
                            if flag && ts <= exec_ts {
                                if let Some(block) = commit_map.remove(&(ts, h)).unwrap() {
                                    commits.push(block);
                                }
                                let _ = instance.pop();
                                (c_ts, c_h) = (ts, h);
                            } else {
                                break;
                            }
                        }
                        if commits.len() > 0 {
                            //清理缓存
                            if let Err(err) = tx_clean.send((commits.clone(), c_ts, c_h)).await {
                                panic!("Failed to send message through clean channel: {}", err);
                            }
                            if let Err(e) = try_to_commit(commits, store.clone()).await {
                                warn!("commit error: {}", e);
                            }
                        }
                    }
                    CommitMessage::ArbcOutput(ts, height, block) => {
                        commit_map.insert((ts, height), block); //有哪些区块被提交
                        commit_instance.push(Reverse((ts, height))); //提交区块插入小顶堆

                        //try to pass
                        let mut flag = false;
                        while commit_instance.len() as Stake >= committee.quorum_threshold() {
                            let (ps, h) = commit_instance.peek().unwrap().clone().0;
                            debug!("try to notify timestamp {} height {}", ps, h);
                            if ps >= pass_ts {
                                flag = true;
                                pass_ts = ps;
                            }
                            let _ = commit_instance.pop();
                        }
                        if flag {
                            let message = ConsensusMessage::NotifyPassMsg(pass_ts);
                            if let Err(err) = tx_core.send(message).await {
                                panic!("Failed to send message through core channel: {}", err);
                            }
                        }
                        //try to commit
                    }
                }
            }
        });

        Self { tx_channel }
    }

    pub async fn add_instance(&mut self, ts: u128, h: SeqNumber) -> ConsensusResult<()> {
        let message = CommitMessage::AddInstance(ts, h);
        if let Err(e) = self.tx_channel.send(message).await {
            panic!("Failed to send message through commit channel: {}", e);
        }
        Ok(())
    }

    pub async fn update_exec(&mut self, ts: u128) -> ConsensusResult<()> {
        debug!("update exec {}", ts);
        let message = CommitMessage::UpdateExec(ts);
        if let Err(e) = self.tx_channel.send(message).await {
            panic!("Failed to send message through commit channel: {}", e);
        }
        Ok(())
    }

    pub async fn add_output(
        &mut self,
        ts: u128,
        h: SeqNumber,
        digest: Option<Digest>,
    ) -> ConsensusResult<()> {
        debug!("add output timestamp {} height {}", ts, h);
        let message = CommitMessage::ArbcOutput(ts, h, digest);
        if let Err(e) = self.tx_channel.send(message).await {
            panic!("Failed to send message through commit channel: {}", e);
        }
        Ok(())
    }
}
