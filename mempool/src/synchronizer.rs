use crate::config::Committee;
use crate::core::MempoolMessage;
use crate::error::{MempoolError, MempoolResult};
use bytes::Bytes;
use consensus::{Block, ConsensusMessage, SeqNumber};
use crypto::{Digest, PublicKey};
use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error, info};
use network::NetMessage;
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/synchronizer_tests.rs"]
pub mod synchronizer_tests;

enum SynchronizerMessage {
    Sync(HashSet<Digest>, Block),
    Clean(u128, SeqNumber),
}

pub struct Synchronizer {
    inner_channel: Sender<SynchronizerMessage>,
    store: Store,
}

impl Synchronizer {
    pub fn new(
        consensus_channel: Sender<ConsensusMessage>,
        store: Store,
        name: PublicKey,
        committee: Committee,
        network_channel: Sender<NetMessage>,
        sync_retry_delay: u64,
    ) -> Self {
        let (tx_inner, mut rx_inner): (_, Receiver<SynchronizerMessage>) = channel(10000);

        let store_copy = store.clone();
        tokio::spawn(async move {
            let mut waiting = FuturesUnordered::new();
            let mut pending: HashMap<(u128, SeqNumber), Sender<()>> = HashMap::new();
            let mut requests = HashMap::new();

            let timer = sleep(Duration::from_millis(5000));
            tokio::pin!(timer);
            loop {
                tokio::select! {
                    Some(message) = rx_inner.recv() => match message {
                        SynchronizerMessage::Sync(mut missing, block) => {//等待缺失的payload
                            // TODO [issue #7]: A bad node may make us run out of memory by sending many blocks
                            // with different round numbers or different payloads.
                            let (ts,height) = (block.timestamp,block.height);
                            let author = block.author;
                            if pending.contains_key(&(ts,height)) {    //如果处理过，就不用在处理了
                                continue;
                            }
                            let wait_for = missing.iter().cloned().map(|x| (x, store_copy.clone())).collect();
                            let (tx_cancel, rx_cancel) = channel(1);
                            pending.insert((ts,height),  tx_cancel);
                            let fut = Self::waiter(wait_for, block, rx_cancel);//等待其他发送缺失的payload，然后从本地的store中取出
                            waiting.push(fut);//存入等待队列中

                            let missing: Vec<_> = missing
                                .drain()
                                .filter(|x| !requests.contains_key(x))
                                .collect();
                            if !missing.is_empty() {
                                let now = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .expect("Failed to measure time")
                                    .as_millis();
                                for x in &missing {
                                    requests.insert(x.clone(), (ts,height, now));
                                }

                                let message = MempoolMessage::PayloadRequest(missing.clone(), name); //向发送block的节点请求payload
                                Self::transmit(
                                    &message,
                                    &name,
                                    Some(&author),
                                    &committee,
                                    &network_channel
                                )
                                .await
                                .expect("Failed to send payload sync request");
                            }
                        },
                        SynchronizerMessage::Clean(timestamp,height) => {//将小于等于 round 轮的请求都清除
                            for ((ts,h),handler) in &pending {
                                if *ts<timestamp || (timestamp == *ts && *h< height) {
                                    let _ = handler.send(()).await;
                                }
                            }
                            pending.retain(|(ts,h), _| *ts>timestamp || (timestamp == *ts && *h> height));
                            requests.retain(|_, (ts,h,_)| *ts>timestamp || (timestamp == *ts && *h> height));
                        }
                    },
                    Some(result) = waiting.next() => { //等待请求有结果了
                        match result {
                            Ok(Some(block)) => {
                                debug!("mempool sync loopback block {:?}", block);
                                let _ = pending.remove(&(block.timestamp,block.height));
                                for x in &block.payload {//将已经收到的payload去除
                                    let _ = requests.remove(x);
                                }
                                info!("loop back block timestamp {} height {}",block.timestamp,block.height);
                                let message = ConsensusMessage::LoopBackMsg(block);
                                if let Err(e) = consensus_channel.send(message).await {
                                    panic!("Failed to send message to consensus: {}", e);
                                }
                            },
                            Ok(None) => (),
                            Err(e) => error!("{}", e)
                        }
                    },
                    () = &mut timer => {//超时后，重复发送request
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Failed to measure time")
                            .as_millis();
                        let retransmit: Vec<_> = requests
                            .iter()
                            .filter(|(_, (_,_, timestamp))| timestamp + (sync_retry_delay as u128) < now)
                            .map(|(digest, _)| digest)
                            .cloned()
                            .collect();
                        if !retransmit.is_empty() {
                            let message = MempoolMessage::PayloadRequest(retransmit, name);
                            Self::transmit(
                                &message,
                                &name,
                                None,
                                &committee,
                                &network_channel
                            )
                            .await
                            .expect("Failed to send payload sync request");
                        }
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(5000));
                    },
                    else => break,
                }
            }
        });
        Self {
            inner_channel: tx_inner,
            store,
        }
    }

    async fn waiter(
        mut missing: Vec<(Digest, Store)>,
        deliver: Block,
        mut handler: Receiver<()>,
    ) -> MempoolResult<Option<Block>> {
        //阻塞，等待有数据，并将其写完
        let waiting: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();
        tokio::select! {
            result = try_join_all(waiting) => {
                result.map(|_| Some(deliver)).map_err(MempoolError::from)
            }
            _ = handler.recv() => Ok(None),
        }
    }

    pub async fn transmit(
        message: &MempoolMessage,
        from: &PublicKey,
        to: Option<&PublicKey>,
        committee: &Committee,
        network_channel: &Sender<NetMessage>,
    ) -> MempoolResult<()> {
        let addresses = if let Some(to) = to {
            //如果没有指定发送地址，则广播给出自己以外的所有人
            debug!("Sending {:?} to {}", message, to);
            vec![committee.mempool_address(to)?]
        } else {
            debug!("Broadcasting {:?}", message);
            committee.broadcast_addresses(&from)
        };
        let bytes = bincode::serialize(message).expect("Failed to serialize core message");
        let message = NetMessage(Bytes::from(bytes), addresses);
        if let Err(e) = network_channel.send(message).await {
            panic!("Failed to send block through network channel: {}", e);
        }
        Ok(())
    }

    pub async fn verify_payload(&mut self, block: Block) -> MempoolResult<bool> {
        let mut missing = HashSet::new();
        for digest in &block.payload {
            if self.store.read(digest.to_vec()).await?.is_none() {
                debug!("Requesting sync for payload {}", digest);
                missing.insert(digest.clone());
            }
        }

        if missing.is_empty() {
            return Ok(true);
        }
        info!(
            "miss block timestamp {} height {}",
            block.timestamp, block.height
        );
        let message = SynchronizerMessage::Sync(missing, block);
        if let Err(e) = self.inner_channel.send(message).await {
            panic!("Failed to send message to synchronizer core: {}", e);
        }
        Ok(false)
    }

    pub async fn cleanup(&mut self, timestamp: u128, height: SeqNumber) {
        let message = SynchronizerMessage::Clean(timestamp, height);
        debug!("cleanup timestamp {} height {}", timestamp, height);
        if let Err(e) = self.inner_channel.send(message).await {
            panic!("Failed to send message to synchronizer core: {}", e);
        }
    }
}
