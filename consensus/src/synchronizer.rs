use crate::config::Committee;
use crate::core::ConsensusMessage;
use crate::error::ConsensusResult;
use crate::filter::FilterInput;
use crypto::{Digest, PublicKey};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error, info};
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/synchronizer_tests.rs"]
pub mod synchronizer_tests;

const TIMER_ACCURACY: u64 = 5_000;

pub struct Synchronizer {
    store: Store,
    inner_channel: Sender<Digest>,
}

impl Synchronizer {
    pub async fn new(
        name: PublicKey,
        committee: Committee,
        store: Store,
        network_filter: Sender<FilterInput>,
        _core_channel: Sender<ConsensusMessage>,
        sync_retry_delay: u64,
    ) -> Self {
        let (tx_inner, mut rx_inner): (_, Receiver<Digest>) = channel(10000);

        let store_copy = store.clone();
        tokio::spawn(async move {
            let mut waiting = FuturesUnordered::new();
            let mut pending = HashSet::new();
            let mut requests = HashMap::new();

            let timer = sleep(Duration::from_millis(TIMER_ACCURACY));
            tokio::pin!(timer);
            loop {
                tokio::select! {
                    Some(digest) = rx_inner.recv() => {
                        if pending.insert(digest.clone()) {

                            let fut = Self::waiter(store_copy.clone(),digest.clone());
                            waiting.push(fut);
                            if !requests.contains_key(&digest){
                                debug!("Requesting sync for block digest {}", digest);
                                let now = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .expect("Failed to measure time")
                                    .as_millis();
                                requests.insert(digest.clone(), now);
                                let message = ConsensusMessage::SyncRequestMsg(digest.clone(), name);
                                Self::transmit(message, &name, None, &network_filter, &committee).await.unwrap();
                            }
                        }
                    },
                    Some(result) = waiting.next() => match result {
                        Ok(digest) => {
                            debug!("consensus sync loopback");
                            let _ = pending.remove(&digest);
                            let _ = requests.remove(&digest);/////////////////?
                            // let message = ConsensusMessage::LoopBackMsg(epoch,height);
                            // if let Err(e) = core_channel.send(message).await {
                            //     panic!("Failed to send message through core channel: {}", e);
                            // }
                        },
                        Err(e) => error!("{}", e)
                    },
                    () = &mut timer => {
                        // This implements the 'perfect point to point link' abstraction.
                        for (digest, timestamp) in &requests {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Failed to measure time")
                                .as_millis();
                            if timestamp + (sync_retry_delay as u128) < now {
                                debug!("Requesting sync for block digest {}", digest);
                                let message = ConsensusMessage::SyncRequestMsg(digest.clone(), name);///////////////?
                                Self::transmit(message, &name, None, &network_filter, &committee).await.unwrap();
                            }
                        }
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_ACCURACY));
                    },
                    else => break,
                }
            }
        });
        Self {
            store,
            inner_channel: tx_inner,
        }
    }

    async fn waiter(mut store: Store, digest: Digest) -> ConsensusResult<Digest> {
        let _ = store.notify_read(digest.to_vec()).await?;
        Ok(digest)
    }

    pub async fn transmit(
        message: ConsensusMessage,
        from: &PublicKey,
        to: Option<&PublicKey>,
        network_filter: &Sender<FilterInput>,
        committee: &Committee,
    ) -> ConsensusResult<()> {
        let addresses = if let Some(to) = to {
            debug!("Sending {:?} to {}", message, to);
            vec![committee.address(to)?]
        } else {
            debug!("Broadcasting {:?}", message);
            committee.broadcast_addresses(from)
        };
        if let Err(e) = network_filter.send((message, addresses)).await {
            panic!("Failed to send block through network channel: {}", e);
        }
        Ok(())
    }

    pub async fn block_request(&mut self, digest: Digest) -> ConsensusResult<()> {
        //如果没有 向其他节点发送request
        info!("block request digest {}", digest);
        if let Err(e) = self.inner_channel.send(digest).await {
            panic!("Failed to send request to synchronizer: {}", e);
        }
        Ok(())
    }
}
