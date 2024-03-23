use crate::config::Committee;
use crate::core::ConsensusMessage;
use crate::error::ConsensusResult;
use crate::filter::FilterInput;
use crate::SeqNumber;
use crypto::PublicKey;
use log::{debug, info};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[cfg(test)]
#[path = "tests/synchronizer_tests.rs"]
pub mod synchronizer_tests;

// const TIMER_ACCURACY: u64 = 5_000;

pub struct Synchronizer {
    _store: Store,
    inner_channel: Sender<(u128, SeqNumber)>,
}

impl Synchronizer {
    pub async fn new(
        name: PublicKey,
        committee: Committee,
        store: Store,
        network_filter: Sender<FilterInput>,
        _core_channel: Sender<ConsensusMessage>,
        _sync_retry_delay: u64,
    ) -> Self {
        let (tx_inner, mut rx_inner): (_, Receiver<(u128, SeqNumber)>) = channel(10000);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some((ts,height)) = rx_inner.recv() => {
                        let message = ConsensusMessage::SyncRequestMsg(ts,height, name);
                        Self::transmit(message, &name, None, &network_filter, &committee).await.unwrap();
                    },
                    else => break,
                }
            }
        });
        Self {
            _store: store,
            inner_channel: tx_inner,
        }
    }

    // async fn waiter(mut store: Store, digest: Digest) -> ConsensusResult<Digest> {
    //     let _ = store.notify_read(digest.to_vec()).await?;
    //     Ok(digest)
    // }

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

    pub async fn block_request(&mut self, ts: u128, height: SeqNumber) -> ConsensusResult<()> {
        //如果没有 向其他节点发送request
        info!("block request timestamp {} height {}", ts, height);
        if let Err(e) = self.inner_channel.send((ts, height)).await {
            panic!("Failed to send request to synchronizer: {}", e);
        }
        Ok(())
    }
}
