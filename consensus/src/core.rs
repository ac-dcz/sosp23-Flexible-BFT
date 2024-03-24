use crate::aggregator::Aggregator;
use crate::commitor::Commitor;
use crate::config::{Committee, Parameters, Stake};
use crate::error::{ConsensusError, ConsensusResult};
use crate::filter::FilterInput;
use crate::mempool::MempoolDriver;
use crate::messages::{
    ABAOutput, ABAVal, Block, EndorseVote, PassMechanism, PromiseVote, Proposal, RandomnessShare,
};
use crate::synchronizer::Synchronizer;
use async_recursion::async_recursion;
use crypto::{Digest, Hash, PublicKey, SignatureService};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::time::{SystemTime, UNIX_EPOCH};
use store::Store;
use threshold_crypto::PublicKeySet;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};
#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

pub type SeqNumber = u64; // For both round and view
pub type HeightNumber = u8; // height={1,2} in fallback chain, height=0 for sync block

pub const ENDORSED: u8 = 0;
pub const ABORT: u8 = 1;

pub const VAL_PHASE: u8 = 0;
pub const MUX_PHASE: u8 = 1;
pub const PROM_PHASE: u8 = 2;

pub const OPT: u8 = 0;
pub const PES: u8 = 1;

#[derive(Serialize, Deserialize, Debug)]
pub enum ConsensusMessage {
    ProposalMsg(Proposal),
    EndorseMsg(EndorseVote),
    PromiseMsg(PromiseVote),
    SuperMVDelayMsg(Proposal, u128),
    ABAValMsg(ABAVal),
    ABAMuxMsg(ABAVal),
    ABAPromMsg(ABAVal),
    ABACoinShareMsg(RandomnessShare),
    ABAOutputMsg(ABAOutput),
    NotifyPassMsg(u128),
    PassMsg(PassMechanism),
    LoopBackMsg(Block),
    SyncRequestMsg(u128, SeqNumber, PublicKey),
    SyncReplyMsg(Block),
}

pub struct Core {
    name: PublicKey,
    committee: Committee,
    parameters: Parameters,
    store: Store,
    signature_service: SignatureService,
    pk_set: PublicKeySet,
    mempool_driver: MempoolDriver,
    synchronizer: Synchronizer,
    commitor: Commitor,
    rx_clean: Receiver<(Vec<Digest>, u128, SeqNumber)>,
    tx_core: Sender<ConsensusMessage>,
    rx_core: Receiver<ConsensusMessage>,
    network_filter: Sender<FilterInput>,
    _commit_channel: Sender<Block>,
    height: SeqNumber,
    pass_ts: u128,
    all_pass_ts: Vec<u128>,
    aggregator: Aggregator,
    arbc_proposal: HashMap<(u128, SeqNumber), Digest>,
    arbc_endorse: HashSet<(u128, SeqNumber)>,
    arbc_instance: HashSet<(u128, SeqNumber)>,
    arbc_output_flag: HashSet<(u128, SeqNumber)>,
    aba_invoke_flags: HashSet<(u128, SeqNumber)>,
    aba_values: HashMap<(u128, SeqNumber, SeqNumber), [HashSet<PublicKey>; 2]>,
    aba_values_flag: HashMap<(u128, SeqNumber, SeqNumber), [bool; 2]>,
    aba_mux_values: HashMap<(u128, SeqNumber, SeqNumber), [HashSet<PublicKey>; 2]>,
    aba_mux_flags: HashMap<(u128, SeqNumber, SeqNumber), [bool; 2]>,
    aba_prom_values: HashMap<(u128, SeqNumber, SeqNumber), [HashSet<PublicKey>; 2]>,
    aba_share_flags: HashSet<(u128, SeqNumber, SeqNumber)>,
    aba_outputs: HashMap<(u128, SeqNumber, SeqNumber), HashSet<PublicKey>>,
    aba_ends: HashMap<(u128, SeqNumber), u8>,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        signature_service: SignatureService,
        pk_set: PublicKeySet,
        store: Store,
        mempool_driver: MempoolDriver,
        synchronizer: Synchronizer,
        tx_core: Sender<ConsensusMessage>,
        rx_core: Receiver<ConsensusMessage>,
        network_filter: Sender<FilterInput>,
        commit_channel: Sender<Block>,
    ) -> Self {
        let (tx_clean, rx_clean): (_, Receiver<(Vec<Digest>, u128, SeqNumber)>) = channel(1000);
        let commitor = Commitor::new(tx_core.clone(), committee.clone(), store.clone(), tx_clean);
        let aggregator = Aggregator::new(name, committee.clone());
        let mut all_pass_ts = Vec::new();
        for _ in 0..committee.size() {
            all_pass_ts.push(0);
        }
        Self {
            all_pass_ts,
            pass_ts: 0,
            height: committee.id(name) as u64,
            name,
            committee,
            parameters,
            signature_service,
            pk_set,
            store,
            mempool_driver,
            synchronizer,
            network_filter,
            _commit_channel: commit_channel,
            commitor,
            rx_clean,
            tx_core,
            rx_core,
            aggregator,
            arbc_proposal: HashMap::new(),
            arbc_endorse: HashSet::new(),
            arbc_output_flag: HashSet::new(),
            arbc_instance: HashSet::new(),
            aba_invoke_flags: HashSet::new(),
            aba_values: HashMap::new(),
            aba_mux_values: HashMap::new(),
            aba_values_flag: HashMap::new(),
            aba_mux_flags: HashMap::new(),
            aba_prom_values: HashMap::new(),
            aba_share_flags: HashSet::new(),
            aba_outputs: HashMap::new(),
            aba_ends: HashMap::new(),
        }
    }

    async fn delay_super_mv(proposal: Proposal, timeout: u128) -> Proposal {
        sleep(Duration::from_millis(timeout as u64)).await;
        proposal
    }

    fn get_local_time() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("local time error")
            .as_millis()
    }

    async fn store_block(&mut self, block: &Block) {
        self.arbc_proposal
            .insert((block.timestamp, block.height), block.digest());
        let key = block.digest().to_vec();
        let value = bincode::serialize(block).expect("Failed to serialize block");
        self.store.write(key, value).await;
    }

    async fn cleanup(
        &mut self,
        digest: Vec<Digest>,
        timestamp: u128,
        height: SeqNumber,
    ) -> ConsensusResult<()> {
        self.aggregator.cleanup(timestamp, height);
        self.mempool_driver.cleanup(digest, timestamp, height).await;
        self.arbc_proposal
            .retain(|(ts, h), _| *ts > timestamp || (*ts == timestamp && *h > height));
        // self.arbc_instance
        //     .retain(|(ts, h)| *ts > timestamp || (*ts == timestamp && *h > height));
        self.arbc_output_flag
            .retain(|(ts, h)| *ts > timestamp || (*ts == timestamp && *h > height));
        self.aba_values
            .retain(|(ts, h, _), _| *ts > timestamp || (*ts == timestamp && *h > height));
        self.aba_values_flag
            .retain(|(ts, h, _), _| *ts > timestamp || (*ts == timestamp && *h > height));
        self.aba_mux_values
            .retain(|(ts, h, _), _| *ts > timestamp || (*ts == timestamp && *h > height));
        self.aba_mux_flags
            .retain(|(ts, h, _), _| *ts > timestamp || (*ts == timestamp && *h > height));
        self.aba_prom_values
            .retain(|(ts, h, _), _| *ts > timestamp || (*ts == timestamp && *h > height));
        self.aba_share_flags
            .retain(|(ts, h, _)| *ts > timestamp || (*ts == timestamp && *h > height));
        self.aba_outputs
            .retain(|(ts, h, _), _| *ts > timestamp || (*ts == timestamp && *h > height));
        Ok(())
    }

    /************* fast negotiation Protocol ******************/
    #[async_recursion]
    async fn generate_arbc_proposal(&mut self) -> ConsensusResult<Block> {
        // Make a new block.
        let payload = self
            .mempool_driver
            .get(self.parameters.max_payload_size)
            .await;
        let block = Block::new(
            self.name,
            self.height,
            Self::get_local_time(),
            payload,
            self.signature_service.clone(),
        )
        .await;
        if !block.payload.is_empty() {
            info!("Created {}", block);

            #[cfg(feature = "benchmark")]
            for x in &block.payload {
                // NOTE: This log entry is used to compute performance.
                info!(
                    "Created B{}({}) timestamp {}",
                    block.height,
                    base64::encode(x),
                    block.timestamp
                );
            }
        }
        debug!("Created {:?}", block);
        let proposal = Proposal::new(block, self.signature_service.clone()).await;
        // Process our new block and broadcast it.
        let message = ConsensusMessage::ProposalMsg(proposal.clone());
        Synchronizer::transmit(
            message,
            &self.name,
            None,
            &self.network_filter,
            &self.committee,
        )
        .await?;
        self.handle_negotiation_proposal(&proposal).await?;

        // Wait for the minimum block delay.
        sleep(Duration::from_millis(self.parameters.min_block_delay)).await;

        Ok(Block::default())
    }

    async fn handle_negotiation_proposal(&mut self, proposal: &Proposal) -> ConsensusResult<()> {
        //vaild
        debug!("Processing {}", proposal);
        proposal.verify(&self.committee)?;
        if !self.is_vaild(proposal).await {
            return Ok(());
        }
        let digest = proposal.block.digest();
        self.store_block(&proposal.block).await;
        if self
            .arbc_instance
            .insert((proposal.timestamp, proposal.height))
        {
            // instance uodate
            self.commitor
                .add_instance(proposal.timestamp, proposal.height)
                .await?;
        }
        // endorse
        if proposal.timestamp > self.pass_ts {
            if let Some(vote) = self
                .make_endorse_vote(proposal.timestamp, proposal.height, digest, ENDORSED)
                .await
            {
                self.arbc_endorse
                    .insert((proposal.timestamp, proposal.height));
                let message = ConsensusMessage::EndorseMsg(vote.clone());
                Synchronizer::transmit(
                    message,
                    &self.name,
                    None,
                    &self.network_filter,
                    &self.committee,
                )
                .await?;
                self.handle_negotiation_endorse(&vote).await?;
            }
        }

        //如果返回 False 无特殊处理
        let _ = self.mempool_driver.verify(proposal.block.clone()).await?;

        Ok(())
    }

    async fn is_vaild(&mut self, proposal: &Proposal) -> bool {
        let local = Self::get_local_time();
        if local < proposal.timestamp {
            let message =
                ConsensusMessage::SuperMVDelayMsg(proposal.clone(), proposal.timestamp - local);
            if let Err(e) = self.tx_core.send(message).await {
                panic!("Failed to send ConsensusMessage to core: {}", e);
            }
            return false;
        }
        return true;
    }

    async fn make_endorse_vote(
        &self,
        timestamp: u128,
        height: SeqNumber,
        digest: Digest,
        signal: u8,
    ) -> Option<EndorseVote> {
        if self.aba_invoke_flags.contains(&(timestamp, height)) {
            return None;
        }
        Some(
            EndorseVote::new(
                self.name,
                height,
                timestamp,
                digest,
                signal,
                self.signature_service.clone(),
            )
            .await,
        )
    }

    async fn handle_negotiation_endorse(&mut self, vote: &EndorseVote) -> ConsensusResult<()> {
        debug!("Processing {}", vote);
        vote.verify(&self.committee)?;
        self.arbc_instance.insert((vote.timestamp, vote.height));
        if let Some((flag, val)) = self.aggregator.add_arbc_endorse_vote(vote)? {
            if flag {
                let promise = PromiseVote::new(
                    self.name,
                    vote.height,
                    vote.timestamp,
                    vote.digest.clone(),
                    vote.signal,
                    self.signature_service.clone(),
                )
                .await;
                let message = ConsensusMessage::PromiseMsg(promise.clone());
                Synchronizer::transmit(
                    message,
                    &self.name,
                    None,
                    &self.network_filter,
                    &self.committee,
                )
                .await?;
            }
            self.invoke_aba(vote.timestamp, vote.height, val).await?;
        }
        Ok(())
    }

    async fn handle_negotiation_promise(&mut self, vote: &PromiseVote) -> ConsensusResult<()> {
        debug!("Processing {}", vote);
        vote.verify(&self.committee)?;
        self.arbc_instance.insert((vote.timestamp, vote.height));
        if let Some(signal) = self.aggregator.add_arbc_promise_vote(vote)? {
            //end
            self.process_negotiation_output(vote.timestamp, vote.height, signal)
                .await?;
        }
        Ok(())
    }

    async fn process_negotiation_output(
        &mut self,
        timestamp: u128,
        height: SeqNumber,
        tag: u8,
    ) -> ConsensusResult<()> {
        debug!(
            "processing ARBC output timestamp {} height {} tag {}",
            timestamp, height, tag
        );
        if !self.arbc_output_flag.contains(&(timestamp, height)) {
            if tag == PES {
                self.arbc_output_flag.insert((timestamp, height));
                self.commitor.add_output(timestamp, height, None).await?;
            } else {
                if self.arbc_proposal.contains_key(&(timestamp, height)) {
                    self.arbc_output_flag.insert((timestamp, height));
                    let digest = self
                        .arbc_proposal
                        .entry((timestamp, height))
                        .or_insert(Digest::default())
                        .clone();
                    self.commitor
                        .add_output(timestamp, height, Some(digest))
                        .await?;

                    //send
                    if height == self.height {
                        self.generate_arbc_proposal().await?;
                    }
                } else {
                    self.synchronizer.block_request(timestamp, height).await?;
                }
            }
        }
        Ok(())
    }

    async fn handle_negotiation_loopback(&mut self, _block: &Block) -> ConsensusResult<()> {
        Ok(())
    }
    /************* fast negotiation Protocol ******************/

    /************* FastBA Protocol ******************/
    async fn invoke_aba(
        &mut self,
        timestamp: u128,
        height: SeqNumber,
        val: u8,
    ) -> ConsensusResult<()> {
        if self.aba_invoke_flags.insert((timestamp, height)) {
            let aba_val = ABAVal::new(
                self.name,
                timestamp,
                height,
                0,
                val as usize,
                VAL_PHASE,
                self.signature_service.clone(),
            )
            .await;
            let message = ConsensusMessage::ABAValMsg(aba_val.clone());
            Synchronizer::transmit(
                message,
                &self.name,
                None,
                &self.network_filter,
                &self.committee,
            )
            .await?;
            self.handle_aba_val(&aba_val).await?;
        }
        Ok(())
    }

    #[async_recursion]
    async fn handle_aba_val(&mut self, aba_val: &ABAVal) -> ConsensusResult<()> {
        debug!(
            "processing aba val timestamp {} height {}",
            aba_val.timestamp, aba_val.height
        );

        aba_val.verify()?;

        let values = self
            .aba_values
            .entry((aba_val.timestamp, aba_val.height, aba_val.round))
            .or_insert([HashSet::new(), HashSet::new()]);

        if values[aba_val.val].insert(aba_val.author) {
            let mut nums = values[aba_val.val].len() as Stake;
            if nums == self.committee.random_coin_threshold()
                && !values[aba_val.val].contains(&self.name)
            {
                //f+1
                let other = ABAVal::new(
                    self.name,
                    aba_val.timestamp,
                    aba_val.height,
                    aba_val.round,
                    aba_val.val,
                    VAL_PHASE,
                    self.signature_service.clone(),
                )
                .await;
                let message = ConsensusMessage::ABAValMsg(other);
                Synchronizer::transmit(
                    message,
                    &self.name,
                    None,
                    &self.network_filter,
                    &self.committee,
                )
                .await?;
                values[aba_val.val].insert(self.name);
                nums += 1;
            }

            if nums == self.committee.quorum_threshold() {
                let values_flag = self
                    .aba_values_flag
                    .entry((aba_val.timestamp, aba_val.height, aba_val.round))
                    .or_insert([false, false]);

                if !values_flag[0] && !values_flag[1] {
                    values_flag[aba_val.val] = true;
                    if !values[1 - aba_val.val].contains(&self.name) {
                        let prom = ABAVal::new(
                            self.name,
                            aba_val.timestamp,
                            aba_val.height,
                            aba_val.round,
                            aba_val.val,
                            PROM_PHASE,
                            self.signature_service.clone(),
                        )
                        .await;
                        let message = ConsensusMessage::ABAPromMsg(prom.clone());
                        Synchronizer::transmit(
                            message,
                            &self.name,
                            None,
                            &self.network_filter,
                            &self.committee,
                        )
                        .await?;
                        self.handle_aba_prom(&prom).await?;
                    } else {
                        let mux = ABAVal::new(
                            self.name,
                            aba_val.timestamp,
                            aba_val.height,
                            aba_val.round,
                            aba_val.val,
                            MUX_PHASE,
                            self.signature_service.clone(),
                        )
                        .await;
                        let message = ConsensusMessage::ABAMuxMsg(mux.clone());
                        Synchronizer::transmit(
                            message,
                            &self.name,
                            None,
                            &self.network_filter,
                            &self.committee,
                        )
                        .await?;
                        self.handle_aba_mux(&mux).await?;
                    }
                } else {
                    values_flag[aba_val.val] = true;
                }
            }
        }
        Ok(())
    }

    async fn handle_aba_mux(&mut self, aba_mux: &ABAVal) -> ConsensusResult<()> {
        debug!(
            "processing aba mux tiemstamp {} height {}",
            aba_mux.timestamp, aba_mux.height
        );
        aba_mux.verify()?;
        let values = self
            .aba_mux_values
            .entry((aba_mux.timestamp, aba_mux.height, aba_mux.round))
            .or_insert([HashSet::new(), HashSet::new()]);
        if values[aba_mux.val].insert(aba_mux.author) {
            let mux_flags = self
                .aba_mux_flags
                .entry((aba_mux.timestamp, aba_mux.height, aba_mux.round))
                .or_insert([false, false]);

            if !mux_flags[0] && !mux_flags[1] {
                let nums_opt = values[OPT as usize].len();
                let nums_pes = values[PES as usize].len();
                if nums_opt + nums_pes >= self.committee.quorum_threshold() as usize {
                    let value_flags = self
                        .aba_values_flag
                        .entry((aba_mux.timestamp, aba_mux.height, aba_mux.round))
                        .or_insert([false, false]);
                    if value_flags[0] && value_flags[1] {
                        mux_flags[0] = nums_opt > 0;
                        mux_flags[1] = nums_pes > 1;
                    } else if value_flags[0] {
                        mux_flags[0] = nums_opt >= self.committee.quorum_threshold() as usize;
                    } else {
                        mux_flags[1] = nums_pes >= self.committee.quorum_threshold() as usize;
                    }
                }

                if mux_flags[0] || mux_flags[1] {
                    self.invoke_random_share_coin(aba_mux.timestamp, aba_mux.height, aba_mux.round)
                        .await?;
                }
            }
        }

        Ok(())
    }

    async fn handle_aba_prom(&mut self, aba_prom: &ABAVal) -> ConsensusResult<()> {
        debug!(
            "processing aba prom tiemstamp {} height {}",
            aba_prom.timestamp, aba_prom.height
        );
        aba_prom.verify()?;
        let values = self
            .aba_prom_values
            .entry((aba_prom.timestamp, aba_prom.height, aba_prom.round))
            .or_insert([HashSet::new(), HashSet::new()]);
        if values[aba_prom.val].insert(aba_prom.author) {
            let mux_flags = self
                .aba_mux_flags
                .entry((aba_prom.timestamp, aba_prom.height, aba_prom.round))
                .or_insert([false, false]);

            if !mux_flags[0] && !mux_flags[1] {
                let nums_opt = values[OPT as usize].len();
                let nums_pes = values[PES as usize].len();
                if nums_opt + nums_pes >= self.committee.quorum_threshold() as usize {
                    let value_flags = self
                        .aba_values_flag
                        .entry((aba_prom.timestamp, aba_prom.height, aba_prom.round))
                        .or_insert([false, false]);
                    if value_flags[0] && value_flags[1] {
                        mux_flags[0] = nums_opt > 0;
                        mux_flags[1] = nums_pes > 1;
                    } else if value_flags[0] {
                        mux_flags[0] = nums_opt >= self.committee.quorum_threshold() as usize;
                    } else {
                        mux_flags[1] = nums_pes >= self.committee.quorum_threshold() as usize;
                    }
                }
                if mux_flags[0] && mux_flags[1] {
                    self.invoke_random_share_coin(
                        aba_prom.timestamp,
                        aba_prom.height,
                        aba_prom.round,
                    )
                    .await?;
                } else if mux_flags[0] || mux_flags[1] {
                    self.process_aba_output(
                        aba_prom.timestamp,
                        aba_prom.height,
                        aba_prom.round,
                        aba_prom.val,
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    async fn invoke_random_share_coin(
        &mut self,
        timestamp: u128,
        height: SeqNumber,
        round: SeqNumber,
    ) -> ConsensusResult<()> {
        if self.aba_share_flags.insert((timestamp, height, round)) {
            let share = RandomnessShare::new(
                timestamp,
                height,
                round,
                self.name,
                self.signature_service.clone(),
            )
            .await;
            let message = ConsensusMessage::ABACoinShareMsg(share.clone());
            Synchronizer::transmit(
                message,
                &self.name,
                None,
                &self.network_filter,
                &self.committee,
            )
            .await?;
            self.handle_aba_share(&share).await?;
        }
        Ok(())
    }

    async fn handle_aba_share(&mut self, share: &RandomnessShare) -> ConsensusResult<()> {
        debug!("processing {:?}", share);
        share.verify(&self.committee, &self.pk_set)?;
        if let Some(coin) = self.aggregator.add_aba_share_coin(share, &self.pk_set)? {
            let mux_flags = self
                .aba_mux_flags
                .entry((share.timestamp, share.height, share.round))
                .or_insert([false, false]);
            let mut val = coin;
            if mux_flags[coin] && !mux_flags[coin ^ 1] {
                self.process_aba_output(share.timestamp, share.height, share.round, coin)
                    .await?;
            } else if !mux_flags[coin] && mux_flags[coin ^ 1] {
                val = coin ^ 1;
            }
            self.aba_adcance_round(share.timestamp, share.height, share.round + 1, val)
                .await?;
        }
        Ok(())
    }

    async fn handle_aba_output(&mut self, output: &ABAOutput) -> ConsensusResult<()> {
        debug!(
            "processing aba output timestamp {} height {}",
            output.timestamp, output.height
        );
        output.verify()?;
        let used = self
            .aba_outputs
            .entry((output.timestamp, output.height, output.round))
            .or_insert(HashSet::new());
        if used.insert(output.author)
            && used.len() == self.committee.random_coin_threshold() as usize
        {
            if !used.contains(&self.name) {
                let output = ABAOutput::new(
                    self.name,
                    output.timestamp,
                    output.height,
                    output.round,
                    output.val,
                    self.signature_service.clone(),
                )
                .await;
                let message = ConsensusMessage::ABAOutputMsg(output);
                Synchronizer::transmit(
                    message,
                    &self.name,
                    None,
                    &self.network_filter,
                    &self.committee,
                )
                .await?;
                used.insert(self.name);
            }
            self.process_aba_output(output.timestamp, output.height, output.round, output.val)
                .await?;
        }

        Ok(())
    }

    async fn process_aba_output(
        &mut self,
        timestamp: u128,
        height: SeqNumber,
        round: SeqNumber,
        val: usize,
    ) -> ConsensusResult<()> {
        debug!(
            "ABA(timestamp {} height {}) end output({})",
            timestamp, height, val
        );

        if !self.aba_ends.contains_key(&(timestamp, height)) {
            //step1. send output message
            let used = self
                .aba_outputs
                .entry((timestamp, height, round))
                .or_insert(HashSet::new());
            if used.insert(self.name) {
                let output = ABAOutput::new(
                    self.name,
                    timestamp,
                    height,
                    round,
                    val,
                    self.signature_service.clone(),
                )
                .await;
                let message = ConsensusMessage::ABAOutputMsg(output);
                Synchronizer::transmit(
                    message,
                    &self.name,
                    None,
                    &self.network_filter,
                    &self.committee,
                )
                .await?;
            }
            self.process_negotiation_output(timestamp, height, val as u8)
                .await?;
        }

        Ok(())
    }

    async fn aba_adcance_round(
        &mut self,
        timestamp: u128,
        height: SeqNumber,
        round: SeqNumber,
        val: usize,
    ) -> ConsensusResult<()> {
        if !self.aba_ends.contains_key(&(timestamp, height)) {
            let aba_val = ABAVal::new(
                self.name,
                timestamp,
                height,
                round,
                val,
                VAL_PHASE,
                self.signature_service.clone(),
            )
            .await;
            let message = ConsensusMessage::ABAValMsg(aba_val.clone());
            Synchronizer::transmit(
                message,
                &self.name,
                None,
                &self.network_filter,
                &self.committee,
            )
            .await?;
            self.handle_aba_val(&aba_val).await?;
        }
        Ok(())
    }
    /************* FastBA Protocol ******************/

    /***************PASS mechanism********************/
    async fn handle_notify_pass(&mut self, ts: u128) -> ConsensusResult<()> {
        debug!("notify pass timestamp {}", ts);
        // 1. update pass_ts
        self.pass_ts = ts;
        let mut endorse = Vec::new();
        for (ts, h) in &self.arbc_endorse {
            endorse.push((*ts, *h))
        }
        self.arbc_endorse.clear();

        let pass = PassMechanism::new(
            self.name,
            self.height,
            ts,
            endorse,
            self.signature_service.clone(),
        )
        .await;

        let message = ConsensusMessage::PassMsg(pass.clone());

        Synchronizer::transmit(
            message,
            &self.name,
            None,
            &self.network_filter,
            &self.committee,
        )
        .await?;
        self.handle_pass_mechanism(&pass).await?;
        Ok(())
    }

    async fn handle_pass_mechanism(&mut self, pass: &PassMechanism) -> ConsensusResult<()> {
        debug!("processing {:?}", pass);
        pass.verify(&self.committee)?;

        for (ts, h) in &pass.endorse {
            if self.arbc_instance.insert((*ts, *h)) {
                self.commitor.add_instance(*ts, *h).await?;
            }
        }
        self.all_pass_ts[pass.height as usize] = pass.timestamp;

        let mut temp = self.all_pass_ts.clone();
        temp.sort();
        let index = (self.committee.random_coin_threshold() - 1) as usize;
        let exec = temp[index];
        // 更新 exec_ts
        self.commitor.update_exec(exec).await?;

        for (ts, h) in self.arbc_instance.clone() {
            if ts <= exec {
                if let Some(vote) = self
                    .make_endorse_vote(ts, h, Digest::default(), ABORT)
                    .await
                {
                    let message = ConsensusMessage::EndorseMsg(vote.clone());
                    Synchronizer::transmit(
                        message,
                        &self.name,
                        None,
                        &self.network_filter,
                        &self.committee,
                    )
                    .await?;
                    self.handle_negotiation_endorse(&vote).await?;
                }
            }
        }
        Ok(())
    }
    /***************PASS mechanism********************/

    async fn handle_sync_request(
        &mut self,
        timestamp: u128,
        height: SeqNumber,
        sender: PublicKey,
    ) -> ConsensusResult<()> {
        debug!(
            "processing sync request timestamp {} height {}",
            timestamp, height
        );
        if let Some(digest) = self.arbc_proposal.get(&(timestamp, height)) {
            let key = digest.to_vec();
            if let Some(bytes) = self.store.read(key).await? {
                let block = bincode::deserialize(&bytes)?;
                let message = ConsensusMessage::SyncReplyMsg(block);
                Synchronizer::transmit(
                    message,
                    &self.name,
                    Some(&sender),
                    &self.network_filter,
                    &self.committee,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn handle_sync_reply(&mut self, block: &Block) -> ConsensusResult<()> {
        debug!(
            "processing sync reply timestamp {} height {}",
            block.timestamp, block.height
        );
        block.verify(&self.committee)?;
        self.store_block(block).await;
        self.process_negotiation_output(block.timestamp, block.height, OPT)
            .await?;
        Ok(())
    }

    pub async fn run(&mut self) {
        if let Err(e) = self.generate_arbc_proposal().await {
            panic!("protocol invoke failed! error {}", e);
        }
        let mut pending_super_mv = FuturesUnordered::new();
        loop {
            let result = tokio::select! {
                Some(message) = self.rx_core.recv() => {
                    match message {
                        ConsensusMessage::ProposalMsg(proposal)=>self.handle_negotiation_proposal(&proposal).await,
                        ConsensusMessage::EndorseMsg(endorse)=>self.handle_negotiation_endorse(&endorse).await,
                        ConsensusMessage::PromiseMsg(promise)=>self.handle_negotiation_promise(&promise).await,
                        ConsensusMessage::ABAValMsg(val)=>self.handle_aba_val(&val).await,
                        ConsensusMessage::ABAMuxMsg(mux)=> self.handle_aba_mux(&mux).await,
                        ConsensusMessage::ABAPromMsg(prom)=>self.handle_aba_prom(&prom).await,
                        ConsensusMessage::ABACoinShareMsg(share)=>self.handle_aba_share(&share).await,
                        ConsensusMessage::ABAOutputMsg(output)=>self.handle_aba_output(&output).await,
                        ConsensusMessage::LoopBackMsg(block) => self.handle_negotiation_loopback(&block).await,
                        ConsensusMessage::SyncRequestMsg(ts,height, sender) => self.handle_sync_request(ts,height, sender).await,
                        ConsensusMessage::SyncReplyMsg(block) => self.handle_sync_reply(&block).await,
                        ConsensusMessage::NotifyPassMsg(ts)=> self.handle_notify_pass(ts).await,
                        ConsensusMessage::PassMsg(pass)=>self.handle_pass_mechanism(&pass).await,
                        ConsensusMessage::SuperMVDelayMsg(proposal,timeout)=>{
                            pending_super_mv.push(Self::delay_super_mv(proposal, timeout));
                            Ok(())
                        }

                    }
                },
                Some(proposal) = pending_super_mv.next()=>{
                    self.handle_negotiation_proposal(&proposal).await
                },
                Some((digests,ts,g)) = self.rx_clean.recv()=>{
                    self.cleanup(digests,ts,g).await
                },
                else => break,
            };
            match result {
                Ok(()) => (),
                Err(ConsensusError::StoreError(e)) => error!("{}", e),
                Err(ConsensusError::SerializationError(e)) => error!("Store corrupted. {}", e),
                Err(e) => warn!("{}", e),
            }
        }
    }
}
