use crate::aggregator::Aggregator;
use crate::config::{Committee, Parameters, Stake};
use crate::error::{ConsensusError, ConsensusResult};
use crate::filter::FilterInput;
use crate::mempool::MempoolDriver;
use crate::messages::{
    self, ABAOutput, ABAVal, Block, EndorseVote, PromiseVote, Proposal, RandomnessShare,
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
use tokio::sync::mpsc::{Receiver, Sender};
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
    LoopBackMsg(Block),
    SyncRequestMsg(Digest, PublicKey),
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
    tx_core: Sender<ConsensusMessage>,
    rx_core: Receiver<ConsensusMessage>,
    network_filter: Sender<FilterInput>,
    _commit_channel: Sender<Block>,
    height: SeqNumber,
    pass_ts: u128,
    aggregator: Aggregator,
    arbc_proposal: HashMap<(u128, SeqNumber), Digest>,
    arbc_endorse: HashSet<(u128, SeqNumber)>,
    aba_invoke_flags: HashSet<(u128, SeqNumber)>,
    aba_values: HashMap<(u128, SeqNumber, SeqNumber), [HashSet<PublicKey>; 2]>,
    aba_values_flag: HashMap<(u128, SeqNumber, SeqNumber), [bool; 2]>,
    aba_mux_values: HashMap<(u128, SeqNumber, SeqNumber), [HashSet<PublicKey>; 2]>,
    aba_mux_flags: HashMap<(u128, SeqNumber, SeqNumber), [bool; 2]>,
    aba_outputs: HashMap<(u128, SeqNumber, SeqNumber), HashSet<PublicKey>>,
    aba_ends: HashMap<(u128, SeqNumber), u8>,
    aba_ends_nums: HashMap<(u128, u8), HashSet<SeqNumber>>,
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
        let aggregator = Aggregator::new(name, committee.clone());
        Self {
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
            tx_core,
            rx_core,
            aggregator,
            arbc_proposal: HashMap::new(),
            arbc_endorse: HashSet::new(),
            aba_invoke_flags: HashSet::new(),
            aba_values: HashMap::new(),
            aba_mux_values: HashMap::new(),
            aba_values_flag: HashMap::new(),
            aba_mux_flags: HashMap::new(),
            aba_outputs: HashMap::new(),
            aba_ends: HashMap::new(),
            aba_ends_nums: HashMap::new(),
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
        let key = block.digest().to_vec();
        let value = bincode::serialize(block).expect("Failed to serialize block");
        self.store.write(key, value).await;
    }

    async fn commit(&mut self, _blocks: Vec<Block>) -> ConsensusResult<()> {
        // if blocks.len() > 0 {
        //     let epoch = blocks[0].epoch;
        //     let mut digest: Vec<Digest> = Vec::new();
        //     for block in blocks {
        //         if !block.payload.is_empty() {
        //             info!("Committed {}", block);

        //             #[cfg(feature = "benchmark")]
        //             for x in &block.payload {
        //                 info!(
        //                     "Committed B{}({}) epoch {}",
        //                     block.height,
        //                     base64::encode(x),
        //                     block.epoch,
        //                 );
        //             }
        //         }
        //         digest.append(&mut block.payload.clone());
        //         debug!("Committed {}", block);
        //     }
        //     self.cleanup(digest, epoch).await?;
        // }
        Ok(())
    }

    async fn cleanup(
        &mut self,
        digest: Vec<Digest>,
        timestamp: u128,
        height: SeqNumber,
    ) -> ConsensusResult<()> {
        self.aggregator.cleanup(timestamp, height);
        self.mempool_driver.cleanup(digest, timestamp, height).await;
        // self.aba_values.retain(|(e, ..), _| *e > epoch);
        // self.aba_mux_values.retain(|(e, ..), _| *e > epoch);
        // self.aba_values_flag.retain(|(e, ..), _| *e > epoch);
        // self.aba_mux_flags.retain(|(e, ..), _| *e > epoch);

        Ok(())
    }

    /************* fast negotiation Protocol ******************/
    #[async_recursion]
    async fn generate_rbc_proposal(&mut self) -> ConsensusResult<Block> {
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
                    block.tiemstamp
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
        self.arbc_proposal
            .insert((proposal.timestamp, proposal.height), digest.clone());

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
        &mut self,
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
        if let Some(signal) = self.aggregator.add_arbc_promise_vote(vote)? {
            //end
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
                    let share = RandomnessShare::new(
                        aba_mux.timestamp,
                        aba_mux.height,
                        aba_mux.round,
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
            }
        }

        Ok(())
    }

    async fn handle_aba_prom(&mut self, aba_prom: &ABAVal) -> ConsensusResult<()> {
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
            "ABA(epoch {} height {}) end output({})",
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
            info!("ABA(epoch {},height {}) ouput {}", timestamp, height, val);
            self.aba_ends.insert((timestamp, height), val as u8);

            self.aba_ends_nums
                .entry((timestamp, val as u8))
                .or_insert(HashSet::new())
                .insert(height);
            let opt_num = self
                .aba_ends_nums
                .entry((timestamp, OPT))
                .or_insert(HashSet::new())
                .len();
            let pes_num = self
                .aba_ends_nums
                .entry((timestamp, PES))
                .or_insert(HashSet::new())
                .len();

            //step2. wait 2f+1
            if opt_num as Stake == self.committee.quorum_threshold() {
                for height in 0..(self.committee.size() as SeqNumber) {
                    if !self.aba_invoke_flags.contains(&(timestamp, height)) {
                        self.invoke_aba(timestamp, height, PES).await?;
                    }
                }
            }

            //step3. receive all aba output?
            if opt_num + pes_num == self.committee.size() {
                // self.handle_epoch_end(timestamp).await?;
            }
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

    /***************PASS mechanism********************/

    async fn handle_sync_request(
        &mut self,
        digest: Digest,
        sender: PublicKey,
    ) -> ConsensusResult<()> {
        debug!("processing sync request digest {}", digest);
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
        Ok(())
    }

    async fn handle_sync_reply(&mut self, block: &Block) -> ConsensusResult<()> {
        debug!(
            "processing sync reply timestamp {} height {}",
            block.timestamp, block.height
        );
        block.verify(&self.committee)?;
        self.store_block(block).await;
        Ok(())
    }

    pub async fn handle_epoch_end(&mut self, _epoch: SeqNumber) -> ConsensusResult<()> {
        // let mut data: Vec<Block> = Vec::new();

        // for height in 0..(self.committee.size() as SeqNumber) {
        //     if *self.aba_ends.get(&(epoch, height)).unwrap() == OPT {
        //         if let Some(block) = self
        //             .synchronizer
        //             .block_request(epoch, height, &self.committee)
        //             .await?
        //         {
        //             if !self.mempool_driver.verify(block.clone()).await? {
        //                 return Ok(());
        //             }
        //             data.push(block);
        //         }
        //     }
        // }
        // self.commit(data).await?;
        // self.advance_epoch(epoch + 1).await?;
        Ok(())
    }

    pub async fn advance_epoch(&mut self, epoch: SeqNumber) -> ConsensusResult<()> {
        // if epoch > self.epoch {
        //     self.epoch = epoch;
        //     self.generate_rbc_proposal().await?;
        // }
        Ok(())
    }

    pub async fn run(&mut self) {
        if let Err(e) = self.generate_rbc_proposal().await {
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
                        ConsensusMessage::SyncRequestMsg(digest, sender) => self.handle_sync_request(digest, sender).await,
                        ConsensusMessage::SyncReplyMsg(block) => self.handle_sync_reply(&block).await,
                        ConsensusMessage::SuperMVDelayMsg(proposal,timeout)=>{
                            pending_super_mv.push(Self::delay_super_mv(proposal, timeout));
                            Ok(())
                        }

                    }
                },
                Some(proposal) = pending_super_mv.next()=>{
                    self.handle_negotiation_proposal(&proposal).await
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
