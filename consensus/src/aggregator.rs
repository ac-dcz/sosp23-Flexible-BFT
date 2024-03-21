use crate::config::{Committee, Stake};
use crate::core::{SeqNumber, ABORT};
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::{EndorseVote, PromiseVote, RandomnessShare};
use crypto::PublicKey;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryInto;
use threshold_crypto::PublicKeySet;

#[cfg(test)]
#[path = "tests/aggregator_tests.rs"]
pub mod aggregator_tests;

// In HotStuff, votes/timeouts aggregated by round
// In VABA and async fallback, votes aggregated by round, timeouts/coin_share aggregated by view
pub struct Aggregator {
    name: PublicKey,
    committee: Committee,
    //(u128,SeqNumber,SeqNumber) = (timestamp,height(proposer),round)
    share_coin_aggregators: HashMap<(u128, SeqNumber, SeqNumber), Box<RandomCoinMaker>>,
    endorse_vote_aggregators: HashMap<(u128, SeqNumber), Box<EndorseVoteMaker>>,
    promise_vote_aggregators: HashMap<(u128, SeqNumber), Box<PromiseVoteMaker>>,
}

impl Aggregator {
    pub fn new(name: PublicKey, committee: Committee) -> Self {
        Self {
            name,
            committee,
            share_coin_aggregators: HashMap::new(),
            endorse_vote_aggregators: HashMap::new(),
            promise_vote_aggregators: HashMap::new(),
        }
    }

    pub fn add_arbc_endorse_vote(
        &mut self,
        vote: &EndorseVote,
    ) -> ConsensusResult<Option<(bool, u8)>> {
        let name = self.name;
        self.endorse_vote_aggregators
            .entry((vote.timestamp, vote.height))
            .or_insert_with(|| Box::new(EndorseVoteMaker::new(name)))
            .append(vote.author, vote.signal, &self.committee)
    }

    pub fn add_arbc_promise_vote(&mut self, vote: &PromiseVote) -> ConsensusResult<Option<u8>> {
        self.promise_vote_aggregators
            .entry((vote.timestamp, vote.height))
            .or_insert_with(|| Box::new(PromiseVoteMaker::new()))
            .append(vote.author, vote.signal, &self.committee)
    }

    pub fn add_aba_share_coin(
        &mut self,
        share: &RandomnessShare,
        pk_set: &PublicKeySet,
    ) -> ConsensusResult<Option<usize>> {
        self.share_coin_aggregators
            .entry((share.timestamp, share.height, share.round))
            .or_insert_with(|| Box::new(RandomCoinMaker::new()))
            .append(share, &self.committee, pk_set)
    }

    pub fn cleanup(&mut self, timestamp: u128, height: SeqNumber) {
        self.endorse_vote_aggregators
            .retain(|(ts, h), _| *ts > timestamp || (*ts == timestamp && *h > height));
        self.share_coin_aggregators
            .retain(|(ts, h, ..), _| *ts > timestamp || (*ts == timestamp && *h > height));
    }
}

struct RandomCoinMaker {
    weight: Stake,
    shares: Vec<RandomnessShare>,
    used: HashSet<PublicKey>,
}

impl RandomCoinMaker {
    pub fn new() -> Self {
        Self {
            weight: 0,
            shares: Vec::new(),
            used: HashSet::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(
        &mut self,
        share: &RandomnessShare,
        committee: &Committee,
        pk_set: &PublicKeySet,
    ) -> ConsensusResult<Option<usize>> {
        let author = share.author;
        // Ensure it is the first time this authority votes.
        ensure!(
            self.used.insert(author),
            ConsensusError::AuthorityReuseinCoin(author)
        );
        self.shares.push(share.clone());
        self.weight += committee.stake(&author);
        if self.weight == committee.random_coin_threshold() {
            // self.weight = 0; // Ensures QC is only made once.
            let mut sigs = BTreeMap::new();
            // Check the random shares.
            for share in self.shares.clone() {
                sigs.insert(
                    committee.id(share.author.clone()),
                    share.signature_share.clone(),
                );
            }
            if let Ok(sig) = pk_set.combine_signatures(sigs.iter()) {
                let id = usize::from_be_bytes((&sig.to_bytes()[0..8]).try_into().unwrap()) % 2;

                return Ok(Some(id));
            }
        }
        Ok(None)
    }
}

struct EndorseVoteMaker {
    name: PublicKey,
    weight_endorse: Stake,
    used_endorse: HashSet<PublicKey>,
    weight_abort: Stake,
    used_abort: HashSet<PublicKey>,
}

impl EndorseVoteMaker {
    pub fn new(name: PublicKey) -> Self {
        Self {
            name,
            weight_endorse: 0,
            used_endorse: HashSet::new(),
            weight_abort: 0,
            used_abort: HashSet::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(
        &mut self,
        author: PublicKey,
        signal: u8,
        committee: &Committee,
    ) -> ConsensusResult<Option<(bool, u8)>> {
        if signal == ABORT {
            // Ensure it is the first time this authority votes.
            ensure!(
                self.used_abort.insert(author),
                ConsensusError::AuthorityReuseinEndorse(author, signal)
            );
            self.weight_abort += committee.stake(&author);
            if self.weight_abort == committee.quorum_threshold() {
                return Ok(Some((!self.used_endorse.contains(&self.name), signal)));
            }
        } else {
            // Ensure it is the first time this authority votes.
            ensure!(
                self.used_endorse.insert(author),
                ConsensusError::AuthorityReuseinEndorse(author, signal)
            );
            self.weight_endorse += committee.stake(&author);
            if self.weight_endorse == committee.quorum_threshold() {
                return Ok(Some((!self.used_abort.contains(&self.name), signal)));
            }
        }

        Ok(None)
    }
}

struct PromiseVoteMaker {
    weight_endorse: Stake,
    used_endorse: HashSet<PublicKey>,
    weight_abort: Stake,
    used_abort: HashSet<PublicKey>,
}

impl PromiseVoteMaker {
    pub fn new() -> Self {
        Self {
            weight_endorse: 0,
            used_endorse: HashSet::new(),
            weight_abort: 0,
            used_abort: HashSet::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(
        &mut self,
        author: PublicKey,
        signal: u8,
        committee: &Committee,
    ) -> ConsensusResult<Option<u8>> {
        if signal == ABORT {
            // Ensure it is the first time this authority votes.
            ensure!(
                self.used_abort.insert(author),
                ConsensusError::AuthorityReuseinEndorse(author, signal)
            );
            self.weight_abort += committee.stake(&author);
            if self.weight_abort == committee.quorum_threshold() {
                return Ok(Some(signal));
            }
        } else {
            // Ensure it is the first time this authority votes.
            ensure!(
                self.used_endorse.insert(author),
                ConsensusError::AuthorityReuseinEndorse(author, signal)
            );
            self.weight_endorse += committee.stake(&author);
            if self.weight_endorse == committee.quorum_threshold() {
                return Ok(Some(signal));
            }
        }
        Ok(None)
    }
}
