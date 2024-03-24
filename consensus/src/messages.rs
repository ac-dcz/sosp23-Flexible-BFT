use crate::config::Committee;
use crate::core::SeqNumber;
use crate::error::{ConsensusError, ConsensusResult};
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::fmt;
use threshold_crypto::{PublicKeySet, SignatureShare};

#[cfg(test)]
#[path = "tests/messages_tests.rs"]
pub mod messages_tests;

// daniel: Add view, height, fallback in Block, Vote and QC
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Block {
    pub author: PublicKey,
    pub height: SeqNumber,
    pub timestamp: u128,
    pub payload: Vec<Digest>,
    pub signature: Signature,
}

impl Block {
    pub async fn new(
        author: PublicKey,
        height: SeqNumber,
        timestamp: u128,
        payload: Vec<Digest>,
        mut signature_service: SignatureService,
    ) -> Self {
        let block = Self {
            author,
            height,
            timestamp,
            payload,
            signature: Signature::default(),
        };

        let signature = signature_service.request_signature(block.digest()).await;
        Self { signature, ..block }
    }

    pub fn genesis() -> Self {
        Block::default()
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        ensure!(
            voting_rights > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature.verify(&self.digest(), &self.author)?;

        Ok(())
    }
}

impl Hash for Block {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.height.to_le_bytes());
        for x in &self.payload {
            hasher.update(x);
        }
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: B(author {}, height {}, payload_len {})",
            self.digest(),
            self.author,
            self.height,
            self.payload.iter().map(|x| x.size()).sum::<usize>(),
        )
    }
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: B(author {}, height {}, payload_len {})",
            self.digest(),
            self.author,
            self.height,
            self.payload.iter().map(|x| x.size()).sum::<usize>(),
        )
    }
}

/************************** The fast negotiation Struct ************************************/
#[derive(Clone, Serialize, Deserialize)]
pub struct Proposal {
    pub block: Block,
    pub author: PublicKey,
    pub height: SeqNumber,
    pub timestamp: u128, //提出时间戳
    pub signature: Signature,
}

impl Proposal {
    pub async fn new(block: Block, mut signature_service: SignatureService) -> Self {
        let mut proposal = Self {
            author: block.author,
            height: block.height,
            timestamp: block.timestamp,
            block,
            signature: Signature::default(),
        };
        proposal.signature = signature_service.request_signature(proposal.digest()).await;
        return proposal;
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        ensure!(
            voting_rights > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature.verify(&self.digest(), &self.author)?;

        Ok(())
    }
}

impl Hash for Proposal {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.timestamp.to_le_bytes());
        hasher.update(self.block.digest());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Proposal {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "Proposal(author {}, timestamp {})",
            self.author, self.timestamp,
        )
    }
}

impl fmt::Display for Proposal {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "Proposal(author {}, timestamp {})",
            self.author, self.timestamp,
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EndorseVote {
    pub author: PublicKey,
    pub height: SeqNumber,
    pub timestamp: u128,
    pub digest: Digest,
    pub signal: u8,
    pub signature: Signature,
}

impl EndorseVote {
    pub async fn new(
        author: PublicKey,
        height: SeqNumber,
        timestamp: u128,
        digest: Digest,
        signal: u8,
        mut signature_service: SignatureService,
    ) -> Self {
        let mut endorse = Self {
            author,
            height,
            timestamp,
            digest,
            signal,
            signature: Signature::default(),
        };
        endorse.signature = signature_service.request_signature(endorse.digest()).await;
        return endorse;
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        ensure!(
            voting_rights > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature.verify(&self.digest(), &self.author)?;

        Ok(())
    }
}

impl Hash for EndorseVote {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.timestamp.to_le_bytes());
        hasher.update(self.digest.0);
        hasher.update(self.signal.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for EndorseVote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "EndorseVote(author {}, timestamp {}, signal {})",
            self.author, self.timestamp, self.signal
        )
    }
}

impl fmt::Display for EndorseVote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "EndorseVote(author {}, timestamp {}, signal {})",
            self.author, self.timestamp, self.signal
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PromiseVote {
    pub author: PublicKey,
    pub height: SeqNumber,
    pub timestamp: u128,
    pub digest: Digest,
    pub signal: u8,
    pub signature: Signature,
}

impl PromiseVote {
    pub async fn new(
        author: PublicKey,
        height: SeqNumber,
        timestamp: u128,
        digest: Digest,
        signal: u8,
        mut signature_service: SignatureService,
    ) -> Self {
        let mut promise = Self {
            author,
            height,
            timestamp,
            digest,
            signal,
            signature: Signature::default(),
        };
        promise.signature = signature_service.request_signature(promise.digest()).await;
        return promise;
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        ensure!(
            voting_rights > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature.verify(&self.digest(), &self.author)?;

        Ok(())
    }
}

impl Hash for PromiseVote {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.timestamp.to_le_bytes());
        hasher.update(self.digest.0);
        hasher.update(self.signal.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for PromiseVote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "EndorseVote(author {}, timestamp {}, signal {})",
            self.author, self.timestamp, self.signal
        )
    }
}

impl fmt::Display for PromiseVote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "EndorseVote(author {}, timestamp {}, signal {})",
            self.author, self.timestamp, self.signal
        )
    }
}

/************************** The fast negotiation Struct ************************************/

/************************** ABA Struct ************************************/
#[derive(Clone, Serialize, Deserialize)]
pub struct ABAVal {
    pub author: PublicKey,
    pub timestamp: u128,
    pub height: SeqNumber,
    pub round: SeqNumber, //ABA 的轮数
    pub phase: u8,        //ABA 的阶段（VAL，AUX）
    pub val: usize,
    pub signature: Signature,
}

impl ABAVal {
    pub async fn new(
        author: PublicKey,
        timestamp: u128,
        height: SeqNumber,
        round: SeqNumber,
        val: usize,
        phase: u8,
        mut signature_service: SignatureService,
    ) -> Self {
        let mut aba_val = Self {
            author,
            timestamp,
            height,
            round,
            val,
            phase,
            signature: Signature::default(),
        };
        aba_val.signature = signature_service.request_signature(aba_val.digest()).await;
        return aba_val;
    }

    pub fn verify(&self) -> ConsensusResult<()> {
        self.signature.verify(&self.digest(), &self.author)?;
        Ok(())
    }
}

impl Hash for ABAVal {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.timestamp.to_le_bytes());
        hasher.update(self.round.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for ABAVal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ABAVal(author{},timestamp {},height {},round {},phase {},val {})",
            self.author, self.timestamp, self.height, self.round, self.phase, self.val
        )
    }
}

impl fmt::Display for ABAVal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ABAVal(author{},timestamp {},height {},round {},phase {},val {})",
            self.author, self.timestamp, self.height, self.round, self.phase, self.val
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ABAOutput {
    pub author: PublicKey,
    pub timestamp: u128,
    pub height: SeqNumber,
    pub round: SeqNumber,
    pub val: usize,
    pub signature: Signature,
}

impl ABAOutput {
    pub async fn new(
        author: PublicKey,
        timestamp: u128,
        height: SeqNumber,
        round: SeqNumber,
        val: usize,
        mut signature_service: SignatureService,
    ) -> Self {
        let mut out = Self {
            author,
            timestamp,
            height,
            round,
            val,
            signature: Signature::default(),
        };
        out.signature = signature_service.request_signature(out.digest()).await;
        return out;
    }

    pub fn verify(&self) -> ConsensusResult<()> {
        self.signature.verify(&self.digest(), &self.author)?;
        Ok(())
    }
}

impl Hash for ABAOutput {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.timestamp.to_le_bytes());
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.round.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for ABAOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ABAOutput(author {},timestamp {},height {},round {},val {})",
            self.author, self.timestamp, self.height, self.round, self.val
        )
    }
}

/************************** ABA Struct ************************************/

/************************** Share Coin Struct ************************************/
#[derive(Clone, Serialize, Deserialize)]
pub struct RandomnessShare {
    pub timestamp: u128,
    pub height: SeqNumber,
    pub round: SeqNumber,
    pub author: PublicKey,
    pub signature_share: SignatureShare,
}

impl RandomnessShare {
    pub async fn new(
        timestamp: u128,
        height: SeqNumber,
        round: SeqNumber,
        author: PublicKey,
        mut signature_service: SignatureService,
    ) -> Self {
        let mut hasher = Sha512::new();
        hasher.update(round.to_le_bytes());
        hasher.update(height.to_le_bytes());
        hasher.update(timestamp.to_le_bytes());
        let digest = Digest(hasher.finalize().as_slice()[..32].try_into().unwrap());
        let signature_share = signature_service
            .request_tss_signature(digest)
            .await
            .unwrap();
        Self {
            round,
            height,
            timestamp,
            author,
            signature_share,
        }
    }

    pub fn verify(&self, committee: &Committee, pk_set: &PublicKeySet) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            ConsensusError::UnknownAuthority(self.author)
        );
        let tss_pk = pk_set.public_key_share(committee.id(self.author));
        // Check the signature.
        ensure!(
            tss_pk.verify(&self.signature_share, &self.digest()),
            ConsensusError::InvalidThresholdSignature(self.author)
        );

        Ok(())
    }
}

impl Hash for RandomnessShare {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.timestamp.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for RandomnessShare {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "RandomnessShare (author {},timestamp {} ,height {},round {})",
            self.author, self.timestamp, self.height, self.round,
        )
    }
}
/************************** Share Coin Struct **************************/

/************************** Pass mechanism Struct **************************/

#[derive(Clone, Serialize, Deserialize)]
pub struct PassMechanism {
    pub author: PublicKey,
    pub height: SeqNumber,
    pub timestamp: u128,
    pub endorse: Vec<(u128, SeqNumber)>,
    pub signature: Signature,
}

impl PassMechanism {
    pub async fn new(
        author: PublicKey,
        height: SeqNumber,
        timestamp: u128,
        endorse: Vec<(u128, SeqNumber)>,
        mut signature_service: SignatureService,
    ) -> Self {
        let mut pass = Self {
            author,
            height,
            timestamp,
            endorse,
            signature: Signature::default(),
        };
        pass.signature = signature_service.request_signature(pass.digest()).await;
        return pass;
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature.verify(&self.digest(), &self.author)?;
        Ok(())
    }
}

impl Hash for PassMechanism {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.timestamp.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for PassMechanism {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "PassMechanism (author {},timestamp {} ,height {})",
            self.author, self.timestamp, self.height,
        )
    }
}
/************************** Pass mechanism Struct **************************/
