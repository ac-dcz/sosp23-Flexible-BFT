use std::collections::HashMap;
use std::u128;

use crate::{Block, Committee, ConsensusMessage, SeqNumber};
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::{Receiver, Sender};

enum CommitMessage {
    AddInstance(u128, SeqNumber),
    UpdateExec(u128),
    ArbcOutput(u128, SeqNumber, Option<Block>),
}

pub struct SortVec {
    data: Vec<u128>,
}

impl SortVec {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }
    pub fn push(&mut self, ts: u128) {
        let index = self.data.partition_point(|&x| x < ts);
        self.data.insert(index, ts);
    }

    pub fn get(&self) -> u128 {
        if self.data.len() == 0 {
            return 0;
        }
        return self.data[0];
    }

    pub fn pop(&mut self) {
        if self.data.len() > 0 {
            self.data.remove(0);
        }
    }
}

pub struct Commitor {
    tx_channel: Sender<CommitMessage>,
}

impl Commitor {
    pub fn new(tx_core: Sender<ConsensusMessage>, committee: &Committee) -> Self {
        let (tx_channel, mut rx_channel): (_, Receiver<CommitMessage>) = channel(1000);
        let mut instance: Vec<SortVec> = Vec::new();
        for _ in 0..committee.size() {
            instance.push(SortVec::new());
        }
        let (mut exec_ts, mut pass_ts): (u128, u128) = (0, 0);
        let mut commit_map: HashMap<(u128, SeqNumber), Option<Block>> = HashMap::new();
        let mut commit_instance: Vec<SortVec> = Vec::new();
        for _ in 0..committee.size() {
            commit_instance.push(SortVec::new());
        }
        tokio::spawn(async move {
            while let Some(message) = rx_channel.recv().await {
                match message {
                    CommitMessage::AddInstance(ts, h) => {
                        instance[h as usize].push(ts);
                    }
                    CommitMessage::UpdateExec(exec) => {
                        if exec > exec_ts {
                            exec_ts = exec;
                        }
                        //try to commit
                    }
                    CommitMessage::ArbcOutput(ts, height, block) => {
                        commit_map.insert((ts, height), block);
                        commit_instance[height as usize].push(ts);
                        let (mut ts, mut h): (u128, SeqNumber) = (u128::max_value(), 0);
                    }
                }
            }
        });

        Self { tx_channel }
    }
}
