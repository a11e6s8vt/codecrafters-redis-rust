use anyhow::{Context, Result};
use std::ascii::AsciiExt;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex, Notify, RwLock};

#[derive(Clone, Debug, Default)]
pub struct StreamEntry {
    pub key: String,
    pub entry_id: String,
    pub data: Vec<(String, String)>,
}

#[derive(Debug, Error)]
pub enum StreamError {
    #[error("ERR The ID specified in XADD must be greater than 0-0")]
    ZeroError,

    #[error("ERR Cannot proess the entry id or it is less than or equal to the last one")]
    NotValid,

    #[error("ERR The ID specified in XADD is equal or smaller than the target stream top item")]
    SmallerThanTop,

    #[error("ERR The key specified does not exist!")]
    KeyNotFound,

    #[error("ERR The stream contains no entries in the range")]
    NoEntriesInRange,
}

#[derive(Debug, Default)]
pub struct RadixNode {
    entry: Option<StreamEntry>,
    children: BTreeMap<char, Arc<RwLock<Self>>>,
    is_key: bool,
    is_entry_id: bool,
}

#[derive(Clone, Debug, Default, PartialEq, PartialOrd)]
pub struct EntryID {
    milliseconds_time: u128,
    sequence_number: u64,
}

impl EntryID {
    pub fn print(&self) -> String {
        format!("{}-{}", self.milliseconds_time, self.sequence_number)
    }
}

#[derive(Clone, Debug)]
pub struct RadixTreeStore {
    root: Arc<RwLock<RadixNode>>,
    last_entry_id: Arc<RwLock<EntryID>>,
    tx: mpsc::Sender<String>,
    rx: Arc<Mutex<mpsc::Receiver<String>>>,
    notify: Arc<Notify>,
}

impl RadixTreeStore {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(32);
        Self {
            root: Arc::new(RwLock::new(RadixNode::default())),
            last_entry_id: Arc::new(RwLock::new(EntryID::default())),
            tx,
            rx: Arc::new(Mutex::new(rx)),
            notify: Arc::new(Notify::new()),
        }
    }

    pub async fn new_entry_id(&self, entry_id_str: &str) -> Result<EntryID> {
        let new_id = match entry_id_str {
            "*" => {
                let start = SystemTime::now();

                // Calculate the duration since the UNIX_EPOCH
                let since_the_epoch = start
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards");

                // Convert the duration to milliseconds
                let millis = since_the_epoch.as_millis();
                EntryID {
                    milliseconds_time: millis,
                    sequence_number: 0,
                }
            }
            _ => {
                let new_id = if let Some((milliseconds_time, sequence_number)) =
                    entry_id_str.split_once("-")
                {
                    let milliseconds_time = milliseconds_time
                        .parse::<u128>()
                        .expect("Expect a valid number");
                    match sequence_number {
                        "*" => {
                            let last_entry_id_lock = self.last_entry_id.read().await;
                            let sequence_number =
                                if milliseconds_time > last_entry_id_lock.milliseconds_time {
                                    0u64
                                } else {
                                    last_entry_id_lock.sequence_number + 1
                                };
                            drop(last_entry_id_lock);
                            EntryID {
                                milliseconds_time,
                                sequence_number,
                            }
                        }
                        _ => {
                            let last_entry_id_lock = self.last_entry_id.read().await;
                            let sequence_number =
                                sequence_number.parse::<u64>().expect("A valid number");

                            if milliseconds_time == 0 && sequence_number == 0 {
                                return Err(StreamError::ZeroError).context(
                                    "ERR The ID specified in XADD must be greater than 0-0",
                                );
                            }

                            if milliseconds_time < last_entry_id_lock.milliseconds_time {
                                return Err(StreamError::SmallerThanTop).context("ERR The ID specified in XADD is equal or smaller than the target stream top item");
                            } else if milliseconds_time == last_entry_id_lock.milliseconds_time {
                                if sequence_number <= last_entry_id_lock.sequence_number {
                                    return Err(StreamError::SmallerThanTop).context("ERR The ID specified in XADD is equal or smaller than the target stream top item");
                                }
                            }
                            drop(last_entry_id_lock);
                            EntryID {
                                milliseconds_time,
                                sequence_number,
                            }
                        }
                    }
                } else {
                    return Err(StreamError::NotValid).context("ERR Cannot proess the entry id or it is less than or equal to the last one");
                };
                new_id
            }
        };
        Ok(new_id)
    }

    async fn find_next_node(
        &self,
        node: &Arc<RwLock<RadixNode>>,
        ch: &char,
    ) -> Option<Arc<RwLock<RadixNode>>> {
        let node_lock = node.read().await;
        if let Some(next_node) = node_lock.children.get(ch) {
            return Some(Arc::clone(next_node));
        }
        None
    }

    async fn insert_node(
        &self,
        node: &Arc<RwLock<RadixNode>>,
        ch: &char,
    ) -> Arc<RwLock<RadixNode>> {
        let mut node_lock = node.write().await;
        let new_node = Arc::new(RwLock::new(RadixNode::default()));
        node_lock.children.insert(*ch, new_node.clone());
        new_node
    }

    pub async fn insert(
        &mut self,
        key: &str,
        entry_id: &str,
        data: Vec<(String, String)>,
    ) -> Result<String> {
        let entry_id = self.new_entry_id(entry_id).await;
        let entry_id = match entry_id {
            Ok(entry_id) => entry_id,
            Err(e) => return Err(e),
        };
        let mut last_entry_id_lock = self.last_entry_id.write().await;
        *last_entry_id_lock = entry_id.clone();
        drop(last_entry_id_lock);

        let mut curr_node = self.root.clone();
        let prefix = format!("{}{}", key, entry_id.print());
        let prefix_chars = prefix.chars().enumerate().peekable();

        for (i, ch) in prefix_chars {
            if let Some(next_node) = self.find_next_node(&curr_node, &ch).await {
                curr_node = Arc::clone(&next_node);
                continue;
            }

            // Insert the node if no matching child was found
            curr_node = self.insert_node(&curr_node, &ch).await;
            if i == key.len() - 1 {
                curr_node.write().await.is_key = true;
            }
        }

        let entry: StreamEntry = StreamEntry {
            key: key.to_owned(),
            entry_id: entry_id.print(),
            data: data.clone(),
        };

        let mut curr_node_lock = curr_node.write().await;
        curr_node_lock.entry = Some(entry);
        curr_node_lock.is_entry_id = true;
        drop(curr_node_lock);
        let _ = self.tx.send(entry_id.print()).await;
        self.notify.notify_one();
        Ok(entry_id.print())
    }

    pub async fn get(&self, key: &str, entry_id: &str) -> Option<StreamEntry> {
        let mut curr_node = self.root.clone();
        let prefix = format!("{}{}", key, entry_id);
        let prefix_chars = prefix.chars();

        for ch in prefix_chars {
            if let Some(next_node) = self.find_next_node(&curr_node, &ch).await {
                curr_node = Arc::clone(&next_node);
            } else {
                return None;
            }
        }

        let curr_node_lock = curr_node.read().await;
        let entry = &curr_node_lock.entry;
        entry.clone()
    }

    pub async fn check_key(&self, key: &str) -> Option<String> {
        let mut current_node = self.root.clone();
        let mut key_matched = String::new();

        let mut key_iter = key.chars();
        loop {
            if let Some(ch) = key_iter.next() {
                if let Some(node) = self.find_next_node(&current_node, &ch).await {
                    key_matched.push(ch);
                    current_node = node.clone();
                    if current_node.read().await.is_key {
                        break;
                    }
                } else {
                    return None;
                }
            } else {
                return None;
            }
        }
        Some(key_matched)
    }

    pub async fn xrange(&self, key: &str, start: &str, end: &str) -> Result<Vec<StreamEntry>> {
        let mut results: Vec<StreamEntry> = Vec::new();
        let mut current_node = self.root.clone();
        let mut key_matched = String::new();

        let mut key_iter = key.chars().peekable();
        while let Some(ch) = key_iter.next() {
            if let Some(node) = self.find_next_node(&current_node, &ch).await {
                key_matched.push(ch);
                current_node = node.clone();
                if key_iter.peek().is_none() && current_node.read().await.is_key {
                    break;
                } else if key_iter.peek().is_some() && current_node.read().await.is_key {
                    return Err(StreamError::KeyNotFound)
                        .context("ERR The key specified does not exist!");
                }
            } else {
                return Err(StreamError::KeyNotFound)
                    .context("ERR The key specified does not exist!");
            }
        }

        let last_entry_id_lock = self.last_entry_id.read().await;
        let last_entry_id = last_entry_id_lock.print();
        drop(last_entry_id_lock);

        let start = match start {
            "-" => "",
            "$" => &last_entry_id,
            _ => start,
        };
        //self.get_range(current_node, "", start, end, &mut results)
        //.await;
        // BFS - Iterate all children of `current_node` to get a list of all entry_id's.
        let mut stack: VecDeque<(String, Arc<RwLock<RadixNode>>)> = VecDeque::new();
        stack.push_back(("".to_owned(), current_node));

        while let Some((prefix, node)) = stack.pop_front() {
            let node_lock = node.read().await;
            if let Some(entry) = &node_lock.entry {
                match end {
                    "+" => {
                        if *prefix >= *start {
                            results.push(entry.clone());
                        }
                    }
                    "++" => {
                        if *prefix > *start {
                            results.push(entry.clone());
                        }
                    }
                    _ => {
                        if *prefix >= *start && *prefix <= *end {
                            results.push(entry.clone());
                        }
                    }
                };
            }

            for (ch, child) in &node_lock.children {
                let new_prefix = format!("{}{}", prefix, ch);
                stack.push_back((new_prefix.to_owned(), child.clone()));
            }
            drop(node_lock);
        }

        if results.is_empty() {
            return Err(StreamError::NoEntriesInRange)
                .context("ERR The stream contains no entries in the range");
        }

        Ok(results)
    }

    pub async fn check_availability(
        &self,
        timeout: u64,
        entry_id: &str,
    ) -> Option<(String, String)> {
        let last_entry_id_lock = self.last_entry_id.read().await;
        let last_entry_id = last_entry_id_lock.print().to_ascii_lowercase();
        drop(last_entry_id_lock);

        let entry_id = if entry_id == "$" {
            last_entry_id.as_str()
        } else {
            entry_id
        };

        dbg!(&entry_id);

        let mut rx = self.rx.lock().await;
        let mut count = 2;
        let timeout_duration = if timeout == 0 {
            tokio::time::Duration::from_millis(10000)
        } else {
            tokio::time::Duration::from_millis(timeout)
        };
        let mut interval = tokio::time::interval(timeout_duration);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    println!("branch 1 - tick : {count}");
                    if timeout > 0 {
                        count -= 1;
                    } else {
                        count += 1;
                    }
                    if count == 0 {
                        return None;
                    }
                }
                _ = self.notify.notified() => {
                    dbg!("Waiting for message");
                    // notification received indicating a new insert
                    if let Some(message) = rx.recv().await {
                        dbg!(&message);
                        if message.to_ascii_lowercase().as_str() > entry_id {
                            dbg!("Greater");
                            return Some((last_entry_id, message));
                        }
                    }
                }
            }
        }
    }
}
