use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub struct ExpiringHashMap<K, V> {
    hash_map: Arc<Mutex<HashMap<K, (V, Option<(Instant, Duration)>)>>>,
}

impl<K, V> ExpiringHashMap<K, V>
where
    K: Clone + Eq + std::hash::Hash,
    V: Display + Debug + Clone,
{
    pub fn new() -> Self {
        Self {
            hash_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn insert(&mut self, k: K, v: V, expiry: Option<Duration>) -> Option<V> {
        let mut guard = self.hash_map.lock().await;
        let val = if expiry.is_some() {
            guard
                .insert(k, (v, Some((Instant::now(), expiry.unwrap()))))
                .map(|v| v.0)
        } else {
            guard.insert(k, (v, None)).map(|v| v.0)
        };
        drop(guard);
        val
    }

    pub async fn get(&self, k: &K) -> Option<V> {
        let now = Instant::now();
        let mut guard = self.hash_map.lock().await;
        let val = if guard.contains_key(&k) {
            let expired = guard.get(&k).and_then(|(x, t)| {
                dbg!("v = {}, t = {}", x, t);
                if t.is_some() {
                    if (now - t.unwrap().0) > t.unwrap().1 {
                        Some(true)
                    } else {
                        Some(false)
                    }
                } else {
                    Some(false)
                }
            });
            if expired.is_some_and(|x| x == true) {
                guard.remove(&k);
                None
            } else {
                guard.get(k).and_then(|(val, t)| Some(val)).cloned()
            }
        } else {
            None
        };
        drop(guard);
        val
    }

    pub async fn contains_key(&self, k: &K) -> bool {
        let guard = self.hash_map.lock().await;
        let val = guard.contains_key(k);
        drop(guard);
        val
    }

    pub async fn prune(&mut self) {
        loop {
            let now = Instant::now();
            // let Self { hash_map, duration } = self;
            let mut guard = self.hash_map.lock().await;
            let keys = guard.keys().cloned().collect::<Vec<K>>();
            for k in keys {
                let expired = guard.get(&k).and_then(|(_, t)| {
                    if t.is_some() {
                        if (now - t.unwrap().0) > t.unwrap().1 {
                            Some(true)
                        } else {
                            Some(false)
                        }
                    } else {
                        Some(false)
                    }
                });

                if expired.is_some_and(|x| x == true) {
                    guard.remove(&k);
                }
            }
            drop(guard);
        }
    }
}

pub async fn prune_database(db: Arc<Mutex<ExpiringHashMap<String, String>>>) {
    loop {
        let mut guard = db.lock().await;
        guard.prune().await;
        drop(guard);
        std::thread::sleep(Duration::from_millis(50));
    }
}
