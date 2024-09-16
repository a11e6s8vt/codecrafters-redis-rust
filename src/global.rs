use std::borrow::BorrowMut;
use std::sync::OnceLock;

pub struct GlobalConfig<T> {
    data: OnceLock<(T, T)>,
    next: OnceLock<Box<GlobalConfig<T>>>,
}
impl<T> GlobalConfig<T> {
    const fn new() -> GlobalConfig<T> {
        GlobalConfig {
            data: OnceLock::new(),
            next: OnceLock::new(),
        }
    }
    pub fn push(&self, input: (T, T)) {
        if let Err(value) = self.data.set((input.0, input.1)) {
            let next = self.next.get_or_init(|| Box::new(GlobalConfig::new()));
            next.push(value)
        };
    }
    fn contains(&self, key: &T) -> bool
    where
        T: PartialEq,
    {
        self.data
            .get()
            .map(|item| item.0 == *key)
            .filter(|v| *v)
            .unwrap_or_else(|| {
                self.next
                    .get()
                    .map(|next| next.contains(&key))
                    .unwrap_or(false)
            })
    }

    pub fn get_val(&self, key: &T) -> Option<&T>
    where
        T: PartialEq,
    {
        let mut data = self.data.get();
        let mut next = self.next.get();
        loop {
            if let Some((k, v)) = data {
                if *k == *key {
                    return Some(v);
                }
                if next.is_some() {
                    let next_item = next.as_mut().unwrap().borrow_mut();
                    data = next_item.data.get();
                    next = next_item.next.get();
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        None
    }
}

pub static STATE: GlobalConfig<String> = GlobalConfig::new();
