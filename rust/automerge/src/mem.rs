//use std::thread;
//use std::time::Duration;
use jemalloc_ctl::{stats, epoch};

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug,Clone, PartialEq, Default)]
pub struct MemState {
  allocated: i64,
  resident: i64
}

impl std::ops::Sub for MemState {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self {allocated: self.allocated - other.allocated, resident: self.resident - other.resident}
    }
}

#[derive(Debug,Clone, PartialEq, Default)]
pub struct Log {
  label: String,
  mem: Option<MemState>,
}

#[derive(Debug,Clone, PartialEq)]
pub struct MemU {
  start: MemState,
  last: Option<MemState>,
  log: Vec<Log>,
}

impl Default for MemU {
  fn default() -> Self {
      Self {
        start: memstate(),
        last: None,
        log: vec![],
      }
  }
}

impl core::fmt::Display for MemState {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
    if self.allocated > (1 << 30) {
      write!(f, "{}G", self.allocated >> 30)?;
    } else if self.allocated > (1 << 20) {
      write!(f, "{}M", self.allocated >> 20)?;
    } else if self.allocated > (1 << 10) {
      write!(f, "{}K", self.allocated >> 10)?;
    } else if self.allocated < 0 {
      write!(f, "0b")?;
    } else {
      write!(f, "{}b", self.allocated)?;
    }
    Ok(())
  }
}

impl MemU {
  pub fn append(&mut self, mut other: MemU) {
    for log in &mut other.log {
        log.label = format!("    {}",&log.label).to_owned();
    }
    self.log.extend(other.log);
  }

  pub fn close(&mut self) {
    if let Some(log) = self.log.get_mut(0) {
      log.mem = Some(self.start.clone() - memstate());
    }
  }

  pub fn println(&self) {
    for log in &self.log {
      if let Some(m) = &log.mem {
        println!("{} :: {}", log.label, m)
      } else {
        println!("{} :::", log.label)
      }
    }
  }
}

fn memstate() -> MemState {
    let e = epoch::mib().unwrap();
    e.advance().unwrap();
    let allocated = stats::allocated::mib().unwrap();
    let resident = stats::resident::mib().unwrap();
    let allocated = allocated.read().unwrap() as i64;
    let resident = resident.read().unwrap() as i64;
    MemState { allocated, resident }
}

pub fn memcheck(label: &str, mem: &mut MemU) {
    let cur = memstate();
    let last = mem.last.clone();
    mem.last = Some(cur.clone());
    let label = label.to_owned();
    match last {
      Some(m) => mem.log.push(Log { label, mem: Some(m - cur) }),
      None => mem.log.push(Log { label, mem: None }),
    }
}
