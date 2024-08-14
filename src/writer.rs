use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;

use crossbeam::channel::Sender;
use heed::{Env, RoTxn, RwTxn};
use heed::EnvFlags;
use heed::EnvOpenOptions;
use heed::types::{Bytes, Str};
use napi_derive::napi;

use crate::Entry;

#[derive(thiserror::Error, Debug)]
pub enum DatabaseWriterError {
  #[error("heed error: {0}")]
  HeedError(#[from] heed::Error),
  #[error("IO error: {0}")]
  IOError(#[from] std::io::Error),
}

#[napi(object)]
pub struct LMDBOptions {
  pub path: String,
  pub async_writes: bool,
  pub map_size: Option<u32>,
}

pub struct DatabaseWriterHandle {
  pub tx: Sender<DatabaseWriterMessage>,
  #[allow(unused)]
  thread_handle: JoinHandle<()>,
}

impl Drop for DatabaseWriterHandle {
  fn drop(&mut self) {
    let _ = self.tx.send(DatabaseWriterMessage::Stop);
  }
}

pub fn start_make_database_writer(
  options: LMDBOptions,
) -> Result<(DatabaseWriterHandle, Arc<DatabaseWriter>), DatabaseWriterError> {
  let (tx, rx) = crossbeam::channel::unbounded();
  let writer = Arc::new(DatabaseWriter::new(options)?);

  let thread_handle = std::thread::spawn({
    let writer = writer.clone();
    move || {
      tracing::debug!("Starting database writer thread");
      let mut current_transaction: Option<RwTxn> = None;

      while let Ok(msg) = rx.recv() {
        match msg {
          DatabaseWriterMessage::Get { key, resolve } => {
            let run = || {
              if let Some(txn) = &current_transaction {
                let result = writer.get(&*txn, &key)?;
                Ok(result)
              } else {
                let txn = writer.environment.read_txn()?;
                let result = writer.get(&txn, &key)?;
                txn.commit()?;
                Ok(result)
              }
            };
            let result = run();
            resolve(result);
          }
          DatabaseWriterMessage::Put {
            value,
            resolve,
            key,
          } => {
            let mut run = || {
              if let Some(txn) = &mut current_transaction {
                let result = writer.put(txn, &key, &value)?;
                Ok(result)
              } else {
                let mut txn = writer.environment.write_txn()?;
                let result = writer.put(&mut txn, &key, &value)?;
                txn.commit()?;
                Ok(result)
              }
            };
            let result = run();
            resolve(result);
          }
          DatabaseWriterMessage::Stop => {
            tracing::debug!("Stopping writer thread");
            break;
          }
          DatabaseWriterMessage::StartTransaction { resolve } => {
            let mut run = || {
              current_transaction = Some(writer.environment.write_txn()?);
              Ok(())
            };
            resolve(run())
          }
          DatabaseWriterMessage::CommitTransaction { resolve } => {
            if let Some(txn) = current_transaction.take() {
              resolve(txn.commit().map_err(|err| DatabaseWriterError::from(err)))
            }
          }
          DatabaseWriterMessage::PutMany { entries, resolve } => {
            let run = || {
              if let Some(txn) = &mut current_transaction {
                for Entry { key, value } in entries {
                  writer.put(txn, &key, &value)?;
                }
                Ok(())
              } else {
                let mut txn = writer.environment.write_txn()?;
                for Entry { key, value } in entries {
                  writer.put(&mut txn, &key, &value)?;
                }
                txn.commit()?;
                Ok(())
              }
            };
            let result = run();
            resolve(result);
          }
        }
      }
    }
  });

  Ok((DatabaseWriterHandle { tx, thread_handle }, writer))
}

pub enum DatabaseWriterMessage {
  Get {
    key: String,
    resolve: Box<dyn FnOnce(Result<Option<Vec<u8>>, DatabaseWriterError>) + Send>,
  },
  Put {
    key: String,
    value: Vec<u8>,
    resolve: Box<dyn FnOnce(Result<(), DatabaseWriterError>) + Send>,
  },
  PutMany {
    entries: Vec<Entry>,
    resolve: Box<dyn FnOnce(Result<(), DatabaseWriterError>) + Send>,
  },
  StartTransaction {
    resolve: Box<dyn FnOnce(Result<(), DatabaseWriterError>) + Send>,
  },
  CommitTransaction {
    resolve: Box<dyn FnOnce(Result<(), DatabaseWriterError>) + Send>,
  },
  Stop,
}

pub struct DatabaseWriter {
  pub environment: Env,
  pub database: heed::Database<Str, Bytes>,
}

impl DatabaseWriter {
  pub fn new(options: LMDBOptions) -> Result<Self, DatabaseWriterError> {
    let path = Path::new(&options.path);
    std::fs::create_dir_all(path)?;
    let environment = unsafe {
      let mut flags = EnvFlags::empty();
      flags.set(EnvFlags::MAP_ASYNC, options.async_writes);
      flags.set(EnvFlags::NO_SYNC, options.async_writes);
      flags.set(EnvFlags::WRITE_MAP, true);
      flags.set(EnvFlags::NO_READ_AHEAD, false);
      flags.set(EnvFlags::NO_META_SYNC, options.async_writes);
      let mut env_open_options = EnvOpenOptions::new();
      env_open_options.flags(flags);
      // http://www.lmdb.tech/doc/group__mdb.html#gaa2506ec8dab3d969b0e609cd82e619e5
      // max DB size that will be memory mapped
      if let Some(map_size) = options.map_size {
        env_open_options.map_size(map_size as usize);
      }
      env_open_options.open(path)
    }?;
    let mut write_txn = environment.write_txn()?;
    let database = environment.create_database(&mut write_txn, None)?;
    write_txn.commit()?;

    Ok(Self {
      database,
      environment,
    })
  }

  pub fn get(&self, txn: &RoTxn, key: &str) -> Result<Option<Vec<u8>>, DatabaseWriterError> {
    let result = self.database.get(&txn, key)?;
    Ok(result.map(|d| d.to_owned()))
  }

  pub fn put(&self, txn: &mut RwTxn, key: &str, data: &[u8]) -> Result<(), DatabaseWriterError> {
    self.database.put(txn, key, data)?;
    Ok(())
  }
}
