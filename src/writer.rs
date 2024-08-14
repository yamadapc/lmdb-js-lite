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

type Result<R> = std::result::Result<R, DatabaseWriterError>;

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
  tx: Sender<DatabaseWriterMessage>,
  #[allow(unused)]
  thread_handle: JoinHandle<()>,
}

impl DatabaseWriterHandle {
  pub fn send(
    &self,
    message: DatabaseWriterMessage,
  ) -> std::result::Result<(), crossbeam::channel::SendError<DatabaseWriterMessage>> {
    self.tx.send(message)
  }
}

impl Drop for DatabaseWriterHandle {
  fn drop(&mut self) {
    let _ = self.tx.send(DatabaseWriterMessage::Stop);
  }
}

pub fn start_make_database_writer(
  options: LMDBOptions,
) -> Result<(DatabaseWriterHandle, Arc<DatabaseWriter>)> {
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
                let result = writer.get(txn, &key)?.map(|d| d.to_owned());
                Ok(result)
              } else {
                let txn = writer.environment.read_txn()?;
                let result = writer.get(&txn, &key)?.map(|d| d.to_owned());
                txn.commit()?;
                Ok(result)
              }
            };
            let result = run();
            resolve(result.map(|o| o.map(|d| d.to_owned())));
          }
          DatabaseWriterMessage::Put {
            value,
            resolve,
            key,
          } => {
            let mut run = || {
              if let Some(txn) = &mut current_transaction {
                writer.put(txn, &key, &value)?;
                Ok(())
              } else {
                let mut txn = writer.environment.write_txn()?;
                writer.put(&mut txn, &key, &value)?;
                txn.commit()?;
                Ok(())
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
              resolve(txn.commit().map_err(DatabaseWriterError::from))
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

type ResolveCallback<T> = Box<dyn FnOnce(Result<T>) + Send>;

pub enum DatabaseWriterMessage {
  Get {
    key: String,
    resolve: ResolveCallback<Option<Vec<u8>>>,
  },
  Put {
    key: String,
    value: Vec<u8>,
    resolve: ResolveCallback<()>,
  },
  PutMany {
    entries: Vec<Entry>,
    resolve: ResolveCallback<()>,
  },
  StartTransaction {
    resolve: ResolveCallback<()>,
  },
  CommitTransaction {
    resolve: ResolveCallback<()>,
  },
  Stop,
}

pub struct DatabaseWriter {
  environment: Env,
  database: heed::Database<Str, Bytes>,
}

impl DatabaseWriter {
  pub fn new(options: LMDBOptions) -> Result<Self> {
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

  pub fn get<'a>(&self, txn: &'a RoTxn, key: &str) -> Result<Option<&'a [u8]>> {
    let result = self.database.get(txn, key)?;
    Ok(result)
  }

  pub fn put(&self, txn: &mut RwTxn, key: &str, data: &[u8]) -> Result<()> {
    self.database.put(txn, key, data)?;
    Ok(())
  }

  pub fn read_txn(&self) -> heed::Result<RoTxn> {
    self.environment.read_txn()
  }

  pub fn static_read_txn(&self) -> heed::Result<RoTxn<'static>> {
    self.environment.clone().static_read_txn()
  }
}
