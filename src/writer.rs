use std::path::Path;
use std::rc::Rc;
use std::sync::mpsc::Sender;
use std::thread::JoinHandle;

use heed::{Env, RoTxn, RwTxn};
use heed::EnvFlags;
use heed::EnvOpenOptions;
use heed::types::{Bytes, Str};
use napi_derive::napi;

#[napi(object)]
pub struct LMDBOptions {
  pub path: String,
  pub async_writes: bool,
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

pub fn start_make_database_writer(options: LMDBOptions) -> anyhow::Result<DatabaseWriterHandle> {
  let (tx, rx) = std::sync::mpsc::channel();
  let writer = DatabaseWriter::new(options)?;

  let thread_handle = std::thread::spawn(move || {
    tracing::debug!("Starting database writer thread");
    let mut current_transaction: Option<RwTxn> = None;
    while let Ok(msg) = rx.recv() {
      match msg {
        DatabaseWriterMessage::Get { key, resolve } => {
          tracing::debug!(%key, "Handling get message");
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
          tracing::debug!(%key, "Handling put message");
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
            resolve(txn.commit().map_err(|err| anyhow::Error::from(err)))
          }
        }
      }
    }
  });

  Ok(DatabaseWriterHandle { tx, thread_handle })
}

pub enum DatabaseWriterMessage {
  Get {
    key: String,
    resolve: Box<dyn FnOnce(anyhow::Result<Option<Vec<u8>>>) + Send>,
  },
  Put {
    key: String,
    value: Vec<u8>,
    resolve: Box<dyn FnOnce(anyhow::Result<()>) + Send>,
  },
  StartTransaction {
    resolve: Box<dyn FnOnce(anyhow::Result<()>) + Send>,
  },
  CommitTransaction {
    resolve: Box<dyn FnOnce(anyhow::Result<()>) + Send>,
  },
  Stop,
}

pub enum TransactionOperation {
  Get {
    key: String,
    resolve: Box<dyn FnOnce(anyhow::Result<Option<Vec<u8>>>) + Send>,
  },
  Put {
    key: String,
    value: Vec<u8>,
    resolve: Box<dyn FnOnce(anyhow::Result<()>) + Send>,
  },
}

pub struct DatabaseWriter {
  environment: Env,
  database: heed::Database<Str, Bytes>,
}

impl DatabaseWriter {
  pub fn new(options: LMDBOptions) -> anyhow::Result<Self> {
    let path = Path::new(&options.path);
    std::fs::create_dir_all(path)?;
    let environment = unsafe {
      let mut flags = EnvFlags::empty();
      flags.set(EnvFlags::MAP_ASYNC, options.async_writes);
      flags.set(EnvFlags::NO_SYNC, options.async_writes);
      flags.set(EnvFlags::NO_META_SYNC, options.async_writes);
      EnvOpenOptions::new()
        // http://www.lmdb.tech/doc/group__mdb.html#gaa2506ec8dab3d969b0e609cd82e619e5
        // 10GB max DB size that will be memory mapped
        .map_size(40 * 1024 * 1024 * 1024)
        .flags(flags)
        .open(path)
    }?;
    let mut write_txn = environment.write_txn()?;
    let database = environment.create_database(&mut write_txn, None)?;
    write_txn.commit()?;

    Ok(Self {
      database,
      environment,
    })
  }

  pub fn get(&self, txn: &RoTxn, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
    let result = self.database.get(&txn, key)?;
    Ok(result.map(|d| d.to_owned()))
  }

  pub fn put(&self, txn: &mut RwTxn, key: &str, data: &[u8]) -> anyhow::Result<()> {
    self.database.put(txn, key, data)?;
    Ok(())
  }
}
