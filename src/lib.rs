#![deny(clippy::all)]

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex, Weak};

use anyhow::anyhow;
use lazy_static::lazy_static;
use napi::bindgen_prelude::Env;
use napi::JsUnknown;
use napi_derive::napi;
use tracing::Level;

use crate::writer::{
  DatabaseWriter, DatabaseWriterError, DatabaseWriterHandle, DatabaseWriterMessage,
  start_make_database_writer,
};
use crate::writer::LMDBOptions;

pub mod writer;

#[cfg(not(test))]
type Buffer = napi::bindgen_prelude::Buffer;
#[cfg(test)]
type Buffer = Vec<u8>;

fn napi_error(err: impl Debug) -> napi::Error {
  napi::Error::from_reason(format!("[napi] {err:?}"))
}

struct DatabaseHandle {
  writer: Arc<DatabaseWriterHandle>,
  database: Arc<DatabaseWriter>,
}

struct LMDBGlobalState {
  /// Grows unbounded. It will not be cleaned-up as that complicates things. Opening and closing
  /// many databases on the same process will cause this to grow.
  databases: HashMap<LMDBOptions, Weak<DatabaseHandle>>,
}

impl LMDBGlobalState {
  fn new() -> Self {
    Self {
      databases: HashMap::new(),
    }
  }

  fn get_database(
    &mut self,
    options: LMDBOptions,
  ) -> Result<Arc<DatabaseHandle>, DatabaseWriterError> {
    let (writer, database) = start_make_database_writer(&options)?;
    let handle = Arc::new(DatabaseHandle {
      writer: Arc::new(writer),
      database,
    });
    self.databases.insert(options, Arc::downgrade(&handle));
    Ok(handle)
  }
}

lazy_static! {
  static ref STATE: Mutex<LMDBGlobalState> = Mutex::new(LMDBGlobalState::new());
}

#[napi]
pub fn init_tracing_subscriber() {
  let _ = tracing_subscriber::FmtSubscriber::builder()
    .with_max_level(Level::DEBUG)
    .try_init();
}

#[napi(object)]
pub struct Entry {
  pub key: String,
  pub value: Buffer,
}

pub struct NativeEntry {
  pub key: String,
  // We copy out of the buffer because it's undefined behaviour to send it across
  pub value: Vec<u8>,
}

#[napi]
pub struct LMDB {
  inner: Option<Arc<DatabaseHandle>>,
  read_transaction: Option<heed::RoTxn<'static>>,
}

#[napi]
impl LMDB {
  #[napi(constructor)]
  pub fn new(options: LMDBOptions) -> napi::Result<Self> {
    let mut state = STATE
      .lock()
      .map_err(|_| napi::Error::from_reason("LMDB State mutex is poisoned"))?;
    let database = state.get_database(options).map_err(napi_error)?;
    Ok(Self {
      inner: Some(database),
      read_transaction: None,
    })
  }

  #[napi(ts_return_type = "Promise<Buffer | null | undefined>")]
  pub fn get(&self, env: Env, key: String) -> napi::Result<napi::JsObject> {
    let database_handle = self.get_database()?;
    let (deferred, promise) = env.create_deferred()?;

    database_handle
      .writer
      .send(DatabaseWriterMessage::Get {
        key,
        resolve: Box::new(|value| match value {
          Ok(value) => deferred.resolve(move |_| Ok(value.map(Buffer::from))),
          Err(err) => deferred.reject(napi_error(err)),
        }),
      })
      .map_err(|err| napi_error(anyhow!("Failed to send {err}")))?;

    Ok(promise)
  }

  #[napi(ts_return_type = "Buffer | null")]
  pub fn get_sync(&self, env: Env, key: String) -> napi::Result<JsUnknown> {
    let database_handle = self.get_database()?;
    let database = &database_handle.database;

    let txn = if let Some(txn) = &self.read_transaction {
      txn
    } else {
      &database
        .read_txn()
        .map_err(|err| napi_error(anyhow!(err)))?
    };
    let buffer = database.get(txn, &key);
    let Some(buffer) = buffer.map_err(|err| napi_error(anyhow!(err)))? else {
      return Ok(env.get_null()?.into_unknown());
    };
    let mut result = env.create_buffer(buffer.len())?;
    result.copy_from_slice(&buffer);
    Ok(result.into_unknown())
  }

  #[napi]
  pub fn get_many_sync(&self, keys: Vec<String>) -> napi::Result<Vec<Option<Buffer>>> {
    let database_handle = self.get_database()?;
    let database = &database_handle.database;

    let mut results = vec![];
    let txn = database
      .read_txn()
      .map_err(|err| napi_error(anyhow!(err)))?;

    for key in keys {
      let buffer = database
        .get(&txn, &key)
        .map_err(|err| napi_error(anyhow!(err)))?
        .map(Buffer::from);
      results.push(buffer);
    }

    Ok(results)
  }

  #[napi(ts_return_type = "Promise<void>")]
  pub fn put_many(&self, env: Env, entries: Vec<Entry>) -> napi::Result<napi::JsObject> {
    let database_handle = self.get_database()?;
    let (deferred, promise) = env.create_deferred()?;

    let message = DatabaseWriterMessage::PutMany {
      entries: entries
        .into_iter()
        .map(|entry| NativeEntry {
          key: entry.key,
          value: entry.value.into(),
        })
        .collect(),
      resolve: Box::new(|value| {
        deferred.resolve(|_| value.map_err(|err| napi_error(anyhow!("Failed to write {err}"))))
      }),
    };
    database_handle
      .writer
      .send(message)
      .map_err(|err| napi_error(anyhow!("Failed to send {err}")))?;

    Ok(promise)
  }

  #[napi(ts_return_type = "Promise<void>")]
  pub fn put(&self, env: Env, key: String, data: Buffer) -> napi::Result<napi::JsObject> {
    let database_handle = self.get_database()?;
    // This costs us 70% over the round-trip time after arg. conversion
    let (deferred, promise) = env.create_deferred()?;

    let message = DatabaseWriterMessage::Put {
      key,
      value: data.to_vec(),
      resolve: Box::new(|value| match value {
        Ok(value) => deferred.resolve(move |_| Ok(value)),
        Err(err) => deferred.reject(napi_error(anyhow!("Failed to write {err}"))),
      }),
    };
    database_handle
      .writer
      .send(message)
      .map_err(|err| napi_error(anyhow!("Failed to send {err}")))?;

    Ok(promise)
  }

  #[napi]
  pub fn put_no_confirm(&self, key: String, data: Buffer) -> napi::Result<()> {
    let database_handle = self.get_database()?;

    let message = DatabaseWriterMessage::Put {
      key,
      value: data.to_vec(),
      resolve: Box::new(|_| {}),
    };
    database_handle
      .writer
      .send(message)
      .map_err(|err| napi_error(anyhow!("Failed to send {err}")))?;

    Ok(())
  }

  #[napi]
  pub fn start_read_transaction(&mut self) -> napi::Result<()> {
    if self.read_transaction.is_some() {
      return Ok(());
    }
    let database_handle = self.get_database()?;
    let txn = database_handle
      .database
      .static_read_txn()
      .map_err(|err| napi_error(anyhow!(err)))?;
    self.read_transaction = Some(txn);
    Ok(())
  }

  #[napi]
  pub fn commit_read_transaction(&mut self) -> napi::Result<()> {
    if let Some(txn) = self.read_transaction.take() {
      txn.commit().map_err(|err| napi_error(anyhow!(err)))?;
      Ok(())
    } else {
      Ok(())
    }
  }

  #[napi(ts_return_type = "Promise<void>")]
  pub fn start_write_transaction(&self, env: Env) -> napi::Result<napi::JsObject> {
    let database_handle = self.get_database()?;
    let (deferred, promise) = env.create_deferred()?;

    let message = DatabaseWriterMessage::StartTransaction {
      resolve: Box::new(|_| deferred.resolve(|_| Ok(()))),
    };
    database_handle
      .writer
      .send(message)
      .map_err(|err| napi_error(anyhow!("Failed to send {err}")))?;

    Ok(promise)
  }

  #[napi(ts_return_type = "Promise<void>")]
  pub fn commit_write_transaction(&self, env: Env) -> napi::Result<napi::JsObject> {
    let database_handle = self.get_database()?;
    let (deferred, promise) = env.create_deferred()?;

    let message = DatabaseWriterMessage::CommitTransaction {
      resolve: Box::new(|_| deferred.resolve(|_| Ok(()))),
    };
    database_handle
      .writer
      .send(message)
      .map_err(|err| napi_error(anyhow!("Failed to send {err}")))?;

    Ok(promise)
  }

  #[napi]
  pub fn close(&mut self) {
    self.inner = None;
  }
}

impl LMDB {
  fn get_database(&self) -> napi::Result<&Arc<DatabaseHandle>> {
    let inner = self
      .inner
      .as_ref()
      .ok_or_else(|| napi::Error::from_reason("Trying to use closed DB"))?;
    Ok(inner)
  }
}

#[cfg(test)]
mod test {
  use std::env::temp_dir;
  use std::sync::mpsc::channel;

  use super::*;

  #[test]
  fn create_database() {
    let options = LMDBOptions {
      path: temp_dir()
        .join("lmdb-cache-tests.db")
        .to_str()
        .unwrap()
        .to_string(),
      async_writes: false,
      map_size: None,
    };
    let mut lmdb = LMDB::new(options).unwrap();
    lmdb.close();
  }

  #[test]
  fn consistency_test() {
    let options = LMDBOptions {
      path: temp_dir()
        .join("lmdb-cache-tests.db")
        .to_str()
        .unwrap()
        .to_string(),
      async_writes: false,
      map_size: None,
    };
    let (write, read) = start_make_database_writer(&options).unwrap();
    let read_txn = read.read_txn().unwrap();

    write
      .send(DatabaseWriterMessage::StartTransaction {
        resolve: Box::new(|_| {}),
      })
      .unwrap();
    write
      .send(DatabaseWriterMessage::Put {
        key: String::from("key"),
        value: vec![1, 2, 3, 4],
        resolve: Box::new(|_| {}),
      })
      .unwrap();

    // If we don't commit the reader will not see the writes.
    let (tx, rx) = channel();
    write
      .send(DatabaseWriterMessage::CommitTransaction {
        resolve: Box::new(move |_| {
          tx.send(()).unwrap();
        }),
      })
      .unwrap();
    rx.recv().unwrap();

    let value = read.get(&read_txn, "key").unwrap().unwrap();
    assert_eq!(value, [1, 2, 3, 4]);
  }
}
