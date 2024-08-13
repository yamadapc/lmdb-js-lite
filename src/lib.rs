#![deny(clippy::all)]

use std::fmt::Debug;
use std::ops::DerefMut;
use std::rc::Rc;
use std::sync::Arc;

use anyhow::anyhow;
use napi::bindgen_prelude::Buffer;
use napi::bindgen_prelude::Env;
use napi::JsUnknown;
use napi_derive::napi;
use tracing::Level;

use crate::writer::{
  DatabaseWriter, DatabaseWriterHandle, DatabaseWriterMessage, start_make_database_writer,
};
use crate::writer::LMDBOptions;

pub mod writer;

fn napi_error(err: impl Debug) -> napi::Error {
  napi::Error::from_reason(format!("[napi] {err:?}"))
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

#[napi]
pub struct LMDB {
  inner: Option<(Rc<DatabaseWriterHandle>, Arc<DatabaseWriter>)>,
  read_transaction: Option<heed::RoTxn<'static>>,
}

#[napi]
impl LMDB {
  #[napi(constructor)]
  pub fn new(options: LMDBOptions) -> napi::Result<Self> {
    let (database_wrapper, writer) = start_make_database_writer(options).map_err(napi_error)?;
    Ok(Self {
      inner: Some((Rc::new(database_wrapper), writer)),
      read_transaction: None,
    })
  }

  #[napi(ts_return_type = "Promise<Buffer | null | undefined>")]
  pub fn get(&self, env: Env, key: String) -> napi::Result<napi::JsObject> {
    let (inner, _) = self.get_database()?;
    let (deferred, promise) = env.create_deferred()?;

    inner
      .tx
      .send(DatabaseWriterMessage::Get {
        key,
        resolve: Box::new(|value| {
          deferred.resolve(|_| {
            let value = value.map_err(napi_error)?;
            Ok(value.map(|buffer| Buffer::from(buffer)))
          })
        }),
      })
      .map_err(|err| napi_error(anyhow!("Failed to send {err}")))?;

    Ok(promise)
  }

  #[napi(ts_return_type = "ArrayBuffer | null")]
  pub fn get_sync(&self, env: Env, key: String) -> napi::Result<JsUnknown> {
    let (_, database) = self.get_database()?;

    if let Some(txn) = &self.read_transaction {
      let buffer = database.database.get(&txn, &key);
      let Some(buffer) = buffer.map_err(|err| napi_error(anyhow!(err)))? else {
        return Ok(env.get_null()?.into_unknown());
      };
      let mut result = env.create_arraybuffer(buffer.len())?;
      result.deref_mut().copy_from_slice(buffer);
      Ok(result.into_unknown())
    } else {
      let txn = database
        .environment
        .read_txn()
        .map_err(|err| napi_error(anyhow!(err)))?;
      let buffer = database.database.get(&txn, &key);
      let Some(buffer) = buffer.map_err(|err| napi_error(anyhow!(err)))? else {
        return Ok(env.get_null()?.into_unknown());
      };
      let mut result = env.create_arraybuffer(buffer.len())?;
      result.deref_mut().copy_from_slice(buffer);
      Ok(result.into_unknown())
    }
  }

  #[napi]
  pub fn get_many_sync(&self, keys: Vec<String>) -> napi::Result<Vec<Option<Buffer>>> {
    let (_, database) = self.get_database()?;

    let mut results = vec![];
    let txn = database
      .environment
      .read_txn()
      .map_err(|err| napi_error(anyhow!(err)))?;

    for key in keys {
      let buffer = database
        .get(&txn, &key)
        .map_err(|err| napi_error(anyhow!(err)))?
        .map(|data| Buffer::from(data));
      results.push(buffer);
    }

    Ok(results)
  }

  #[napi(ts_return_type = "Promise<void>")]
  pub fn put_many(&self, env: Env, entries: Vec<Entry>) -> napi::Result<napi::JsObject> {
    let (inner, _) = self.get_database()?;
    let (deferred, promise) = env.create_deferred()?;

    let message = DatabaseWriterMessage::PutMany {
      entries,
      resolve: Box::new(|value| {
        deferred.resolve(|_| value.map_err(|err| napi_error(anyhow!("Failed to write {err}"))))
      }),
    };
    inner
      .tx
      .send(message)
      .map_err(|err| napi_error(anyhow!("Failed to send {err}")))?;

    Ok(promise)
  }

  #[napi(ts_return_type = "Promise<void>")]
  pub fn put(&self, env: Env, key: String, data: Buffer) -> napi::Result<napi::JsObject> {
    let (inner, _) = self.get_database()?;
    // This costs us 70% over the round-trip time after arg. conversion
    let (deferred, promise) = env.create_deferred()?;

    let message = DatabaseWriterMessage::Put {
      key,
      value: data.to_vec(),
      resolve: Box::new(|value| {
        deferred.resolve(|_| value.map_err(|err| napi_error(anyhow!("Failed to write {err}"))))
      }),
    };
    inner
      .tx
      .send(message)
      .map_err(|err| napi_error(anyhow!("Failed to send {err}")))?;

    Ok(promise)
  }

  #[napi]
  pub fn put_no_confirm(&self, key: String, data: Buffer) -> napi::Result<()> {
    let (inner, _) = self.get_database()?;

    let message = DatabaseWriterMessage::Put {
      key,
      value: data.to_vec(),
      resolve: Box::new(|_| {}),
    };
    inner
      .tx
      .send(message)
      .map_err(|err| napi_error(anyhow!("Failed to send {err}")))?;

    Ok(())
  }

  #[napi]
  pub fn start_read_transaction(&mut self) -> napi::Result<()> {
    if self.read_transaction.is_some() {
      return Ok(());
    }
    let (_, database) = self.get_database()?;
    let txn = database
      .environment
      .clone()
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
    let (inner, _) = self.get_database()?;
    let (deferred, promise) = env.create_deferred()?;

    let message = DatabaseWriterMessage::StartTransaction {
      resolve: Box::new(|_| deferred.resolve(|_| Ok(()))),
    };
    inner
      .tx
      .send(message)
      .map_err(|err| napi_error(anyhow!("Failed to send {err}")))?;

    Ok(promise)
  }

  #[napi(ts_return_type = "Promise<void>")]
  pub fn commit_write_transaction(&self, env: Env) -> napi::Result<napi::JsObject> {
    let (inner, _) = self.get_database()?;
    let (deferred, promise) = env.create_deferred()?;

    let message = DatabaseWriterMessage::CommitTransaction {
      resolve: Box::new(|_| deferred.resolve(|_| Ok(()))),
    };
    inner
      .tx
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
  fn get_database(&self) -> napi::Result<&(Rc<DatabaseWriterHandle>, Arc<DatabaseWriter>)> {
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
    };
    let mut lmdb = LMDB::new(options).unwrap();
    lmdb.close();
  }
}
