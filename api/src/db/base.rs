use crate::encryption::{DecryptionError, EncryptionError, EncryptionKey};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ToDbError {
    #[error("An encryption error happened while converting to the db variant: {0}")]
    Encryption(#[from] EncryptionError),
}

#[derive(Debug, Error)]
pub enum ToMemoryError {
    #[error("A decryption error happened while converting to the memory variant: {0}")]
    Decryption(#[from] DecryptionError),
}

pub trait ToDb<T> {
    fn to_db_type(self, encryption_key: &EncryptionKey) -> Result<T, ToDbError>;
}

impl<T> ToDb<T> for T {
    fn to_db_type(self, _: &EncryptionKey) -> Result<T, ToDbError> {
        Ok(self)
    }
}

pub trait ToMemory<T> {
    fn to_memory_type(self, encryption_key: &EncryptionKey) -> Result<T, ToMemoryError>;
}

impl<T> ToMemory<T> for T {
    fn to_memory_type(self, _: &EncryptionKey) -> Result<T, ToMemoryError> {
        Ok(self)
    }
}

#[derive(Debug, Error)]
pub enum DbSerializationError {
    #[error("Error while serializing data to the db: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("An error occurred while converting to the db representation: {0}")]
    ToDb(#[from] ToDbError),
}

#[derive(Debug, Error)]
pub enum DbDeserializationError {
    #[error("Error while deserializing data from the db: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("An error occurred while converting to the memory representation: {0}")]
    ToMemory(#[from] ToMemoryError),
}

pub fn serialize_to_db_as_json<T, S>(
    value: T,
    encryption_key: &EncryptionKey,
) -> Result<serde_json::Value, DbSerializationError>
where
    T: ToDb<S>,
    S: Serialize,
{
    let value = value.to_db_type(encryption_key)?;
    let serialized_value = serde_json::to_value(value)?;

    Ok(serialized_value)
}

pub fn deserialize_from_db_json_str<T, S>(
    value: &str,
    encryption_key: &EncryptionKey,
) -> Result<S, DbDeserializationError>
where
    T: ToMemory<S>,
    T: DeserializeOwned,
{
    let deserialized_value: T = serde_json::from_str(value)?;
    let value = deserialized_value.to_memory_type(encryption_key)?;

    Ok(value)
}

pub fn deserialize_from_db_json_value<T, S>(
    value: serde_json::Value,
    encryption_key: &EncryptionKey,
) -> Result<S, DbDeserializationError>
where
    T: ToMemory<S>,
    T: DeserializeOwned,
{
    let deserialized_value: T = serde_json::from_value(value)?;
    let value = deserialized_value.to_memory_type(encryption_key)?;

    Ok(value)
}
