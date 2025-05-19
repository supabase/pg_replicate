//! PostgreSQL database connection utilities for all crates.
//!
//! This crate provides database connection options and utilities for working with PostgreSQL.
//! It supports both the [`sqlx`] and [`tokio-postgres`] crates through feature flags.
//!
//! # Features
//!
//! - `sqlx`: Enables SQLx-specific database connection options and utilities
//! - `tokio`: Enables tokio-postgres-specific database connection options and utilities
//! - `test-utils`: Enables test utilities for both SQLx and tokio-postgres implementations

pub mod schema;
#[cfg(feature = "sqlx")]
pub mod sqlx;
#[cfg(feature = "tokio")]
pub mod tokio;
