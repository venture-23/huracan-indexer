use std::time::{SystemTime, UNIX_EPOCH};

use influxdb::{InfluxDbWriteable, Timestamp};
use tracing::{debug, info, warn};

use crate::conf::get_influx_singleton;

// Please see tag vs field docs before adding additional structs:
// https://docs.influxdata.com/influxdb/v2.7/write-data/best-practices/schema-design/#use-tags-and-fields

// Created new object in MongoDB.
// See: https://www.mongodb.com/docs/manual/reference/method/WriteResult/#mongodb-data-WriteResult.nUpserted
#[derive(InfluxDbWriteable)]
pub struct InsertObject {
	pub(crate) time:  Timestamp,
	pub(crate) count: i32,
}

// Modified or deleted an object in MongoDB.
// See: https://www.mongodb.com/docs/manual/reference/method/WriteResult/#mongodb-data-WriteResult.nModified
#[derive(InfluxDbWriteable)]
pub struct ModifiedObject {
	pub(crate) time:  Timestamp,
	pub(crate) count: i32,
}

// Attempted to update an object to MongoDB, but the item was identical.
// See: https://www.mongodb.com/docs/manual/reference/method/WriteResult/#mongodb-data-WriteResult.nMatched
// This is calculated in our code as Matched - Upserted - Modified.
// This is actually pretty common on Sui, since a transaction block may contain several objects,
// and not all are modified in a given transaction.
#[derive(InfluxDbWriteable)]
pub struct UnchangedObject {
	pub(crate) time:  Timestamp,
	pub(crate) count: i32,
}

// Error writing to MongoDB.
#[derive(InfluxDbWriteable)]
pub struct MongoWriteError {
	pub(crate) time: Timestamp,
}

pub async fn write_metric_mongo_write_error() {
	let influx_client = get_influx_singleton();
	let time = get_influx_timestamp_as_milliseconds().await;
	let influx_item = MongoWriteError { time }.into_query("mongo_write_error");
	let write_result = influx_client.query(influx_item).await;
	match write_result {
		Ok(string) => debug!(string),
		Err(error) => warn!("Could not write to influx: {}", error),
	}
}

// Finished processing a new checkpoint.
#[derive(InfluxDbWriteable)]
pub struct CreateCheckpoint {
	pub(crate) time:          Timestamp,
	pub(crate) checkpoint_id: u64,
}

pub async fn write_metric_create_checkpoint(checkpoint_id: u64) {
	let influx_client = get_influx_singleton();
	let time = get_influx_timestamp_as_milliseconds().await;
	let influx_item = CreateCheckpoint { time, checkpoint_id }.into_query("create_checkpoint");
	let write_result = influx_client.query(influx_item).await;
	match write_result {
		Ok(string) => debug!(string),
		Err(error) => warn!("Could not write to influx: {}", error),
	}
}

// Hit an error writing checkpoint data.
#[derive(InfluxDbWriteable)]
pub struct CheckpointError {
	pub(crate) time:          Timestamp,
	pub(crate) checkpoint_id: u64,
}

pub async fn write_metric_checkpoint_error(checkpoint_id: u64) {
	let influx_client = get_influx_singleton();
	let time = get_influx_timestamp_as_milliseconds().await;
	let influx_item = CheckpointError { time, checkpoint_id }.into_query("checkpoint_error");
	let write_result = influx_client.query(influx_item).await;
	match write_result {
		Ok(string) => debug!(string),
		Err(error) => warn!("Could not write to influx: {}", error),
	}
}

// Hit some kind of error during ingest.
#[derive(InfluxDbWriteable)]
pub struct IngestError {
	pub(crate) time:       Timestamp,
	pub(crate) object_id:  String,
	// This could be an enum, but not needed at the moment.
	#[influxdb(tag)]
	pub(crate) error_type: String,
}

pub async fn write_metric_ingest_error(object_id: String, error_type: String) {
	let influx_client = get_influx_singleton();
	let time = get_influx_timestamp_as_milliseconds().await;
	let influx_item = IngestError { time, object_id, error_type }.into_query("ingest_error");
	let write_result = influx_client.query(influx_item).await;
	match write_result {
		Ok(string) => debug!(string),
		Err(error) => warn!("Could not write to influx: {}", error),
	}
}

// Hit an RPC error.
// If this happens frequently, there may be an issue with the RPC provider.
#[derive(InfluxDbWriteable)]
pub struct RPCError {
	pub(crate) time:       Timestamp,
	#[influxdb(tag)]
	pub(crate) rpc_method: String,
}

pub async fn write_metric_rpc_error(rpc_method: String) {
	let influx_client = get_influx_singleton();
	let time = get_influx_timestamp_as_milliseconds().await;
	let influx_item = RPCError { time, rpc_method }.into_query("rpc_error");
	let write_result = influx_client.query(influx_item).await;
	match write_result {
		Ok(string) => debug!(string),
		Err(error) => warn!("Could not write to influx: {}", error),
	}
}

// Sent an RPC request.
#[derive(InfluxDbWriteable)]
pub struct RPCRequest {
	pub(crate) time:       Timestamp,
	#[influxdb(tag)]
	pub(crate) rpc_method: String,
}

pub async fn write_metric_rpc_request(rpc_method: String) {
	let influx_client = get_influx_singleton();
	let time = get_influx_timestamp_as_milliseconds().await;
	let influx_item = RPCRequest { time, rpc_method }.into_query("rpc_request");
	let write_result = influx_client.query(influx_item).await;
	match write_result {
		Ok(string) => debug!(string),
		Err(error) => warn!("Could not write to influx: {}", error),
	}
}

#[derive(InfluxDbWriteable)]
pub struct CheckpointsBehind {
	pub(crate) time:      Timestamp,
	pub(crate) behind_by: u64,
}

pub async fn write_metric_checkpoints_behind(behind_by: u64) {
	let influx_client = get_influx_singleton();
	let time = get_influx_timestamp_as_milliseconds().await;
	let influx_item = CheckpointsBehind { time, behind_by }.into_query("checkpoints_behind");
	let write_result = influx_client.query(influx_item).await;
	match write_result {
		Ok(string) => debug!(string),
		Err(error) => warn!("Could not write to influx: {}", error),
	}
}

#[derive(InfluxDbWriteable)]
pub struct CurrentCheckpoint {
	pub(crate) time:          Timestamp,
	pub(crate) checkpoint_id: u64,
}

pub async fn write_metric_current_checkpoint(checkpoint_id: u64) {
	let influx_client = get_influx_singleton();
	let time = get_influx_timestamp_as_milliseconds().await;
	let influx_item = CurrentCheckpoint { time, checkpoint_id }.into_query("current_checkpoint");
	let write_result = influx_client.query(influx_item).await;
	match write_result {
		Ok(string) => debug!(string),
		Err(error) => warn!("Could not write to influx: {}", error),
	}
}

// This is the final checkpoint handled by do_scan() and indexing will stop when this is published.
#[derive(InfluxDbWriteable)]
pub struct FinalCheckpoint {
	pub(crate) time:          Timestamp,
	pub(crate) checkpoint_id: u64,
}

pub async fn write_metric_final_checkpoint(checkpoint_id: u64) {
	let influx_client = get_influx_singleton();
	let time = get_influx_timestamp_as_milliseconds().await;
	let influx_item = FinalCheckpoint { time, checkpoint_id }.into_query("final_checkpoint");
	let write_result = influx_client.query(influx_item).await;
	match write_result {
		Ok(string) => debug!(string),
		Err(error) => warn!("Could not write to influx: {}", error),
	}
}

#[derive(InfluxDbWriteable)]
pub struct BackfillInit {
	pub(crate) time:             Timestamp,
	pub(crate) start_checkpoint: u64,
}

pub async fn write_metric_backfill_init(start_checkpoint: u64) {
	let influx_client = get_influx_singleton();
	let time = get_influx_timestamp_as_milliseconds().await;
	let influx_item = BackfillInit { time, start_checkpoint }.into_query("backfill_init");
	let write_result = influx_client.query(influx_item).await;
	match write_result {
		Ok(string) => debug!(string),
		Err(error) => warn!("Could not write to influx: {}", error),
	}
}

#[derive(InfluxDbWriteable)]
pub struct PauseLivescan {
	pub(crate) time:      Timestamp,
	pub(crate) behind_by: u64,
}

pub async fn write_metric_pause_livescan(behind_by: u64) {
	let influx_client = get_influx_singleton();
	let time = get_influx_timestamp_as_milliseconds().await;
	let influx_item = PauseLivescan { time, behind_by }.into_query("pause_livescan");
	let write_result = influx_client.query(influx_item).await;
	match write_result {
		Ok(string) => debug!(string),
		Err(error) => warn!("Could not write to influx: {}", error),
	}
}

#[derive(InfluxDbWriteable)]
pub struct StartLivescan {
	pub(crate) time: Timestamp,
}

pub async fn write_metric_start_livescan() {
	let influx_client = get_influx_singleton();
	let time = get_influx_timestamp_as_milliseconds().await;
	let influx_item = StartLivescan { time }.into_query("start_livescan");
	let write_result = influx_client.query(influx_item).await;
	match write_result {
		Ok(string) => debug!(string),
		Err(error) => warn!("Could not write to influx: {}", error),
	}
}

#[derive(InfluxDbWriteable)]
pub struct ExtractionLatency {
	pub(crate) time:       Timestamp,
	#[influxdb(tag)]
	pub(crate) source:     String,
	pub(crate) latency_ms: i32,
}

pub async fn write_metric_extraction_latency(source: String, latency_ms: i32) {
	let influx_client = get_influx_singleton();
	let time = get_influx_timestamp_as_milliseconds().await;
	let influx_item = ExtractionLatency { time, latency_ms, source }.into_query("extraction_latency");
	let write_result = influx_client.query(influx_item).await;
	match write_result {
		Ok(string) => debug!(string),
		Err(error) => warn!("Could not write to influx: {}", error),
	}
}

pub(crate) async fn get_influx_timestamp_as_milliseconds() -> Timestamp {
	let start = SystemTime::now();
	let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("Time went backwards").as_millis();
	return Timestamp::Milliseconds(since_the_epoch);
}
