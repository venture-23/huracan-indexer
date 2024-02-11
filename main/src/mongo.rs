use std::borrow::Cow;

use bson::doc;
use influxdb::InfluxDbWriteable;
use mongodb::{options::UpdateOptions, Database};
use sui_sdk::rpc_types::{
	SuiProgrammableMoveCall, SuiTransactionBlock, SuiTransactionBlockData, SuiTransactionBlockDataV1,
	SuiTransactionBlockResponse,
};
use sui_types::{
	digests::TransactionDigest, messages_checkpoint::CheckpointSequenceNumber, signature::GenericSignature,
};

use crate::{
	_prelude::*,
	influx::{write_metric_checkpoint_error, write_metric_create_checkpoint, write_metric_mongo_write_error},
};

#[derive(Serialize, Deserialize)]
pub struct Checkpoint {
	// TODO mongo u64 issue
	pub _id:  u64,
	// marks the oldest checkpoint we need to look at, with the assurance that every checkpoint
	// older than this one has already been completed, even if we may not store that info otherwise
	pub stop: Option<bool>,
}

pub fn mongo_collection_name(cfg: &AppConfig, suffix: &str) -> String {
	format!("{}_{}_{}{}", cfg.env, cfg.net, cfg.mongo.collectionbase, suffix)
}

pub async fn mongo_checkpoint(cfg: &AppConfig, pc: &PipelineConfig, db: &Database, cp: CheckpointSequenceNumber) {
	let mut retries_left = pc.mongo.retries;
	loop {
		if let Err(err) = db
			.run_command(
				doc! {
					// e.g. prod_testnet_objects_checkpoints
					"update": mongo_collection_name(&cfg, "_checkpoints"),
					"updates": vec![
						doc! {
							// FIXME how do we store a u64 in mongo? this will be an issue when the chain
							//		 has been running for long enough!
							"q": doc! { "_id": cp as i64 },
							"u": doc! { "_id": cp as i64 },
							"upsert": true,
						}
					]
				},
				None,
			)
			.await
		{
			warn!("failed saving checkpoint to mongo: {:?}", err);
			write_metric_mongo_write_error().await;
			write_metric_checkpoint_error(cp as u64).await;
			if retries_left > 0 {
				retries_left -= 1;
				continue
			}
			error!(error = ?err, "checkpoint {} fully completed, but could not save checkpoint status to mongo!", cp);
		}
		// At this point, we have successfully saved the checkpoint to MongoDB.
		write_metric_create_checkpoint(cp as u64).await;
		break
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockDataCol {
	pub checkpoint: CheckpointSequenceNumber,
	// This is vec only to support deserialization
	// While inserting ( serialization ), always expect to .len()==1
	// see: insert_transaction_block_data for more
	pub blocks:     Vec<SuiTransactionBlockResponse>,
}

pub async fn insert_transaction_block_data(
	cfg: &AppConfig,
	block: &BlockDataCol,
	db: &Database,
) -> Result<(), mongodb::error::Error> {
	// We only insert one digest at once
	// mongodb query below depends on this assumption
	assert!(block.blocks.len() == 1, "Cam only insert one digest at once");
	let collection = db.collection::<BlockDataCol>(&mongo_collection_name(cfg, "_blocks"));

	let bson_block = match block.blocks.first() {
		None => bson::Bson::Document(doc! {}),
		Some(bl) => bson::to_bson(&bl).map_err(mongodb::error::ErrorKind::BsonSerialization)?,
	};

	// TODO: CRITICAL:
	// Inserting with conversion to i64 is not ideal. WIll occur overflow
	let find_cp = doc! { "checkpoint": block.checkpoint as i64 };
	let is_new_cp = collection.count_documents(find_cp, None).await? == 0;
	if is_new_cp {
		// This is new cp, just insert the whole block with checkpointSequenceNumber
		info!("Found first digest for checkpoint: {}", block.checkpoint);
		collection.insert_one(block, None).await?;
		Ok(())
	} else {
		// This checkpoint have already some other digest
		// we have to only insert if this digest is unique
		// so put this check in filter as well
		let filter = doc! {
			"checkpoint": { "$eq": block.checkpoint as i64 },
			"blocks": { "$ne": &bson_block }
		};

		let update = doc! { "$push": { "blocks": &bson_block } };
		collection.update_one(filter, update, None).await?;
		Ok(())
	}
}
