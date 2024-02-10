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
	pub blocks:     Vec<SuiTransactionBlockResponse>,
}

pub async fn insert_transaction_block_data(
	cfg: &AppConfig,
	block: &BlockDataCol,
	db: &Database,
) -> Result<(), mongodb::error::Error> {
	let collection = db.collection::<BlockDataCol>(&mongo_collection_name(cfg, "_blocks"));

	let blocks_bson = bson::Bson::Array(
		block
			.blocks
			.iter()
			.map(bson::to_bson)
			.collect::<bson::ser::Result<Vec<_>>>()
			.map_err(mongodb::error::ErrorKind::BsonSerialization)?,
	);

	// TODO: CRITICAL:
	// Inserting with conversion to i64 is not ideal. WIll occur overflow
	let filter = doc! { "checkpoint": block.checkpoint as i64 };
	let update = doc! { "$push": { "blocks": { "$each": blocks_bson  } } };

	let push_res = collection.update_one(filter, update, None).await;

	if let Ok(push_res) = push_res {
		// check if the filter matched any record. if not this means, this is the first
		// record for given checkpoint. So just insert it
		if push_res.matched_count == 0 {
			info!("Found first digest for checkpoint: {}", block.checkpoint);
			collection.insert_one(block, None).await?;
			Ok(())
		} else {
			// some record have been updated
			Ok(())
		}
	} else {
		push_res?;
		unreachable!()
	}
}
