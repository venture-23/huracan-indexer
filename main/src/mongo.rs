use std::borrow::Cow;

use bson::doc;
use influxdb::InfluxDbWriteable;
use mongodb::{error as mongoerror, options::UpdateOptions, Database};
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

const fn always_true<A>(_: &A) -> bool {
	true
}

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

const SUI_TYPES: &[&str] =
	&["0x2::kiosk::Kiosk", "0x2::kiosk::Item", "0x2::kiosk::Lock", "0x2::kiosk::Listing", "0x2::transfer_policy"];
const SUI_TYPES_COL_NAME: &[&str] = &["_kiosks", "_kiosks_item", "_kiosks_lock", "_kiosks_listing", "_transfer_policy"];

const OBJECT_TYPES_NAME: &[(&str, &str)] = &[
	("0x2::kiosk::Kiosk", "_kiosk"),
	("0x2::kiosk::Item", "_kiosk_item"),
	("0x2::kiosk::Lock", "_kiosk_lock"),
	("0x2::kiosk::Listing", "_kiosk_listing"),
	("0x2::transfer_policy", "transfer_policy"),
];

pub fn parse_object_type(object_type: &str) -> (String, String, String, Vec<String>) {
	let mut generics = Vec::new();
	let ty = if let Some((ty, terms)) = object_type.split_once('<') {
		let terms = &terms[..terms.len() - 1];
		for term in terms.split(",") {
			generics.push(term.trim_start().to_string());
		}
		ty
	} else {
		object_type
	};

	let mut it = ty.split("::");
	let package = it.next().unwrap().to_string();
	let module = it.next().unwrap().to_string();
	let class = it.next().unwrap().to_string();

	(package, module, class, generics)
}

pub fn object_col_name(cfg: &AppConfig, object_type: &str) -> String {
	let (package, module, class, generics) = parse_object_type(object_type);

	let generics = generics.iter().map(AsRef::as_ref).collect::<Vec<&str>>();
	match (package.as_str(), module.as_str(), class.as_str(), generics.as_slice()) {
		("0x2", "coin", "Coin", ["0x2::sui::SUI"]) => mongo_collection_name(cfg, "_sui_coin"),
		("0x2", "kiosk", "Kiosk", []) => mongo_collection_name(cfg, "_kiosks"),
		_ => mongo_collection_name(cfg, "_untyped"),
	}
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DigestCol<'parent> {
	// Sui-Sdk struct: https://mystenlabs.github.io/sui/sui_json_rpc_types/struct.SuiTransactionBlockResponse.html
	// [Accessed as of 12th Feb 2023]:
	// pub struct SuiTransactionBlockResponse {
	//      pub digest: TransactionDigest,
	//      pub transaction: Option<SuiTransactionBlock>,
	//      pub raw_transaction: Vec<u8>,
	//      pub effects: Option<SuiTransactionBlockEffects>,
	//      pub events: Option<SuiTransactionBlockEvents>,
	//      pub object_changes: Option<Vec<ObjectChange>>,
	//      pub balance_changes: Option<Vec<BalanceChange>>,
	//      pub timestamp_ms: Option<u64>,
	//      pub confirmed_local_execution: Option<bool>,
	//      pub checkpoint: Option<CheckpointSequenceNumber>,
	//      pub errors: Vec<String>,
	//      pub raw_effects: Vec<u8>,
	//  }
	pub transaction:               Cow<'parent, Option<sui_sdk::rpc_types::SuiTransactionBlock>>,
	pub raw_transaction:           Cow<'parent, Vec<u8>>,
	pub effects:                   Cow<'parent, Option<sui_sdk::rpc_types::SuiTransactionBlockEffects>>,
	pub events:                    Cow<'parent, Option<sui_sdk::rpc_types::SuiTransactionBlockEvents>>,
	pub balance_changes:           Cow<'parent, Option<Vec<sui_sdk::rpc_types::BalanceChange>>>,
	pub timestamp_ms:              Cow<'parent, Option<u64>>,
	pub confirmed_local_execution: Cow<'parent, Option<bool>>,
	pub checkpoint:                Cow<'parent, Option<u64>>,
	pub errors:                    Cow<'parent, Vec<String>>,
	pub digest:                    Cow<'parent, TransactionDigest>,
	// since object changes is already tracked seperately,
	// we dont need this:
	// TODO: remove the whole field instead
	#[serde(skip_serializing_if = "always_true")]
	#[serde(skip_deserializing)]
	pub object_changes:            Cow<'parent, Option<Vec<sui_sdk::rpc_types::ObjectChange>>>,
}

impl<'parent> DigestCol<'parent> {
	pub fn new(block_response: &'parent SuiTransactionBlockResponse) -> Self {
		let SuiTransactionBlockResponse {
			transaction,
			raw_transaction,
			effects,
			events,
			balance_changes,
			timestamp_ms,
			confirmed_local_execution,
			checkpoint,
			errors,
			digest,
			object_changes,
		} = block_response;
		Self {
			transaction:               Cow::Borrowed(transaction),
			raw_transaction:           Cow::Borrowed(raw_transaction),
			effects:                   Cow::Borrowed(effects),
			events:                    Cow::Borrowed(events),
			balance_changes:           Cow::Borrowed(balance_changes),
			timestamp_ms:              Cow::Borrowed(timestamp_ms),
			confirmed_local_execution: Cow::Borrowed(confirmed_local_execution),
			checkpoint:                Cow::Borrowed(checkpoint),
			errors:                    Cow::Borrowed(errors),
			digest:                    Cow::Borrowed(digest),
			object_changes:            Cow::Borrowed(object_changes),
		}
	}
}

pub async fn insert_transaction_block_data(
	cfg: &AppConfig,
	block: &DigestCol<'_>,
	db: &Database,
) -> Result<(), mongodb::error::Error> {
	let collection = db.collection::<bson::Document>(&mongo_collection_name(cfg, "_digests"));
	let encoded_digest = block.digest.base58_encode();

	let is_new_digest = collection.count_documents(doc! {"_id": &encoded_digest}, None).await? == 0;
	if is_new_digest {
		let document = doc! {
			"_id": encoded_digest,
			"digest": bson::to_bson(block).map_err(mongoerror::ErrorKind::BsonSerialization)?,
		};
		collection.insert_one(document, None).await.map(|_| ())
	} else {
		Ok(())
	}
}
