use pulsar::{
	authentication::oauth2::{OAuth2Authentication, OAuth2Params},
	Producer, Pulsar, TokioExecutor,
};
use tokio::sync::OnceCell;

use crate::_prelude::*;
use crate::conf::get_config_singleton;

pub async fn make_producer(topic_suffix: &str ) -> anyhow::Result<Producer<TokioExecutor>> {
	let client = get_pulsar_singleton();
	let cfg = get_config_singleton();
	Ok(client
		.producer()
		// e.g. {persistent://public/default/}{prod}_{testnet}_{objects}_{retries}
		// braces added for clarity of discerning between the different parts
		.with_topic(&format!("{}{}_{}_{}_{}", cfg.pulsar.topicbase, cfg.env, cfg.net, cfg.mongo.collectionbase, topic_suffix))
		.build()
		.await?)
}

pub async fn make_transaction_producer(topic_suffix: &str ) -> anyhow::Result<Producer<TokioExecutor>> {
	let client = get_pulsar_singleton();
	let cfg = get_config_singleton();
	Ok(client
        .producer()
        // e.g. {persistent://public/default/}{prod}_{testnet}_{your-variable}
        // braces added for clarity of discerning between the different parts
        .with_topic(&format!("{}{}_{}_{}_", cfg.pulsar.topicbase, cfg.env, cfg.net, topic_suffix))
        .build()
        .await?)
}


pub(crate) static PULSARCLIENT: OnceCell<Pulsar<TokioExecutor>> = OnceCell::const_new();

// Setup config singleton
pub async fn setup_pulsar_singleton() -> &'static Pulsar<TokioExecutor> {
	let cfg = get_config_singleton();
	PULSARCLIENT.get_or_init(|| async {
		Pulsar::builder(&cfg.pulsar.url, TokioExecutor)
			   // .with_auth_provider(OAuth2Authentication::client_credentials(OAuth2Params {
				  //  issuer_url:      cfg.pulsar.issuer.clone(),
				  //  credentials_url: cfg.pulsar.credentials.to_string().clone(),
				  //  audience:        Some(cfg.pulsar.audience.clone()),
				  //  scope:           None,
			   // }))
			   .build()
			   .await.unwrap()
	}).await
}

pub fn get_pulsar_singleton() -> &'static Pulsar<TokioExecutor> {
	PULSARCLIENT.get().expect("ConfigError: Pulsar Client singleton could not be loaded.")
}
