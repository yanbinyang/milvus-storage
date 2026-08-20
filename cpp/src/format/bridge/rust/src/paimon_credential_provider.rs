// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Resolve delegated cloud credentials into paimon-rust storage options.

use anyhow::{Result, anyhow};
use object_store::CredentialProvider;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use url::Url;

use crate::aliyun_oss_provider::{
    RefreshableAliyunOssStore, build_oss_config_from_iceberg_opts, parse_oss_credential_duration,
};
use crate::aws_arn_provider::AssumeRoleConfig;
use crate::azure_sas_provider::{AzureBrokerConfig, AzureSasStorageOptionsProvider};
use crate::cloud_provider_cache::{CACHE_CAPACITY, CACHE_KEY, GlobalLruCache};
use crate::gcp_impersonation::{
    ICEBERG_TARGET_SERVICE_ACCOUNT, ImpersonatingGcsCredentialProvider,
    REFRESH_OFFSET_SECS as GCP_REFRESH_OFFSET_SECS, TOKEN_LIFETIME_SECONDS,
};

pub(crate) const ERROR_INVALID_PREFIX: &str = "[paimon:error=invalid]";

const CLOUD_PROVIDER_KEY: &str = "cloud_provider";
const AWS_ROLE_ARN: &str = "client.assume-role.arn";
const AWS_SESSION_NAME: &str = "client.assume-role.session-name";
const AWS_EXTERNAL_ID: &str = "client.assume-role.external-id";
const AWS_CREDENTIAL_REFRESH_SECS: &str = "aws_credential_refresh_secs";

static AWS_PROVIDER_CACHE: LazyLock<GlobalLruCache<object_store::aws::AwsCredentialProvider>> =
    LazyLock::new(|| GlobalLruCache::new(CACHE_CAPACITY));
static ALIYUN_PROVIDER_CACHE: LazyLock<GlobalLruCache<Arc<RefreshableAliyunOssStore>>> =
    LazyLock::new(|| GlobalLruCache::new(CACHE_CAPACITY));
static GCP_PROVIDER_CACHE: LazyLock<GlobalLruCache<Arc<ImpersonatingGcsCredentialProvider>>> =
    LazyLock::new(|| GlobalLruCache::new(CACHE_CAPACITY));
static AZURE_PROVIDER_CACHE: LazyLock<GlobalLruCache<Arc<AzureSasStorageOptionsProvider>>> =
    LazyLock::new(|| GlobalLruCache::new(CACHE_CAPACITY));

fn invalid_config(message: impl std::fmt::Display) -> anyhow::Error {
    anyhow!("{ERROR_INVALID_PREFIX} {message}")
}

fn take_u64(options: &mut HashMap<String, String>, key: &str) -> Result<u64> {
    options
        .remove(key)
        .unwrap_or_default()
        .parse::<u64>()
        .map_err(|error| invalid_config(format_args!("invalid {key}: {error}")))
}

async fn prepare_aws_storage_options(
    mut options: HashMap<String, String>,
    cache_key: Option<&str>,
) -> Result<HashMap<String, String>> {
    let role_arn = options.remove(AWS_ROLE_ARN).unwrap_or_default();
    let session_name = options.remove(AWS_SESSION_NAME).unwrap_or_default();
    let external_id = options.remove(AWS_EXTERNAL_ID).unwrap_or_default();
    if role_arn.is_empty() {
        options.remove(AWS_CREDENTIAL_REFRESH_SECS);
        return Ok(options);
    }

    let region = options.get("s3.region").cloned().unwrap_or_default();
    let refresh_secs = take_u64(&mut options, AWS_CREDENTIAL_REFRESH_SECS)?;
    let config = AssumeRoleConfig::parse(
        &role_arn,
        &session_name,
        &external_id,
        &region,
        refresh_secs,
    )
    .map_err(|error| invalid_config(format_args!("invalid AWS AssumeRole configuration: {error}")))?
    .expect("non-empty role ARN produces an AssumeRole configuration");
    let provider = match cache_key.filter(|key| !key.is_empty()) {
        Some(cache_key) => {
            AWS_PROVIDER_CACHE
                .get(cache_key, || async {
                    config
                        .build_credentials()
                        .await
                        .map_err(anyhow::Error::from)
                })
                .await?
        }
        None => config.build_credentials().await?,
    };
    let credential = provider.get_credential().await?;
    options.insert("s3.access-key".to_string(), credential.key_id.clone());
    options.insert("s3.secret-key".to_string(), credential.secret_key.clone());
    if let Some(token) = &credential.token {
        options.insert("s3.session.token".to_string(), token.clone());
    }
    Ok(options)
}

async fn prepare_aliyun_storage_options(
    uri: &str,
    mut options: HashMap<String, String>,
    cache_key: Option<&str>,
) -> Result<HashMap<String, String>> {
    if !options.contains_key("oss.role-arn") {
        options.remove("oss_credential_refresh_secs");
        return Ok(options);
    }

    let url = Url::parse(uri)
        .map_err(|error| invalid_config(format_args!("invalid Paimon OSS URI '{uri}': {error}")))?;
    let bucket = url
        .host_str()
        .filter(|bucket| !bucket.is_empty())
        .ok_or_else(|| invalid_config(format_args!("Paimon OSS URI has no bucket: {uri}")))?
        .to_string();
    let config = build_oss_config_from_iceberg_opts(bucket, &url, &options).map_err(|error| {
        invalid_config(format_args!("invalid Aliyun role configuration: {error}"))
    })?;
    let duration = parse_oss_credential_duration(&options).map_err(|error| {
        invalid_config(format_args!("invalid Aliyun role configuration: {error}"))
    })?;
    let provider = match cache_key.filter(|key| !key.is_empty()) {
        Some(cache_key) => {
            ALIYUN_PROVIDER_CACHE
                .get(cache_key, || async {
                    Ok::<_, anyhow::Error>(Arc::new(RefreshableAliyunOssStore::new(
                        config, duration,
                    )))
                })
                .await?
        }
        None => Arc::new(RefreshableAliyunOssStore::new(config, duration)),
    };
    let credential = provider.current_config().await?;
    for key in [
        "oss.endpoint",
        "oss.region",
        "oss.role-arn",
        "oss.role-session-name",
        "oss.external-id",
        "oss_credential_refresh_secs",
    ] {
        options.remove(key);
    }
    for (source, target) in [
        ("endpoint", "fs.oss.endpoint"),
        ("access_key_id", "fs.oss.accessKeyId"),
        ("access_key_secret", "fs.oss.accessKeySecret"),
        ("security_token", "fs.oss.securityToken"),
    ] {
        if let Some(value) = credential.get(source).filter(|value| !value.is_empty()) {
            options.insert(target.to_string(), value.clone());
        }
    }
    Ok(options)
}

async fn prepare_gcp_storage_options(
    mut options: HashMap<String, String>,
    cache_key: Option<&str>,
) -> Result<HashMap<String, String>> {
    let target_sa = options
        .remove(ICEBERG_TARGET_SERVICE_ACCOUNT)
        .unwrap_or_default();
    let token_lifetime_secs = options
        .remove(TOKEN_LIFETIME_SECONDS)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    if target_sa.is_empty() {
        return Ok(options);
    }
    if !(900..=3600).contains(&token_lifetime_secs) {
        return Err(invalid_config(format_args!(
            "invalid GCP impersonation configuration: gcp_credential_refresh_secs must be in [900, 3600], got {token_lifetime_secs}"
        )));
    }
    let provider = match cache_key.filter(|key| !key.is_empty()) {
        Some(cache_key) => {
            let target_sa = target_sa.clone();
            GCP_PROVIDER_CACHE
                .get(cache_key, || async move {
                    new_gcp_credential_provider(target_sa, token_lifetime_secs).await
                })
                .await?
        }
        None => new_gcp_credential_provider(target_sa, token_lifetime_secs).await?,
    };
    let credential = provider.get_credential().await?;
    options.insert("gcs.token".to_string(), credential.bearer.clone());
    // The delegated identity is explicit. Do not fall back to a VM or local
    // credential if the supplied impersonated token cannot be used.
    options.insert("gcs.disable-vm-metadata".to_string(), "true".to_string());
    options.insert("gcs.disable-config-load".to_string(), "true".to_string());
    Ok(options)
}

async fn new_gcp_credential_provider(
    target_sa: String,
    token_lifetime_secs: u64,
) -> Result<Arc<ImpersonatingGcsCredentialProvider>> {
    let provider = Arc::new(ImpersonatingGcsCredentialProvider::new(
        target_sa,
        Duration::from_secs(token_lifetime_secs),
        Duration::from_secs(GCP_REFRESH_OFFSET_SECS),
    ));
    provider.get_credential().await?;
    Ok(provider)
}

async fn prepare_azure_storage_options(
    mut options: HashMap<String, String>,
    cache_key: Option<&str>,
) -> Result<HashMap<String, String>> {
    let config = AzureBrokerConfig::extract(&mut options).map_err(|error| {
        invalid_config(format_args!("invalid Azure broker configuration: {error}"))
    })?;
    let Some(config) = config else {
        return Ok(options);
    };
    let provider = match cache_key.filter(|key| !key.is_empty()) {
        Some(cache_key) => {
            AZURE_PROVIDER_CACHE
                .get(cache_key, || async {
                    AzureSasStorageOptionsProvider::new(config)
                        .map(Arc::new)
                        .map_err(anyhow::Error::from)
                })
                .await?
        }
        None => Arc::new(AzureSasStorageOptionsProvider::new(config)?),
    };
    let credential = provider.current_credential().await?;
    options.insert("azure.sas-token".to_string(), credential.token.clone());
    Ok(options)
}

/// Resolve bridge-private delegated identity options into the concrete,
/// short-lived credential fields understood by paimon-rust/OpenDAL.
pub(crate) async fn prepare_cloud_storage_options(
    uri: &str,
    mut options: HashMap<String, String>,
) -> Result<HashMap<String, String>> {
    let provider = options.remove(CLOUD_PROVIDER_KEY);
    let cache_key = options.remove(CACHE_KEY);
    match provider.as_deref() {
        None => Ok(options),
        Some("aws") => prepare_aws_storage_options(options, cache_key.as_deref()).await,
        Some("aliyun") => {
            prepare_aliyun_storage_options(uri, options, cache_key.as_deref()).await
        }
        Some("gcp") => prepare_gcp_storage_options(options, cache_key.as_deref()).await,
        Some("azure") => prepare_azure_storage_options(options, cache_key.as_deref()).await,
        Some(provider) => Err(invalid_config(format_args!(
            "unsupported Paimon cloud provider: {provider}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn static_cloud_options_only_drop_bridge_keys() {
        let options = HashMap::from([
            (CLOUD_PROVIDER_KEY.to_string(), "aws".to_string()),
            (CACHE_KEY.to_string(), "fs:1".to_string()),
            (
                "s3.endpoint".to_string(),
                "https://s3.example.com".to_string(),
            ),
        ]);
        let prepared = prepare_cloud_storage_options("s3://bucket/table", options)
            .await
            .unwrap();
        assert_eq!(prepared["s3.endpoint"], "https://s3.example.com");
        assert!(!prepared.contains_key(CLOUD_PROVIDER_KEY));
        assert!(!prepared.contains_key(CACHE_KEY));

        let error = prepare_cloud_storage_options(
            "s3://bucket/table",
            HashMap::from([(CLOUD_PROVIDER_KEY.to_string(), "unknown".to_string())]),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains(ERROR_INVALID_PREFIX));
    }

    #[tokio::test]
    async fn gcp_impersonation_options_match_shared_provider_validation() {
        let prepared = prepare_gcp_storage_options(
            HashMap::from([(
                TOKEN_LIFETIME_SECONDS.to_string(),
                "invalid".to_string(),
            )]),
            None,
        )
        .await
        .unwrap();
        assert!(!prepared.contains_key(TOKEN_LIFETIME_SECONDS));

        let error = prepare_gcp_storage_options(
            HashMap::from([
                (
                    ICEBERG_TARGET_SERVICE_ACCOUNT.to_string(),
                    "target@example.com".to_string(),
                ),
                (TOKEN_LIFETIME_SECONDS.to_string(), "899".to_string()),
            ]),
            None,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains(ERROR_INVALID_PREFIX));
        assert!(error.to_string().contains("must be in [900, 3600]"));
    }
}
