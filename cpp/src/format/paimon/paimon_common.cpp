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

#include "milvus-storage/format/paimon/paimon_common.h"

#include <cstdlib>

#include <arrow/util/base64.h>

namespace milvus_storage::paimon {

arrow::Result<std::unordered_map<std::string, std::string>> ToStorageOptions(const ArrowFileSystemConfig& config) {
  std::unordered_map<std::string, std::string> options;
  auto set = [&options](const std::string& key, const std::string& value) {
    if (!value.empty()) {
      options[key] = value;
    }
  };
  auto endpoint = [&config]() {
    return config.address.empty() ? std::string{} : StorageUri::BuildEndpointUrl(config.address, config.use_ssl);
  };
  auto set_credential_cache_key = [&]() { options["milvus_fs_cache_key"] = config.GetCacheKey(); };

  if (config.storage_type == "local") {
    return options;
  }
  options["cloud_provider"] = config.cloud_provider;
  if (config.cloud_provider == kCloudProviderAWS) {
    set("s3.endpoint", endpoint());
    set("s3.region", config.region);
    options["s3.path-style-access"] = config.use_virtual_host ? "false" : "true";
    if (!config.role_arn.empty()) {
      set_credential_cache_key();
      set("client.assume-role.arn", config.role_arn);
      set("client.assume-role.session-name", config.session_name);
      set("client.assume-role.external-id", config.external_id);
      if (config.load_frequency > 0) {
        options["aws_credential_refresh_secs"] = std::to_string(config.load_frequency);
      }
    } else if (!config.use_iam) {
      set("s3.access-key", config.access_key_id);
      set("s3.secret-key", config.access_key_value);
    }
    return options;
  }
  if (config.cloud_provider == kCloudProviderAliyun) {
    if (!config.role_arn.empty()) {
      set_credential_cache_key();
      set("oss.endpoint", endpoint());
      set("oss.region", config.region);
      set("oss.role-arn", config.role_arn);
      set("oss.role-session-name", config.session_name);
      set("oss.external-id", config.external_id);
      if (config.load_frequency > 0) {
        options["oss_credential_refresh_secs"] = std::to_string(config.load_frequency);
      }
    } else {
      set("fs.oss.endpoint", endpoint());
      set("fs.oss.accessKeyId", config.access_key_id);
      set("fs.oss.accessKeySecret", config.access_key_value);
    }
    return options;
  }
  if (config.cloud_provider == kCloudProviderAzure) {
    if (!config.address.empty()) {
      auto address = config.address;
      if (address.find("://") != std::string::npos || address.starts_with(config.access_key_id + ".")) {
        options["azure.endpoint"] = StorageUri::BuildEndpointUrl(address, config.use_ssl);
      } else {
        const auto* azurite = std::getenv("USE_AZURITE");
        const bool path_style = azurite && std::string(azurite) == "true";
        if (!path_style && !address.starts_with(".")) {
          address = ".dfs." + address;
        }
        options["azure.endpoint"] =
            StorageUri::BuildAzureEndpointAddress(address, config.access_key_id, config.use_ssl);
      }
    }
    set("azure.account-name", config.access_key_id);
    if (config.IsAzureCredentialBrokerEnabled()) {
      set_credential_cache_key();
      set("azure_broker_endpoint", config.azure_credential_endpoint);
      set("azure_broker_client_id", config.azure_client_id);
      set("azure_broker_tenant_id", config.azure_tenant_id);
      set("azure_broker_account_name", config.access_key_id);
      set("azure_broker_region", config.region);
      set("azure_broker_bucket", config.bucket_name);
      options["azure_broker_duration_seconds"] = std::to_string(config.load_frequency);
      options["azure_broker_request_timeout_ms"] = std::to_string(config.request_timeout_ms);
    } else if (config.use_iam) {
      auto nonempty_env = [](const char* name) -> const char* {
        const auto* value = std::getenv(name);
        return value && *value != '\0' ? value : nullptr;
      };
      const auto* client_id = nonempty_env("AZURE_CLIENT_ID");
      const auto* client_secret = nonempty_env("AZURE_CLIENT_SECRET");
      const auto* tenant_id = nonempty_env("AZURE_TENANT_ID");
      if (nonempty_env("AZURE_FEDERATED_TOKEN_FILE") && !(client_id && client_secret && tenant_id)) {
        return arrow::Status::NotImplemented("Paimon Azure workload identity credentials are not supported");
      }
      if (client_id)
        set("azure.client-id", client_id);
      if (client_secret)
        set("azure.client-secret", client_secret);
      if (tenant_id)
        set("azure.tenant-id", tenant_id);
      if (const auto* value = nonempty_env("AZURE_AUTHORITY_HOST"))
        set("azure.authority-host", value);
    } else {
      set("azure.account-key", config.access_key_value);
    }
    return options;
  }
  if (config.cloud_provider == kCloudProviderGCP) {
    set("gcs.endpoint", endpoint());
    if (config.use_iam && !config.gcp_target_service_account.empty()) {
      set_credential_cache_key();
      set("gcs.service-account", config.gcp_target_service_account);
      if (config.load_frequency > 0) {
        options["gcp_credential_refresh_secs"] = std::to_string(config.load_frequency);
      }
    } else {
      set("gcs.credential", arrow::util::base64_encode(config.gcp_credential_json));
      if (config.gcp_native_without_auth) {
        options["gcs.allow-anonymous"] = "true";
      }
    }
    return options;
  }
  return arrow::Status::NotImplemented("Unsupported cloud provider for Paimon: ", config.cloud_provider);
}

std::string ToStandardUri(const std::string& milvus_uri) {
  auto parsed = StorageUri::Parse(milvus_uri);
  if (!parsed.ok() || parsed->scheme.empty()) {
    return milvus_uri;
  }
  auto result = StorageUri::Make(parsed.ValueOrDie(), false);
  return result.ok() ? result.ValueOrDie() : milvus_uri;
}

std::string ToMilvusUri(const std::string& standard_uri, const std::string& address) {
  if (address.empty()) {
    return standard_uri;
  }
  auto parsed = StorageUri::Parse(standard_uri, false);
  if (!parsed.ok() || parsed->scheme.empty()) {
    return standard_uri;
  }
  parsed->address = address;
  auto result = StorageUri::Make(parsed.ValueOrDie());
  return result.ok() ? result.ValueOrDie() : standard_uri;
}

}  // namespace milvus_storage::paimon
