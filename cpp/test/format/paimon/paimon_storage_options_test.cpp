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

#include <cstdlib>
#include <optional>
#include <string>

#include <gtest/gtest.h>

#include "milvus-storage/format/paimon/paimon_common.h"
#include "test_env.h"

namespace milvus_storage::paimon::test {
namespace {

class ScopedEnvVar {
  public:
  ScopedEnvVar(const char* name, const char* value) : name_(name) {
    if (const auto* old_value = std::getenv(name)) {
      old_value_ = old_value;
    }
    if (value) {
      setenv(name, value, 1);
    } else {
      unsetenv(name);
    }
  }

  ~ScopedEnvVar() { old_value_ ? setenv(name_.c_str(), old_value_->c_str(), 1) : unsetenv(name_.c_str()); }

  ScopedEnvVar(const ScopedEnvVar&) = delete;
  ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;

  private:
  std::string name_;
  std::optional<std::string> old_value_;
};

ArrowFileSystemConfig MakeAwsConfig() {
  ArrowFileSystemConfig config;
  config.storage_type = "remote";
  config.cloud_provider = kCloudProviderAWS;
  config.access_key_id = "access-key";
  config.access_key_value = "secret-key";
  config.region = "us-west-2";
  config.address = "s3.us-west-2.amazonaws.com";
  config.use_ssl = true;
  return config;
}

TEST(PaimonStorageOptionsTest, AwsStaticCredentialsAndAddressing) {
  ASSERT_AND_ASSIGN(auto options, ToStorageOptions(MakeAwsConfig()));
  EXPECT_EQ(options.at("s3.access-key"), "access-key");
  EXPECT_EQ(options.at("s3.secret-key"), "secret-key");
  EXPECT_EQ(options.at("s3.region"), "us-west-2");
  EXPECT_EQ(options.at("s3.endpoint"), "https://s3.us-west-2.amazonaws.com");
  EXPECT_EQ(options.at("s3.path-style-access"), "true");

  auto virtual_host = MakeAwsConfig();
  virtual_host.use_virtual_host = true;
  ASSERT_AND_ASSIGN(auto virtual_host_options, ToStorageOptions(virtual_host));
  EXPECT_EQ(virtual_host_options.at("s3.path-style-access"), "false");
}

TEST(PaimonStorageOptionsTest, AwsDelegatedCredentialsUseSharedProvider) {
  auto config = MakeAwsConfig();
  config.role_arn = "arn:aws:iam::123456789012:role/paimon-reader";
  config.session_name = "paimon-session";
  config.external_id = "external-id";
  config.load_frequency = 1800;

  ASSERT_AND_ASSIGN(auto options, ToStorageOptions(config));
  EXPECT_EQ(options.at("cloud_provider"), kCloudProviderAWS);
  EXPECT_EQ(options.at("client.assume-role.arn"), config.role_arn);
  EXPECT_EQ(options.at("client.assume-role.session-name"), config.session_name);
  EXPECT_EQ(options.at("client.assume-role.external-id"), config.external_id);
  EXPECT_EQ(options.at("aws_credential_refresh_secs"), "1800");
  EXPECT_EQ(options.at("milvus_fs_cache_key"), config.GetCacheKey());
  EXPECT_EQ(options.count("s3.access-key"), 0);
  EXPECT_EQ(options.count("s3.secret-key"), 0);
}

TEST(PaimonStorageOptionsTest, GcpDelegatedCredentialsUseSharedProvider) {
  ArrowFileSystemConfig config;
  config.storage_type = "remote";
  config.cloud_provider = kCloudProviderGCP;
  config.use_iam = true;
  config.gcp_target_service_account = "target@project.iam.gserviceaccount.com";
  config.load_frequency = 1800;

  ASSERT_AND_ASSIGN(auto options, ToStorageOptions(config));
  EXPECT_EQ(options.at("gcs.service-account"), config.gcp_target_service_account);
  EXPECT_EQ(options.at("gcp_credential_refresh_secs"), "1800");
  EXPECT_EQ(options.at("milvus_fs_cache_key"), config.GetCacheKey());
  EXPECT_EQ(options.count("gcs.credential"), 0);
}

TEST(PaimonStorageOptionsTest, AwsIamUsesDefaultCredentialChain) {
  auto config = MakeAwsConfig();
  config.use_iam = true;
  ASSERT_AND_ASSIGN(auto options, ToStorageOptions(config));
  EXPECT_EQ(options.count("s3.access-key"), 0);
  EXPECT_EQ(options.count("s3.secret-key"), 0);
}

TEST(PaimonStorageOptionsTest, EndpointSchemeIsNormalized) {
  auto config = MakeAwsConfig();
  config.address = "localhost:9000";
  config.use_ssl = false;
  ASSERT_AND_ASSIGN(auto path_style_options, ToStorageOptions(config));
  EXPECT_EQ(path_style_options.at("s3.endpoint"), "http://localhost:9000");

  config.address = "https://s3.us-west-2.amazonaws.com";
  ASSERT_AND_ASSIGN(auto virtual_host_options, ToStorageOptions(config));
  EXPECT_EQ(virtual_host_options.at("s3.endpoint"), "https://s3.us-west-2.amazonaws.com");
}

TEST(PaimonStorageOptionsTest, LocalIsEmpty) {
  ArrowFileSystemConfig config;
  config.storage_type = "local";
  ASSERT_AND_ASSIGN(auto options, ToStorageOptions(config));
  EXPECT_TRUE(options.empty());
}

TEST(PaimonStorageOptionsTest, AliyunStaticCredentials) {
  ArrowFileSystemConfig config;
  config.storage_type = "remote";
  config.cloud_provider = kCloudProviderAliyun;
  config.address = "oss-cn-hangzhou.aliyuncs.com";
  config.use_ssl = true;
  config.access_key_id = "oss-access-key";
  config.access_key_value = "oss-secret-key";

  ASSERT_AND_ASSIGN(auto options, ToStorageOptions(config));
  EXPECT_EQ(options.at("fs.oss.endpoint"), "https://oss-cn-hangzhou.aliyuncs.com");
  EXPECT_EQ(options.at("fs.oss.accessKeyId"), config.access_key_id);
  EXPECT_EQ(options.at("fs.oss.accessKeySecret"), config.access_key_value);
}

TEST(PaimonStorageOptionsTest, AliyunDelegatedCredentialsUseSharedProvider) {
  ArrowFileSystemConfig config;
  config.storage_type = "remote";
  config.cloud_provider = kCloudProviderAliyun;
  config.address = "oss-cn-hangzhou.aliyuncs.com";
  config.use_ssl = true;
  config.role_arn = "acs:ram::123456789012:role/paimon-reader";
  config.session_name = "paimon-session";
  config.external_id = "external-id";
  config.load_frequency = 1800;

  ASSERT_AND_ASSIGN(auto options, ToStorageOptions(config));
  EXPECT_EQ(options.at("oss.endpoint"), "https://oss-cn-hangzhou.aliyuncs.com");
  EXPECT_EQ(options.at("oss.role-arn"), config.role_arn);
  EXPECT_EQ(options.at("oss.role-session-name"), config.session_name);
  EXPECT_EQ(options.at("oss.external-id"), config.external_id);
  EXPECT_EQ(options.at("oss_credential_refresh_secs"), "1800");
  EXPECT_EQ(options.at("milvus_fs_cache_key"), config.GetCacheKey());
  EXPECT_EQ(options.count("fs.oss.accessKeyId"), 0);
  EXPECT_EQ(options.count("fs.oss.accessKeySecret"), 0);
}

TEST(PaimonStorageOptionsTest, AzureAccountKey) {
  ArrowFileSystemConfig config;
  config.storage_type = "remote";
  config.cloud_provider = kCloudProviderAzure;
  config.address = "account.dfs.core.windows.net";
  config.use_ssl = true;
  config.access_key_id = "account";
  config.access_key_value = "account-key";

  ASSERT_AND_ASSIGN(auto options, ToStorageOptions(config));
  EXPECT_EQ(options.at("azure.endpoint"), "https://account.dfs.core.windows.net");
  EXPECT_EQ(options.at("azure.account-name"), "account");
  EXPECT_EQ(options.at("azure.account-key"), "account-key");
}

TEST(PaimonStorageOptionsTest, AzureBrokerUsesSharedProvider) {
  ArrowFileSystemConfig config;
  config.storage_type = "remote";
  config.cloud_provider = kCloudProviderAzure;
  config.address = "core.windows.net";
  config.use_ssl = true;
  config.access_key_id = "account";
  config.bucket_name = "container";
  config.region = "westus3";
  config.azure_client_id = "client-id";
  config.azure_tenant_id = "tenant-id";
  config.azure_credential_endpoint = "https://broker.example.com/credentials";
  config.load_frequency = 1800;
  config.request_timeout_ms = 5000;

  ASSERT_AND_ASSIGN(auto options, ToStorageOptions(config));
  EXPECT_EQ(options.at("azure.endpoint"), "https://account.dfs.core.windows.net");
  EXPECT_EQ(options.at("azure_broker_endpoint"), config.azure_credential_endpoint);
  EXPECT_EQ(options.at("azure_broker_client_id"), config.azure_client_id);
  EXPECT_EQ(options.at("azure_broker_tenant_id"), config.azure_tenant_id);
  EXPECT_EQ(options.at("azure_broker_account_name"), config.access_key_id);
  EXPECT_EQ(options.at("azure_broker_bucket"), config.bucket_name);
  EXPECT_EQ(options.at("azure_broker_duration_seconds"), "1800");
  EXPECT_EQ(options.at("azure_broker_request_timeout_ms"), "5000");
  EXPECT_EQ(options.at("milvus_fs_cache_key"), config.GetCacheKey());
  EXPECT_EQ(options.count("azure.account-key"), 0);
}

TEST(PaimonStorageOptionsTest, AzureWorkloadIdentityRequiresCompleteServicePrincipal) {
  ArrowFileSystemConfig config;
  config.storage_type = "remote";
  config.cloud_provider = kCloudProviderAzure;
  config.use_iam = true;

  ScopedEnvVar token("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/azure-identity-token");
  ScopedEnvVar client_id("AZURE_CLIENT_ID", "client-id");
  ScopedEnvVar tenant_id("AZURE_TENANT_ID", "tenant-id");
  ScopedEnvVar missing_secret("AZURE_CLIENT_SECRET", nullptr);

  EXPECT_TRUE(ToStorageOptions(config).status().IsNotImplemented());

  ScopedEnvVar client_secret("AZURE_CLIENT_SECRET", "client-secret");
  ASSERT_AND_ASSIGN(auto options, ToStorageOptions(config));
  EXPECT_EQ(options.at("azure.client-id"), "client-id");
  EXPECT_EQ(options.at("azure.client-secret"), "client-secret");
  EXPECT_EQ(options.at("azure.tenant-id"), "tenant-id");
}

TEST(PaimonStorageOptionsTest, GcpStaticAndAnonymous) {
  ArrowFileSystemConfig config;
  config.storage_type = "remote";
  config.cloud_provider = kCloudProviderGCP;
  config.address = "storage.googleapis.com";
  config.use_ssl = true;
  config.gcp_credential_json = R"({"type":"service_account"})";
  config.gcp_native_without_auth = true;

  ASSERT_AND_ASSIGN(auto options, ToStorageOptions(config));
  EXPECT_EQ(options.at("gcs.endpoint"), "https://storage.googleapis.com");
  EXPECT_EQ(options.at("gcs.credential"), "eyJ0eXBlIjoic2VydmljZV9hY2NvdW50In0=");
  EXPECT_EQ(options.at("gcs.allow-anonymous"), "true");
}

TEST(PaimonStorageOptionsTest, UnsupportedProviderFailsClosed) {
  auto config = MakeAwsConfig();
  config.cloud_provider = kCloudProviderTencent;
  EXPECT_TRUE(ToStorageOptions(config).status().IsNotImplemented());
}

TEST(PaimonStorageOptionsTest, MilvusUriConversion) {
  EXPECT_EQ(ToStandardUri("s3://localhost:9000/bucket/table"), "s3://bucket/table");
  EXPECT_EQ(ToMilvusUri("s3://bucket/table/data.parquet", "localhost:9000"),
            "s3://localhost:9000/bucket/table/data.parquet");
  EXPECT_EQ(ToMilvusUri("/tmp/table/data.parquet", "localhost:9000"), "/tmp/table/data.parquet");
  EXPECT_EQ(ToMilvusUri("s3://bucket/table/data.parquet", ""), "s3://bucket/table/data.parquet");
}

}  // namespace
}  // namespace milvus_storage::paimon::test
