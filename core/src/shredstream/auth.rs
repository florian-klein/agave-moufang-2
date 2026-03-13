//! Token authentication for Jito shredstream
//!
//! Implements challenge-response authentication with Jito's block engine
//! using a Solana keypair signature.

use {
    super::protos::auth::{
        auth_service_client::AuthServiceClient, GenerateAuthChallengeRequest,
        GenerateAuthTokensRequest, RefreshAccessTokenRequest, Role, Token,
    },
    arc_swap::{ArcSwap, ArcSwapAny},
    prost_types::Timestamp,
    solana_keypair::Keypair,
    solana_metrics::datapoint_info,
    solana_signer::Signer,
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::{Duration, Instant, SystemTime},
    },
    thiserror::Error,
    tokio::{task::JoinHandle, time::sleep},
    tonic::{
        metadata::errors::InvalidMetadataValue,
        service::Interceptor,
        transport::{Channel, Endpoint},
        Request, Status,
    },
};

/// Errors that can occur during block engine connection
#[derive(Debug, Error)]
pub enum BlockEngineConnectionError {
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("client error: {0}")]
    Client(#[from] Status),

    #[error("deserializing error")]
    Deserialization,
}

pub type BlockEngineConnectionResult<T> = Result<T, BlockEngineConnectionError>;

/// Client interceptor that adds Bearer token to each gRPC request.
/// Manages token refresh in a background thread.
#[derive(Clone)]
pub struct ClientInterceptor {
    /// The access token added to each request header
    bearer_token: Arc<ArcSwap<String>>,
}

impl ClientInterceptor {
    /// Create a new client interceptor with initial authentication.
    /// Spawns a background thread to refresh tokens before expiration.
    pub async fn new(
        mut auth_service_client: AuthServiceClient<Channel>,
        keypair: Arc<Keypair>,
        role: Role,
        service_name: String,
        exit: Arc<AtomicBool>,
    ) -> BlockEngineConnectionResult<(Self, JoinHandle<()>)> {
        let (
            Token {
                value: access_token,
                expires_at_utc: access_token_expiration,
            },
            refresh_token,
        ) = Self::auth(&mut auth_service_client, &keypair, role).await?;
        let bearer_token = Arc::new(ArcSwap::from_pointee(access_token));

        let refresh_thread_handle = Self::spawn_token_refresh_thread(
            auth_service_client,
            bearer_token.clone(),
            refresh_token,
            access_token_expiration.ok_or(BlockEngineConnectionError::Deserialization)?,
            keypair,
            role,
            service_name,
            exit,
        );

        Ok((Self { bearer_token }, refresh_thread_handle))
    }

    /// Perform challenge-response authentication.
    /// Returns (access_token, refresh_token).
    async fn auth(
        auth_service_client: &mut AuthServiceClient<Channel>,
        keypair: &Keypair,
        role: Role,
    ) -> BlockEngineConnectionResult<(Token, Token)> {
        let pubkey_vec = keypair.pubkey().as_ref().to_vec();

        // Step 1: Get challenge from server
        let challenge_resp = auth_service_client
            .generate_auth_challenge(GenerateAuthChallengeRequest {
                role: role as i32,
                pubkey: pubkey_vec.clone(),
            })
            .await?
            .into_inner();

        // Step 2: Sign challenge: "pubkey-challenge"
        let challenge = format!("{}-{}", keypair.pubkey(), challenge_resp.challenge);
        let signed_challenge = keypair.sign_message(challenge.as_bytes()).as_ref().to_vec();

        // Step 3: Exchange signed challenge for tokens
        let tokens = auth_service_client
            .generate_auth_tokens(GenerateAuthTokensRequest {
                challenge,
                client_pubkey: pubkey_vec,
                signed_challenge,
            })
            .await?
            .into_inner();

        Ok((
            tokens
                .access_token
                .ok_or(BlockEngineConnectionError::Deserialization)?,
            tokens
                .refresh_token
                .ok_or(BlockEngineConnectionError::Deserialization)?,
        ))
    }

    /// Spawn a background thread that refreshes tokens before expiration
    #[allow(clippy::too_many_arguments)]
    fn spawn_token_refresh_thread(
        mut auth_service_client: AuthServiceClient<Channel>,
        bearer_token: Arc<ArcSwap<String>>,
        initial_refresh_token: Token,
        initial_access_token_expiration: Timestamp,
        keypair: Arc<Keypair>,
        role: Role,
        service_name: String,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut refresh_token = initial_refresh_token;
            let mut access_token_expiration = initial_access_token_expiration;

            while !exit.load(Ordering::Relaxed) {
                let now = SystemTime::now();

                // Check refresh token TTL - re-auth if < 5 minutes
                let refresh_token_ttl =
                    SystemTime::try_from(refresh_token.expires_at_utc.as_ref().unwrap().clone())
                        .unwrap()
                        .duration_since(now)
                        .unwrap_or_default();

                if refresh_token_ttl < Duration::from_secs(5 * 60) {
                    let start = Instant::now();
                    let is_error = {
                        if let Ok((new_access_token, new_refresh_token)) =
                            Self::auth(&mut auth_service_client, &keypair, role).await
                        {
                            bearer_token.store(Arc::new(new_access_token.value));
                            access_token_expiration = new_access_token.expires_at_utc.unwrap();
                            refresh_token = new_refresh_token;
                            false
                        } else {
                            true
                        }
                    };
                    datapoint_info!(
                        "shredstream-token_auth",
                        ("auth_type", "full_auth", String),
                        ("service", service_name.clone(), String),
                        ("is_error", is_error, bool),
                        ("latency_us", start.elapsed().as_micros(), i64),
                    );
                    continue;
                }

                // Check access token TTL - refresh if < 5 minutes
                let access_token_ttl = SystemTime::try_from(access_token_expiration.clone())
                    .unwrap()
                    .duration_since(now)
                    .unwrap_or_default();

                if access_token_ttl < Duration::from_secs(5 * 60) {
                    let start = Instant::now();
                    let is_error = {
                        if let Ok(refresh_resp) = auth_service_client
                            .refresh_access_token(RefreshAccessTokenRequest {
                                refresh_token: refresh_token.value.clone(),
                            })
                            .await
                        {
                            let access_token = refresh_resp.into_inner().access_token.unwrap();
                            bearer_token.store(Arc::new(access_token.value.clone()));
                            access_token_expiration = access_token.expires_at_utc.unwrap();
                            false
                        } else {
                            true
                        }
                    };

                    datapoint_info!(
                        "shredstream-token_auth",
                        ("auth_type", "access_token", String),
                        ("service", service_name.clone(), String),
                        ("is_error", is_error, bool),
                        ("latency_us", start.elapsed().as_micros(), i64),
                    );
                    continue;
                }

                sleep(Duration::from_secs(5)).await;
            }
        })
    }
}

impl Interceptor for ClientInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let l_token = ArcSwapAny::load(&self.bearer_token);
        if l_token.is_empty() {
            return Err(Status::invalid_argument("missing bearer token"));
        }
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {l_token}")
                .parse()
                .map_err(|e: InvalidMetadataValue| Status::invalid_argument(e.to_string()))?,
        );

        Ok(request)
    }
}

/// Create a gRPC channel with optional TLS based on URL scheme.
/// Includes connect and request timeouts to prevent indefinite blocking
/// on unreachable hosts (TCP SYN retries can take 2+ minutes otherwise).
pub async fn create_grpc_channel(url: String) -> BlockEngineConnectionResult<Channel> {
    let endpoint = if url.starts_with("https") {
        Endpoint::from_shared(url)
            .map_err(BlockEngineConnectionError::Transport)?
            .tls_config(tonic::transport::ClientTlsConfig::new())?
    } else {
        Endpoint::from_shared(url).map_err(BlockEngineConnectionError::Transport)?
    };
    Ok(endpoint
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(10))
        .connect()
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that we can establish a TLS connection to the Jito block engine.
    /// This test requires network access and is ignored by default.
    /// Run with: cargo test -p solana-core test_block_engine_tls_connection -- --ignored
    #[tokio::test]
    #[ignore]
    async fn test_block_engine_tls_connection() {
        let urls = [
            "https://amsterdam.mainnet.block-engine.jito.wtf",
            "https://frankfurt.mainnet.block-engine.jito.wtf",
            "https://ny.mainnet.block-engine.jito.wtf",
            "https://mainnet.block-engine.jito.wtf",
        ];

        for url in urls {
            println!("Testing connection to {}", url);
            let result = create_grpc_channel(url.to_string()).await;
            match &result {
                Ok(_) => println!("  SUCCESS: Connected to {}", url),
                Err(e) => println!("  FAILED: {} - {:?}", url, e),
            }
            assert!(
                result.is_ok(),
                "Failed to connect to {}: {:?}",
                url,
                result.err()
            );
        }
    }

    /// Test that we can authenticate with the Jito block engine using a keypair.
    /// This test requires network access and the shredstream auth keypair.
    /// Run with: cargo test -p solana-core test_block_engine_auth -- --ignored
    #[tokio::test]
    #[ignore]
    async fn test_block_engine_auth() {
        use solana_keypair::read_keypair_file;

        let keypair_path = "/home/solana/moufang-arb/shredstream-proxy/my_keypair.json";
        let keypair = match read_keypair_file(keypair_path) {
            Ok(kp) => Arc::new(kp),
            Err(e) => {
                println!("Skipping test: could not read keypair from {}: {}", keypair_path, e);
                return;
            }
        };

        let url = "https://amsterdam.mainnet.block-engine.jito.wtf";
        println!("Testing auth to {} with pubkey {}", url, keypair.pubkey());

        // Create channel
        let channel = create_grpc_channel(url.to_string())
            .await
            .expect("Failed to create channel");

        // Create auth client and authenticate
        let mut auth_client = AuthServiceClient::new(channel);

        // Step 1: Get challenge
        let challenge_resp = auth_client
            .generate_auth_challenge(GenerateAuthChallengeRequest {
                role: Role::ShredstreamSubscriber as i32,
                pubkey: keypair.pubkey().as_ref().to_vec(),
            })
            .await
            .expect("Failed to get auth challenge");

        println!("  Got challenge: {}", challenge_resp.get_ref().challenge);

        // Step 2: Sign challenge
        let challenge = format!("{}-{}", keypair.pubkey(), challenge_resp.get_ref().challenge);
        let signed_challenge = keypair.sign_message(challenge.as_bytes()).as_ref().to_vec();

        // Step 3: Get tokens
        let tokens = auth_client
            .generate_auth_tokens(GenerateAuthTokensRequest {
                challenge,
                client_pubkey: keypair.pubkey().as_ref().to_vec(),
                signed_challenge,
            })
            .await
            .expect("Failed to get auth tokens");

        let access_token = tokens.get_ref().access_token.as_ref().expect("No access token");
        println!("  SUCCESS: Got access token (expires: {:?})", access_token.expires_at_utc);
    }
}
