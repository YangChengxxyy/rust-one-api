use rust_one_api::{config::Config, provider_quota, server, storage};

#[tokio::main]
async fn main() {
    let cfg = match Config::load() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("config error: {e}");
            std::process::exit(1);
        }
    };
    let filter = tracing_subscriber::EnvFilter::try_new(&cfg.log)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let pool = match storage::connect(&cfg.database_url).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("database connect error: {e}");
            std::process::exit(1);
        }
    };
    if let Err(e) = storage::migrate(&pool).await {
        eprintln!("migration error: {e}");
        std::process::exit(1);
    }

    if let Err(e) = rust_one_api::pricing_seed::seed_model_prices(&pool).await {
        tracing::warn!("model price seeding failed: {e}");
    }

    let http = reqwest::Client::new();
    provider_quota::spawn_scheduler(pool.clone(), http);

    if let Err(e) = server::run(cfg, pool).await {
        eprintln!("server error: {e}");
        std::process::exit(1);
    }
}
