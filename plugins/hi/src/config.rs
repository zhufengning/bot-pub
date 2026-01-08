use std::path::Path;

use kovi::log::warn;
use kovi::tokio::fs;

#[derive(Clone)]
pub struct Config {
    pub max_messages_per_group: usize,
    pub idle_seconds: u64,
    pub mention_wait_seconds: u64,
    pub dify: Option<DifyConfig>,
    pub vision: Option<VisionConfig>,
    pub dify_retry_times: u32,
    pub idle_prob_initial: f64,
    pub idle_prob_max: f64,
    pub idle_prob_full_hits: u32,
    pub idle_prob_growth_power: f64,
    pub max_split_segments: usize,
    pub delay_per_char: f64,
    pub enable_split_messages: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_messages_per_group: 200,
            idle_seconds: 150,
            mention_wait_seconds: 5,
            dify: None,
            vision: None,
            dify_retry_times: 2,
            idle_prob_initial: 0.3,
            idle_prob_max: 1.0,
            idle_prob_full_hits: 5,
            idle_prob_growth_power: 2.0,
            max_split_segments: 5,
            delay_per_char: 0.2,
            enable_split_messages: true,
        }
    }
}

pub async fn load_config(data_path: &Path) -> Config {
    #[derive(serde::Deserialize)]
    struct RawConfig {
        max_messages_per_group: Option<usize>,
        idle_seconds: Option<u64>,
        mention_wait_seconds: Option<u64>,
        dify: Option<DifyConfig>,
        vision: Option<VisionConfig>,
        dify_retry_times: Option<u32>,
        idle_prob_initial: Option<f64>,
        idle_prob_max: Option<f64>,
        idle_prob_full_hits: Option<u32>,
        idle_prob_growth_power: Option<f64>,
        max_split_segments: Option<usize>,
        delay_per_char: Option<f64>,
        enable_split_messages: Option<bool>,
    }

    let config_path = data_path.join("config.toml");

    let Ok(buf) = fs::read(&config_path).await else { return Config::default() };

    let s = match String::from_utf8(buf) {
        Ok(s) => s,
        Err(e) => {
            warn!("config.toml is not valid UTF-8: {e}");
            return Config::default();
        }
    };

    match toml::from_str::<RawConfig>(&s) {
        Ok(raw) => Config {
            max_messages_per_group: raw.max_messages_per_group.unwrap_or(200),
            idle_seconds: raw.idle_seconds.unwrap_or(150),
            mention_wait_seconds: raw.mention_wait_seconds.unwrap_or(5),
            dify: raw.dify,
            vision: raw.vision,
            dify_retry_times: raw.dify_retry_times.unwrap_or(2),
            idle_prob_initial: raw.idle_prob_initial.unwrap_or(0.3),
            idle_prob_max: raw.idle_prob_max.unwrap_or(1.0),
            idle_prob_full_hits: raw.idle_prob_full_hits.unwrap_or(5),
            idle_prob_growth_power: raw.idle_prob_growth_power.unwrap_or(2.0),
            max_split_segments: raw.max_split_segments.unwrap_or(5),
            delay_per_char: raw.delay_per_char.unwrap_or(0.2),
            enable_split_messages: raw.enable_split_messages.unwrap_or(true),
        },
        Err(e) => {
            warn!("failed to parse config.toml: {e}");
            Config::default()
        }
    }
}

#[derive(Clone, serde::Deserialize)]
pub struct DifyConfig {
    pub base_url: String,
    pub api_key: String,
    pub timeout_seconds: Option<u64>,
}

#[derive(Clone, serde::Deserialize)]
pub struct VisionConfig {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub system_prompt: String,
}
