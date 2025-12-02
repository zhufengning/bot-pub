use std::collections::{HashMap, HashSet, hash_map::DefaultHasher};
use std::hash::Hasher;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kovi::tokio::fs;
use kovi::{
    Message, PluginBuilder as plugin,
    log::{debug, error, info, warn},
    serde_json,
    tokio::sync::Mutex,
};

use base64::Engine as _;
use chrono::{Local, TimeZone};
use dify_client::{Client as DifyClient, Config as DifyConfig, request};

use crate::my_structs::{MyMsg, MyMsgContent, MyMsgList, SenderInfo};

pub mod config;
pub mod my_structs;

fn enforce_limit(list: &mut MyMsgList, max: usize) {
    if max == 0 {
        return;
    }

    let len = list.messages.len();
    if len <= max {
        return;
    }

    let remove = len - max;
    list.messages.drain(0..remove);
}

fn calc_idle_trigger_prob(hit: u32, cfg: &config::Config) -> f64 {
    let p0 = cfg.idle_prob_initial;
    let mut p_max = cfg.idle_prob_max;
    if p_max < p0 {
        p_max = p0;
    }

    let full_hits = cfg.idle_prob_full_hits.max(1) as f64;
    let power = if cfg.idle_prob_growth_power <= 0.0 {
        1.0
    } else {
        cfg.idle_prob_growth_power
    };

    let n = (hit as f64).min(full_hits);
    let x = (n / full_hits).clamp(0.0, 1.0);
    let y = x.powf(power);
    let p = p0 + (p_max - p0) * y;
    p.clamp(0.0, 1.0)
}

async fn load_history(data_path: &PathBuf) -> HashMap<i64, MyMsgList> {
    let mut group_map: HashMap<i64, MyMsgList> = HashMap::new();

    match fs::read_dir(&data_path).await {
        Ok(mut dir) => {
            while let Ok(Some(entry)) = dir.next_entry().await {
                let path = entry.path();

                if path.extension().and_then(|e| e.to_str()) != Some("json") {
                    continue;
                }

                match fs::read(&path).await {
                    Ok(buf) => {
                        // Try new format first
                        let parsed_new = serde_json::from_slice::<MyMsgList>(&buf);
                        if let Ok(mut list) = parsed_new {
                            // Merge into map by group_id in messages
                            let mut pushed_any = false;
                            for msg in list.messages.drain(..) {
                                let entry =
                                    group_map.entry(msg.group_id).or_insert_with(|| MyMsgList {
                                        messages: Vec::<MyMsg>::new(),
                                        conversation_id: None,
                                    });
                                // prefer keeping existing conversation_id if already set
                                if entry.conversation_id.is_none() {
                                    entry.conversation_id = list.conversation_id.clone();
                                }
                                entry.messages.push(msg);
                                pushed_any = true;
                            }
                            if !pushed_any {
                                // no messages in file, try set conversation_id by inferring group_id from file name
                                if let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                                    && let Ok(gid) = stem.parse::<i64>()
                                {
                                    let entry = group_map.entry(gid).or_insert_with(|| MyMsgList {
                                        messages: Vec::<MyMsg>::new(),
                                        conversation_id: None,
                                    });
                                    if entry.conversation_id.is_none() {
                                        entry.conversation_id = list.conversation_id;
                                    }
                                }
                            }
                        } else {
                            // Backward compatibility: old format was Vec<MyMsg>
                            match serde_json::from_slice::<Vec<MyMsg>>(&buf) {
                                Ok(vec_old) => {
                                    for msg in vec_old {
                                        group_map
                                            .entry(msg.group_id)
                                            .or_insert_with(|| MyMsgList {
                                                messages: Vec::<MyMsg>::new(),
                                                conversation_id: None,
                                            })
                                            .messages
                                            .push(msg);
                                    }
                                }
                                Err(e) => {
                                    warn!("failed to parse history file {:?}: {}", path, e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("failed to read history file {:?}: {}", path, e);
                    }
                }
            }
        }
        Err(e) => {
            warn!("failed to read history dir {:?}: {}", data_path, e);
        }
    }
    group_map
}

async fn download_to_hashed_file(target_dir: &PathBuf, url: &str) -> Option<PathBuf> {
    if let Err(e) = fs::create_dir_all(target_dir).await {
        warn!("failed to create target dir {:?}: {}", target_dir, e);
        return None;
    }

    let resp = match reqwest::get(url).await {
        Ok(r) => r,
        Err(e) => {
            warn!("failed to download file {}: {}", url, e);
            return None;
        }
    };

    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let ext = if content_type.starts_with("image/jpeg") || content_type.starts_with("image/jpg") {
        "jpg"
    } else if content_type.starts_with("image/png") {
        "png"
    } else if content_type.starts_with("image/gif") {
        "gif"
    } else if content_type.starts_with("image/webp") {
        "webp"
    } else {
        "dat"
    };

    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            warn!("failed to read file body {}: {}", url, e);
            return None;
        }
    };

    let mut hasher = DefaultHasher::new();
    hasher.write(&bytes);
    let hash = hasher.finish();
    let file_name = format!("{:x}.{}", hash, ext);
    let file_path = target_dir.join(&file_name);

    let need_write = match fs::metadata(&file_path).await {
        Ok(meta) => !meta.is_file(),
        Err(_) => true,
    };

    if need_write && let Err(e) = fs::write(&file_path, &bytes).await {
        warn!("failed to save file {:?}: {}", file_path, e);
        return None;
    }

    Some(PathBuf::from(file_name))
}

fn join_url(base: &str, path: &str) -> String {
    let mut b = base.trim_end_matches('/').to_string();
    b.push('/');
    b.push_str(path.trim_start_matches('/'));
    b
}

async fn summarize_image_via_openai(
    vision_cfg: &config::VisionConfig,
    image_path: &Path,
) -> Option<String> {
    let url = join_url(&vision_cfg.base_url, "/v1/chat/completions");
    debug!("vision: preparing request to {} for {:?}", url, image_path);

    let img_bytes = match fs::read(image_path).await {
        Ok(b) => b,
        Err(e) => {
            warn!("failed to read image for vision {:?}: {}", image_path, e);
            return None;
        }
    };

    let mime = match image_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
    {
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        _ => "application/octet-stream",
    };

    let b64 = base64::engine::general_purpose::STANDARD.encode(&img_bytes);
    let data_url = format!("data:{};base64,{}", mime, b64);

    let body = serde_json::json!({
        "model": vision_cfg.model,
        "messages": [
            {"role": "system", "content": vision_cfg.system_prompt},
            {"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": data_url}}
            ]}
        ]
    });

    let client = reqwest::Client::new();
    let resp = match client
        .post(&url)
        .bearer_auth(&vision_cfg.api_key)
        .json(&body)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!("vision api request failed: {}", e);
            return None;
        }
    };

    if !resp.status().is_success() {
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        warn!(
            "vision api non-success status: {} body: {}",
            status, body_text
        );
        return None;
    }

    let v: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            warn!("vision api parse failed: {}", e);
            return None;
        }
    };

    let summary = v["choices"][0]["message"]["content"]
        .as_str()
        .map(|s| s.to_string());
    if summary.is_none() {
        warn!("vision api returned unexpected payload: {}", v);
    }
    summary
}

async fn get_image_summary(
    img_abs_path: &Path,
    cache_txt_path: &Path,
    vision_cfg_opt: Option<&config::VisionConfig>,
) -> Option<String> {
    // try cache first
    debug!("vision: try cache at {:?}", cache_txt_path);
    if let Ok(buf) = fs::read(cache_txt_path).await
        && let Ok(s) = String::from_utf8(buf)
    {
        debug!("vision: cache hit for {:?}", img_abs_path);
        return Some(s);
    }

    let Some(vision_cfg) = vision_cfg_opt else {
        info!(
            "vision: no config provided, skip summarizing for {:?}",
            img_abs_path
        );
        return None;
    };

    let summary = summarize_image_via_openai(vision_cfg, img_abs_path).await;
    if let Some(ref s) = summary {
        if let Err(e) = fs::write(cache_txt_path, s.as_bytes()).await {
            warn!(
                "failed to write image summary cache {:?}: {}",
                cache_txt_path, e
            );
        } else {
            debug!("vision: wrote cache {:?}", cache_txt_path);
        }
    }
    summary
}

async fn package_messages(
    _data_path: &Path,
    img_path: &Path,
    msgs: &[MyMsg],
    vision_cfg_opt: Option<&config::VisionConfig>,
) -> String {
    let mut out = String::new();
    let mut text_cnt = 0usize;
    let mut img_cnt = 0usize;
    let mut current_msg_id: Option<i32> = None;

    for m in msgs.iter() {
        let user = display_name(&m.sender);

        let content_str = match &m.content {
            MyMsgContent::Text(t) => {
                text_cnt += 1;
                t.clone()
            }
            MyMsgContent::Img(rel) => {
                img_cnt += 1;
                let img_abs = img_path.join(rel);
                let fname = img_abs
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("image");
                let cache_txt = img_abs.with_file_name(format!("{}.txt", fname));
                // cache file named as same file name with extra .txt, e.g. a.jpg.txt
                let mut res = String::from("[image]");
                if let Some(summary) = get_image_summary(&img_abs, &cache_txt, vision_cfg_opt).await
                {
                    res = format!("[image: {}]", summary);
                }
                res
            }
        };

        let time_str = match Local.timestamp_opt(m.time, 0).single() {
            Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
            None => m.time.to_string(),
        };

        // 当遇到新的 msg_id 时，关闭上一个 <message>，并开启新的 <message>
        if current_msg_id != Some(m.msg_id) {
            if current_msg_id.is_some() {
                out.push_str("</message>\n");
            }
            current_msg_id = Some(m.msg_id);
            out.push_str(&format!(
                "<message id={}>\n",
                m.msg_id
            ));
        }

        out.push_str(&format!(
            "  <messag_seg user_name=\"{}\" user_id={} time=\"{}\">\n{}\n  </messag_seg>\n",
            user, m.sender.user_id, time_str, content_str
        ));
        debug!(
            "pack: user='{}' time={} kind={}",
            user,
            m.time,
            match &m.content {
                MyMsgContent::Text(_) => "text",
                MyMsgContent::Img(_) => "image",
            }
        );
    }

    if current_msg_id.is_some() {
        out.push_str("</message>\n");
    }
    debug!(
        "packaged {} messages (text={}, image={}), bytes={}",
        msgs.len(),
        text_cnt,
        img_cnt,
        out.len()
    );
    out
}

fn display_name(s: &SenderInfo) -> String {
    let take_non_empty = |opt: &Option<String>| {
        opt.as_ref()
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string())
    };
    take_non_empty(&s.card)
        .or_else(|| take_non_empty(&s.nickname))
        .unwrap_or_else(|| s.user_id.to_string())
}

/// 清空当前积累的消息，调用 Dify，并在成功时回复且持久化会话 ID；失败则回填消息。
/// reply_msg_id 用于在被 @ 触发时给回复消息加上引用。
async fn clear_send_and_reply(
    bot: &kovi::RuntimeBot,
    group_list_arc: Arc<Mutex<MyMsgList>>,
    group_id: i64,
    data_path: &Path,
    img_path: &Path,
    cfg: &config::Config,
    reply_msg_id: Option<i32>,
) {
    // 若未配置 Dify，直接跳过，不清空
    let Some(dify_cfg) = &cfg.dify else {
        warn!("dify config not set; skip idle send for group {}", group_id);
        return;
    };

    // 取走当前消息快照，避免请求期间的新消息被清空
    let msgs_snapshot: Vec<MyMsg>;
    let conversation_id_existing: Option<String>;
    {
        let mut list = group_list_arc.lock().await;
        if list.messages.is_empty() {
            info!("idle for group {} but no messages", group_id);
            return;
        }
        info!("idle for group {}: sending to dify", group_id);
        msgs_snapshot = mem::take(&mut list.messages);
        conversation_id_existing = list.conversation_id.clone();
    }

    info!(
        "dify: using base={} timeout={}s has_cid={}",
        dify_cfg.base_url,
        dify_cfg.timeout_seconds.unwrap_or(60),
        conversation_id_existing.is_some()
    );

    let client = DifyClient::new_with_config(DifyConfig {
        base_url: dify_cfg.base_url.clone(),
        api_key: dify_cfg.api_key.clone(),
        timeout: Duration::from_secs(dify_cfg.timeout_seconds.unwrap_or(60)),
    });

    let user_id = format!("GroupMessage:{}", group_id);
    let packaged = package_messages(data_path, img_path, &msgs_snapshot, cfg.vision.as_ref()).await;
    debug!("dify: packaged length={} chars", packaged.len());

    let attempts = (1 + cfg.dify_retry_times as usize).max(1);
    let mut last_err: Option<String> = None;

    for attempt in 1..=attempts {
        let mut req = request::ChatMessagesRequest {
            query: packaged.clone(),
            user: user_id.clone(),
            ..Default::default()
        };
        if let Some(cid) = &conversation_id_existing {
            req.conversation_id = cid.clone();
        }

        match client.api().chat_messages(req).await {
            Ok(resp) => {
                let answer = resp.answer;
                let new_cid = resp.base.conversation_id.clone();
                info!(
                    "dify: got answer (attempt {}/{}), new_cid_set={} len={}",
                    attempt,
                    attempts,
                    new_cid.is_some(),
                    answer.len()
                );

                // 成功：只更新会话 ID 并持久化，保留请求期间新收到的消息
                {
                    let mut list = group_list_arc.lock().await;
                    if list.conversation_id.is_none() {
                        list.conversation_id = new_cid;
                    }
                    let history_path = data_path.join(format!("{}.json", group_id));
                    if let Ok(buf) = serde_json::to_vec_pretty(&*list) {
                        if let Err(e) = fs::write(&history_path, buf).await {
                            warn!(
                                "failed to write history after dify success for group {}: {}",
                                group_id, e
                            );
                        } else {
                            debug!(
                                "persisted conversation_id after dify success for group {}",
                                group_id
                            );
                        }
                    }
                }

                let mut msg: Message = answer.into();
                if let Some(reply_id) = reply_msg_id {
                    msg = msg.add_reply(reply_id);
                }
                bot.send_group_msg(group_id, msg);
                return;
            }
            Err(e) => {
                warn!(
                    "dify chat_messages failed (attempt {}/{} ) for group {}: {}",
                    attempt, attempts, group_id, e
                );
                last_err = Some(e.to_string());
                kovi::tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    // 全部失败：回填消息，避免丢失
    error!(
        "dify: all attempts failed for group {}: {:?}",
        group_id, last_err
    );
    let mut list2 = group_list_arc.lock().await;
    list2.messages.extend(msgs_snapshot.into_iter());
    let history_path = data_path.join(format!("{}.json", group_id));
    if let Ok(buf) = serde_json::to_vec_pretty(&*list2) {
        if let Err(e) = fs::write(&history_path, buf).await {
            warn!(
                "failed to write history after dify failure for group {}: {}",
                group_id, e
            );
        } else {
            debug!(
                "re-queued messages after dify failure for group {}",
                group_id
            );
        }
    }
}

#[kovi::plugin]
async fn main() {
    let bot = plugin::get_runtime_bot();
    bot.set_plugin_access_control_mode("hi", kovi::bot::AccessControlMode::WhiteList)
        .unwrap();

    let self_id = match bot.get_login_info().await {
        Ok(info) => {
            info!("bot info: {:#?}", info);
            info.data.get("user_id").and_then(|v| v.as_i64())
        }
        Err(e) => {
            warn!("get_login_info failed: {}", e);
            None
        }
    };

    let data_path = bot.get_data_path();
    let img_path = data_path.join("imgs");

    let config = config::load_config(&data_path).await;

    // Clone a RuntimeBot handle for use in async tasks (avoid calling get_runtime_bot outside plugin context)
    let runtime_bot = bot.clone();

    let initial_history = load_history(&data_path).await;
    let mut initial_group_map: HashMap<i64, Arc<Mutex<MyMsgList>>> = HashMap::new();
    for (gid, list) in initial_history {
        initial_group_map.insert(gid, Arc::new(Mutex::new(list)));
    }

    let msg_list_arc = Arc::new(Mutex::new(initial_group_map));
    let last_seen_arc: Arc<Mutex<HashMap<i64, Instant>>> = Arc::new(Mutex::new(HashMap::new()));
    let watching_arc: Arc<Mutex<HashSet<i64>>> = Arc::new(Mutex::new(HashSet::new()));

    plugin::on_group_msg(move |event| {
        // info!("received group msg: {:#?}", event);
        let msg_list_arc = msg_list_arc.clone();
        let img_path = img_path.clone();
        let data_path = data_path.clone();
        let config = config.clone();
        let last_seen_arc = last_seen_arc.clone();
        let watching_arc = watching_arc.clone();
        let runtime_bot = runtime_bot.clone();
        let self_id = self_id;
        async move {
            let kovi::event::PostType::Message = &event.post_type else {
                return;
            };

            if event.get_text().contains("#bot_ignore") {
                info!("bot_ignore");
                return;
            }

            let group_id = event.group_id;
            let receive_time = event.time;
            let sender = SenderInfo::from(&event.sender);
            let msg_id = event.message_id;

            let msg = &event.message;

            let mut mentioned_me = false;
            let self_id_val = self_id.unwrap();

            {
                let mut last_seen = last_seen_arc.lock().await;
                last_seen.insert(group_id, Instant::now());
            }

            let group_list_arc = {
                let mut group_map = msg_list_arc.lock().await;
                group_map
                    .entry(group_id)
                    .or_insert_with(|| {
                        Arc::new(Mutex::new(MyMsgList {
                            messages: Vec::<MyMsg>::new(),
                            conversation_id: None,
                        }))
                    })
                    .clone()
            };

            {
                let mut watching = watching_arc.lock().await;
                if !watching.contains(&group_id) {
                    watching.insert(group_id);
                    let group_list_arc_cloned = group_list_arc.clone();
                    let last_seen_cloned = last_seen_arc.clone();
                    let watching_cloned = watching_arc.clone();
                    let config_cloned = config.clone();
                    let data_path_cloned = data_path.clone();
                    let img_path_cloned = img_path.clone();
                    let bot_for_send = runtime_bot.clone();

                    kovi::spawn(async move {
                        let idle = Duration::from_secs(config_cloned.idle_seconds);
                        let mut idle_hits: u32 = 0;

                        loop {
                            kovi::tokio::time::sleep(Duration::from_secs(1)).await;

                            let last_opt = {
                                let last_seen = last_seen_cloned.lock().await;
                                last_seen.get(&group_id).cloned()
                            };

                            let Some(last) = last_opt else {
                                break;
                            };

                            if last.elapsed() < idle {
                                idle_hits = 0;
                                continue;
                            }
                            

                            idle_hits = idle_hits.saturating_add(1);
                            let p = calc_idle_trigger_prob(idle_hits, &config_cloned);
                            let roll: f64 = rand::random();
                            info!(
                                "idle watcher: group {} idle_hits={} prob={} roll={}",
                                group_id, idle_hits, p, roll
                            );

                            if roll < p {
                                clear_send_and_reply(
                                    &bot_for_send,
                                    group_list_arc_cloned.clone(),
                                    group_id,
                                    &data_path_cloned,
                                    &img_path_cloned,
                                    &config_cloned,
                                    None,
                                )
                                .await;

                                break;
                            } else {
                                // 未命中本次触发概率，相当于重新开始一个新的 idle 窗口
                                {
                                    let mut last_seen = last_seen_cloned.lock().await;
                                    last_seen.insert(group_id, Instant::now());
                                }
                                //idle_hits = 0;
                            }
                        }

                        let mut watching = watching_cloned.lock().await;
                        watching.remove(&group_id);
                    });
                }
            }

            let mut msg_list = group_list_arc.lock().await;

            for v in msg.iter() {
                match v.type_.as_str() {
                    "text" => {
                        let content = MyMsgContent::Text(
                            v.data["text"].as_str().unwrap_or_default().to_string(),
                        );
                        msg_list.messages.push(MyMsg {
                            group_id,
                            time: receive_time,
                            sender: sender.clone(),
                            content,
                            msg_id,
                        });
                        enforce_limit(&mut msg_list, config.max_messages_per_group);
                    }
                    "image" => {
                        let img_url = v.data["url"].as_str().unwrap_or_default();
                        if img_url.is_empty() {
                            warn!("image segment without url");
                            continue;
                        }

                        if let Some(rel_path) = download_to_hashed_file(&img_path, img_url).await {
                            msg_list.messages.push(MyMsg {
                                group_id,
                                time: receive_time,
                                sender: sender.clone(),
                                content: MyMsgContent::Img(rel_path),
                                msg_id,
                            });
                            enforce_limit(&mut msg_list, config.max_messages_per_group);
                        }
                    }
                    "at" => {
                        let qq = v.data["qq"].as_str().unwrap_or_default();
                        if qq == self_id_val.to_string() {
                            mentioned_me = true;
                            debug!(
                                "detected @mention to self in group {} message_id {}",
                                group_id, msg_id
                            );
                        }
                        let hint_text = if mentioned_me {
                            "<at target=\"self\" />".to_string()
                        } else {
                            format!("<at target=\"{}\" />", qq)
                        };
                        msg_list.messages.push(MyMsg {
                            group_id,
                            time: receive_time,
                            sender: sender.clone(),
                            content: MyMsgContent::Text(hint_text),
                            msg_id,
                        });
                        enforce_limit(&mut msg_list, config.max_messages_per_group);
                    }
                    "reply" => {
                        let raw_id = &v.data["id"];
                        let reply_msg_id_opt = raw_id
                            .as_i64()
                            .map(|v| v as i32)
                            .or_else(|| raw_id.as_str().and_then(|s| s.parse::<i32>().ok()));

                        let Some(reply_msg_id) = reply_msg_id_opt else {
                            warn!("reply segment without valid id: {:?}", raw_id);
                            continue;
                        };

                        match runtime_bot.get_msg(reply_msg_id).await {
                            Ok(ret) => {
                                if let Some(msg_val) = ret.data.get("message") {
                                    match serde_json::from_value::<Message>(msg_val.clone()) {
                                        Ok(orig_msg) => {
                                            let orig_text = orig_msg.to_human_string();
                                            let quoted =
                                                format!("<reply_to msg_id={}>\n{}\n</reply_to>", reply_msg_id, orig_text);
                                            msg_list.messages.push(MyMsg {
                                                group_id,
                                                time: receive_time,
                                                sender: sender.clone(),
                                                content: MyMsgContent::Text(quoted),
                                                msg_id,
                                            });
                                            enforce_limit(
                                                &mut msg_list,
                                                config.max_messages_per_group,
                                            );
                                        }
                                        Err(e) => {
                                            warn!(
                                                "failed to parse get_msg message for reply {}: {}",
                                                reply_msg_id, e
                                            );
                                        }
                                    }
                                } else {
                                    warn!(
                                        "get_msg returned no message field for reply {}",
                                        reply_msg_id
                                    );
                                }
                            }
                            Err(e) => {
                                warn!("get_msg failed for reply {}: {:?}", reply_msg_id, e);
                            }
                        }
                    }
                    _ => {
                        msg_list.messages.push(MyMsg {
                            group_id,
                            time: receive_time,
                            sender: sender.clone(),
                            content: MyMsgContent::Text("".to_string()),
                            msg_id,
                        });
                        enforce_limit(&mut msg_list, config.max_messages_per_group);
                    }
                }
            }

            let history_path = data_path.join(format!("{}.json", group_id));

            match serde_json::to_vec_pretty(&*msg_list) {
                Ok(buf) => {
                    if let Err(e) = fs::write(&history_path, buf).await {
                        warn!("failed to write history for group {}: {}", group_id, e);
                    }
                }
                Err(e) => {
                    warn!("failed to serialize history for group {}: {}", group_id, e);
                }
            }

            // 如果本条消息包含对机器人的 @，立刻触发一次清空+调用 Dify+回复
            if mentioned_me {
                debug!("mentioned me, clear and reply");

                let runtime_bot_cloned = runtime_bot.clone();
                let group_list_arc_cloned = group_list_arc.clone();
                let data_path_cloned = data_path.clone();
                let img_path_cloned = img_path.clone();
                let config_cloned = config.clone();

                kovi::spawn(async move {
                    clear_send_and_reply(
                        &runtime_bot_cloned,
                        group_list_arc_cloned,
                        group_id,
                        &data_path_cloned,
                        &img_path_cloned,
                        &config_cloned,
                        Some(msg_id),
                    )
                    .await;
                });
            }
        }
    });
}
