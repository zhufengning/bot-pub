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
    tokio::sync::{Mutex, Notify},
};

use base64::Engine as _;
use chrono::{Local, TimeZone};
use dify_client::{Client as DifyClient, Config as DifyConfig, request};

use crate::my_structs::{MyMsg, MyMsgContent, MyMsgList, SenderInfo};
use regex::Regex;

pub mod config;
pub mod my_structs;

struct MentionWaiterHandle {
    token: u64,
    notifier: Arc<Notify>,
}

#[derive(Clone)]
struct AppContext {
    msg_list: Arc<Mutex<HashMap<i64, Arc<Mutex<MyMsgList>>>>>,
    last_seen: Arc<Mutex<HashMap<i64, Instant>>>,
    watching: Arc<Mutex<HashSet<i64>>>,
    mention_waiting: Arc<Mutex<HashMap<i64, MentionWaiterHandle>>>,
    // Per-group reply lock to prevent interleaved segmented replies when multiple triggers occur
    reply_locks: Arc<Mutex<HashMap<i64, Arc<Mutex<()>>>>>,
    config: config::Config,
    data_path: PathBuf,
    img_path: PathBuf,
    runtime_bot: Arc<kovi::RuntimeBot>,
    self_id: Option<i64>,
}

impl AppContext {
    // Spawn a waiter for @mention: wait for quiet period of mention_wait_seconds, resetting when new
    // messages arrive; when expired, send reply once and exit.
    async fn spawn_mention_waiter(
        &self,
        group_id: i64,
        group_list_arc: Arc<Mutex<MyMsgList>>,
        reply_msg_id: i32,
    ) {
        let notifier = Arc::new(Notify::new());
        let (token, prev_notifier) = {
            let mut waiting = self.mention_waiting.lock().await;
            let next_token = waiting
                .get(&group_id)
                .map(|handle| handle.token.wrapping_add(1))
                .unwrap_or(1);
            let prev = waiting.insert(
                group_id,
                MentionWaiterHandle {
                    token: next_token,
                    notifier: notifier.clone(),
                },
            );
            (next_token, prev.map(|handle| handle.notifier))
        };

        if let Some(prev) = prev_notifier {
            prev.notify_waiters();
        }

        let ctx = self.clone();
        kovi::spawn(async move {
            // stop idle watcher for this group while we're waiting to avoid double triggers
            {
                let mut watching = ctx.watching.lock().await;
                watching.remove(&group_id);
            }

            let wait = Duration::from_secs(ctx.config.mention_wait_seconds.max(1));
            loop {
                let notified = notifier.notified();
                let forced = kovi::tokio::time::timeout(Duration::from_millis(500), notified)
                    .await
                    .is_ok();
                if forced {
                    break;
                }

                let last_opt = {
                    let last_seen = ctx.last_seen.lock().await;
                    last_seen.get(&group_id).cloned()
                };
                let Some(last) = last_opt else {
                    break;
                };
                if last.elapsed() >= wait {
                    break;
                }
            }

            // quiet enough or forced: send reply once
            clear_send_and_reply(
                &ctx.runtime_bot,
                ctx.reply_locks.clone(),
                group_list_arc.clone(),
                group_id,
                &ctx.data_path,
                &ctx.img_path,
                &ctx.config,
                Some(reply_msg_id),
            )
            .await;

            let mut waiting = ctx.mention_waiting.lock().await;
            if let Some(handle) = waiting.get(&group_id) {
                if handle.token == token {
                    waiting.remove(&group_id);
                }
            }
        });
    }
    // Append segments from a Kovi Message into our MyMsgList, expanding reply segments
    // iteratively (no async recursion) so we can preserve nested images/text.
    // - Keeps msg_id of the outer message so nested replies render inside the same <message>
    // - Emits <reply ...> and </reply> markers as Text entries for the packager
    // - Downloads images to hashed files and preserves them as Img entries
    async fn append_segments_recursive(
        &self,
        msg_list: &mut MyMsgList,
        group_id: i64,
        receive_time: i64,
        sender: &SenderInfo,
        current_msg_id: i32,
        msg: &Message,
    ) -> bool {
        enum Step {
            OpenReply {
                reply_msg_id: i32,
                reply_sender: SenderInfo,
                reply_time: Option<i64>,
            },
            CloseReply,
            Process {
                msg: Message,
                sender: SenderInfo,
                time: i64,
            },
        }

        let mut has_unknown_seg = false;
        let mut stack: Vec<Step> = Vec::new();
        stack.push(Step::Process {
            msg: msg.clone(),
            sender: sender.clone(),
            time: receive_time,
        });

        while let Some(step) = stack.pop() {
            match step {
                Step::OpenReply {
                    reply_msg_id,
                    reply_sender,
                    reply_time,
                } => {
                    let sender_name = display_name(&reply_sender);
                    let open = if let Some(t) = reply_time
                        && let Some(dt) = Local.timestamp_opt(t, 0).single()
                    {
                        format!(
                            "<reply to_msg_id={} sender_id={} sender_name=\"{}\" time=\"{}\">",
                            reply_msg_id,
                            reply_sender.user_id,
                            sender_name,
                            dt.format("%Y-%m-%d %H:%M:%S")
                        )
                    } else {
                        format!(
                            "<reply to_msg_id={} sender_id={} sender_name=\"{}\">",
                            reply_msg_id, reply_sender.user_id, sender_name
                        )
                    };
                    msg_list.messages.push(MyMsg {
                        group_id,
                        time: reply_time.unwrap_or(receive_time),
                        sender: reply_sender.clone(),
                        content: MyMsgContent::Text(open),
                        msg_id: current_msg_id,
                    });
                    enforce_limit(msg_list, self.config.max_messages_per_group);
                }
                Step::CloseReply => {
                    msg_list.messages.push(MyMsg {
                        group_id,
                        time: receive_time,
                        sender: sender.clone(),
                        content: MyMsgContent::Text("</reply>".to_string()),
                        msg_id: current_msg_id,
                    });
                    enforce_limit(msg_list, self.config.max_messages_per_group);
                }
                Step::Process { msg, sender, time } => {
                    for v in msg.iter() {
                        match v.type_.as_str() {
                            "text" => {
                                let text_content =
                                    v.data["text"].as_str().unwrap_or_default().to_string();
                                msg_list.messages.push(MyMsg {
                                    group_id,
                                    time,
                                    sender: sender.clone(),
                                    content: MyMsgContent::Text(text_content),
                                    msg_id: current_msg_id,
                                });
                                enforce_limit(msg_list, self.config.max_messages_per_group);
                            }
                            "image" => {
                                let img_url = v.data["url"].as_str().unwrap_or_default();
                                if img_url.is_empty() {
                                    warn!("image segment without url");
                                    continue;
                                }
                                if let Some(rel_path) =
                                    download_to_hashed_file(&self.img_path, img_url).await
                                {
                                    msg_list.messages.push(MyMsg {
                                        group_id,
                                        time,
                                        sender: sender.clone(),
                                        content: MyMsgContent::Img(rel_path),
                                        msg_id: current_msg_id,
                                    });
                                    enforce_limit(msg_list, self.config.max_messages_per_group);
                                }
                            }
                            "at" => {
                                let qq = v.data["qq"].as_str().unwrap_or_default();
                                let hint_text = if self.self_id.is_some()
                                    && qq == self.self_id.unwrap().to_string()
                                {
                                    "<at target=\"self\" />".to_string()
                                } else {
                                    format!("<at target=\"{}\" />", qq)
                                };
                                msg_list.messages.push(MyMsg {
                                    group_id,
                                    time,
                                    sender: sender.clone(),
                                    content: MyMsgContent::Text(hint_text),
                                    msg_id: current_msg_id,
                                });
                                enforce_limit(msg_list, self.config.max_messages_per_group);
                            }
                            "reply" => {
                                let raw_id = &v.data["id"];
                                let reply_msg_id_opt =
                                    raw_id.as_i64().map(|v| v as i32).or_else(|| {
                                        raw_id.as_str().and_then(|s| s.parse::<i32>().ok())
                                    });

                                if let Some(reply_msg_id) = reply_msg_id_opt {
                                    if let Some((reply_sender, orig_msg, orig_time)) =
                                        fetch_reply_message(&self.runtime_bot, reply_msg_id).await
                                    {
                                        // push reverse order so they pop in desired sequence: Open -> Process -> Close
                                        stack.push(Step::CloseReply);
                                        stack.push(Step::Process {
                                            msg: orig_msg,
                                            sender: reply_sender.clone(),
                                            time: orig_time.unwrap_or(time),
                                        });
                                        stack.push(Step::OpenReply {
                                            reply_msg_id,
                                            reply_sender,
                                            reply_time: orig_time,
                                        });
                                    } else {
                                        warn!("reply segment fetch failed: id={}", reply_msg_id);
                                        has_unknown_seg = true;
                                        msg_list.messages.push(MyMsg {
                                            group_id,
                                            time,
                                            sender: sender.clone(),
                                            content: MyMsgContent::Text(
                                                "<unknown_segment kind=\"reply\"/>".to_string(),
                                            ),
                                            msg_id: current_msg_id,
                                        });
                                        enforce_limit(msg_list, self.config.max_messages_per_group);
                                    }
                                } else {
                                    warn!("reply segment without valid id: {:?}", raw_id);
                                    has_unknown_seg = true;
                                    msg_list.messages.push(MyMsg {
                                        group_id,
                                        time,
                                        sender: sender.clone(),
                                        content: MyMsgContent::Text(
                                            "<unknown_segment kind=\"reply\"/>".to_string(),
                                        ),
                                        msg_id: current_msg_id,
                                    });
                                    enforce_limit(msg_list, self.config.max_messages_per_group);
                                }
                            }
                            _ => {
                                has_unknown_seg = true;
                                msg_list.messages.push(MyMsg {
                                    group_id,
                                    time,
                                    sender: sender.clone(),
                                    content: MyMsgContent::Text(
                                        "<unknown_segment></unknown_segment>".to_string(),
                                    ),
                                    msg_id: current_msg_id,
                                });
                                enforce_limit(msg_list, self.config.max_messages_per_group);
                            }
                        }
                    }
                }
            }
        }
        has_unknown_seg
    }
    async fn spawn_idle_watcher(&self, group_id: i64, group_list_arc: Arc<Mutex<MyMsgList>>) {
        {
            let mut watching = self.watching.lock().await;
            if watching.contains(&group_id) {
                return;
            }
            watching.insert(group_id);
        }

        let ctx = self.clone();
        kovi::spawn(async move {
            let idle = Duration::from_secs(ctx.config.idle_seconds);
            let mut idle_hits: u32 = 0;

            loop {
                kovi::tokio::time::sleep(Duration::from_secs(1)).await;

                let last_opt = {
                    let watching = ctx.watching.lock().await;
                    if !watching.contains(&group_id) {
                        break;
                    }
                    let last_seen = ctx.last_seen.lock().await;
                    last_seen.get(&group_id).cloned()
                };

                let Some(last) = last_opt else {
                    break;
                };

                if last.elapsed() < idle {
                    continue;
                }

                idle_hits = idle_hits.saturating_add(1);
                let p = calc_idle_trigger_prob(idle_hits, &ctx.config);
                let roll: f64 = rand::random();
                info!(
                    "idle watcher: group {} idle_hits={} prob={} roll={}",
                    group_id, idle_hits, p, roll
                );

                if roll < p {
                    clear_send_and_reply(
                        &ctx.runtime_bot,
                        ctx.reply_locks.clone(),
                        group_list_arc.clone(),
                        group_id,
                        &ctx.data_path,
                        &ctx.img_path,
                        &ctx.config,
                        None,
                    )
                    .await;

                    break;
                } else {
                    {
                        let mut last_seen = ctx.last_seen.lock().await;
                        last_seen.insert(group_id, Instant::now());
                    }
                }
            }

            let mut watching = ctx.watching.lock().await;
            watching.remove(&group_id);
        });
    }

    async fn handle_group_msg(
        &self,
        group_id: i64,
        receive_time: i64,
        sender: SenderInfo,
        msg_id: i32,
        msg: Message,
    ) {
        let mut mentioned_me = false;
        if self.self_id.is_none() {
            warn!("self_id not set, ignoring message");
            return;
        }
        let self_id_val = self.self_id.unwrap();

        {
            let mut last_seen = self.last_seen.lock().await;
            last_seen.insert(group_id, Instant::now());
        }

        let group_list_arc = {
            let mut group_map = self.msg_list.lock().await;
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

        self.spawn_idle_watcher(group_id, group_list_arc.clone())
            .await;

        // 1) pre-scan for top-level @self only to trigger immediate reply
        for v in msg.iter() {
            if v.type_ == "at" {
                let qq = v.data["qq"].as_str().unwrap_or_default();
                if qq == self_id_val.to_string() {
                    mentioned_me = true;
                    debug!(
                        "detected @mention to self in group {} message_id {}",
                        group_id, msg_id
                    );
                }
            }
        }

        // 2) append segments (with recursive reply expansion) into list
        let mut msg_list = group_list_arc.lock().await;
        let has_unknown_seg = self
            .append_segments_recursive(&mut msg_list, group_id, receive_time, &sender, msg_id, &msg)
            .await;
        if has_unknown_seg {
            msg_list.messages.push(MyMsg {
                group_id,
                time: receive_time,
                sender: sender.clone(),
                content: MyMsgContent::Text(format!(
                    "<msg_overview reason=\"has_unknown_seg\">{}</msg_overview>",
                    msg.to_human_string()
                )),
                msg_id,
            });
        }

        let history_path = self.data_path.join(format!("{}.json", group_id));
        if let Ok(buf) = serde_json::to_vec_pretty(&*msg_list) {
            if let Err(e) = fs::write(&history_path, buf).await {
                warn!("failed to write history for group {}: {}", group_id, e);
            }
        } else {
            warn!("failed to serialize history for group {}", group_id);
        }

        if mentioned_me {
            debug!("mentioned me, delay reply after quiet window");
            self.spawn_mention_waiter(group_id, group_list_arc.clone(), msg_id)
                .await;
        }
    }
}

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

    let mut dir = match fs::read_dir(&data_path).await {
        Ok(dir) => dir,
        Err(e) => {
            warn!("failed to read history dir {:?}: {}", data_path, e);
            return group_map;
        }
    };

    while let Ok(Some(entry)) = dir.next_entry().await {
        let path = entry.path();

        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }

        let buf = match fs::read(&path).await {
            Ok(buf) => buf,
            Err(e) => {
                warn!("failed to read history file {:?}: {}", path, e);
                continue;
            }
        };

        // Try new format first
        let parsed_new = serde_json::from_slice::<MyMsgList>(&buf);
        if let Ok(mut list) = parsed_new {
            // Merge into map by group_id in messages
            let mut pushed_any = false;
            for msg in list.messages.drain(..) {
                let entry = group_map.entry(msg.group_id).or_insert_with(|| MyMsgList {
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
        let s = s.trim();
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

fn push_indented(out: &mut String, indent: usize, s: &str) {
    for _ in 0..indent {
        out.push_str("  ");
    }
    out.push_str(s);
    out.push('\n');
}

async fn package_messages(
    _data_path: &Path,
    img_path: &Path,
    msgs: &[MyMsg],
    vision_cfg_opt: Option<&config::VisionConfig>,
) -> String {
    fn time_to_str(sec: i64) -> String {
        match Local.timestamp_opt(sec, 0).single() {
            Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
            None => sec.to_string(),
        }
    }

    let mut out = String::new();
    let mut text_cnt = 0usize;
    let mut img_cnt = 0usize;
    let mut current_msg_id: Option<i32> = None;
    let mut indent_level: usize = 1; // inside <message>

    for m in msgs.iter() {
        let user = display_name(&m.sender);
        let time_str = time_to_str(m.time);

        // switch message boundary
        if current_msg_id != Some(m.msg_id) {
            if current_msg_id.is_some() {
                out.push_str("</message>\n");
            }
            current_msg_id = Some(m.msg_id);
            out.push_str(&format!("<message id={}>\n", m.msg_id));
            indent_level = 1; // reset for each message
        }

        match &m.content {
            MyMsgContent::Text(t) => {
                let t_trim = t.trim();
                // reply block open/close markers are passed-through to create nesting
                if t_trim.starts_with("<reply ")
                    && t_trim.ends_with('>')
                    && !t_trim.contains("</reply>")
                {
                    push_indented(&mut out, indent_level, t_trim);
                    indent_level += 1;
                } else if t_trim == "</reply>" {
                    if indent_level > 1 {
                        indent_level -= 1;
                    }
                    push_indented(&mut out, indent_level, t_trim);
                } else {
                    text_cnt += 1;
                    push_indented(
                        &mut out,
                        indent_level,
                        &format!(
                            "<segment type=\"text\" user_name=\"{}\" user_id={} time=\"{}\">",
                            user, m.sender.user_id, time_str
                        ),
                    );
                    indent_level += 1;
                    // keep the text as-is (it may contain our lightweight tags like <at .../>)
                    push_indented(&mut out, indent_level, t);
                    indent_level -= 1;
                    push_indented(&mut out, indent_level, "</segment>");
                }
            }
            MyMsgContent::Img(rel) => {
                img_cnt += 1;
                let img_abs = img_path.join(rel);
                let fname = img_abs
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("image");
                let cache_txt = img_abs.with_file_name(format!("{}.txt", fname));
                let summary_opt = get_image_summary(&img_abs, &cache_txt, vision_cfg_opt).await;

                push_indented(
                    &mut out,
                    indent_level,
                    &format!(
                        "<segment type=\"image\" user_name=\"{}\" user_id={} time=\"{}\">",
                        user, m.sender.user_id, time_str
                    ),
                );
                indent_level += 1;
                push_indented(
                    &mut out,
                    indent_level,
                    &format!("<image file=\"{}\">", rel.display()),
                );
                if let Some(summary) = summary_opt {
                    indent_level += 1;
                    push_indented(&mut out, indent_level, "<summary>");
                    indent_level += 1;
                    push_indented(&mut out, indent_level, summary.trim());
                    indent_level -= 1;
                    push_indented(&mut out, indent_level, "</summary>");
                    indent_level -= 1;
                }
                push_indented(&mut out, indent_level, "</image>");
                indent_level -= 1;
                push_indented(&mut out, indent_level, "</segment>");
            }
        }

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

// Fetch reply message content with typed Message for recursive processing
async fn fetch_reply_message(
    bot: &kovi::RuntimeBot,
    reply_id: i32,
) -> Option<(SenderInfo, Message, Option<i64>)> {
    let ret = match bot.get_msg(reply_id).await {
        Ok(v) => v,
        Err(e) => {
            warn!("get_msg failed for reply {}: {:?}", reply_id, e);
            return None;
        }
    };

    let sender_val = if let Some(v) = ret.data.get("sender") {
        v
    } else {
        warn!("get_msg returned no sender field for reply {}", reply_id);
        return None;
    };

    let user_id = sender_val
        .get("user_id")
        .and_then(|v| v.as_i64())
        .unwrap_or_default();
    let nickname = sender_val
        .get("nickname")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let card = sender_val
        .get("card")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let sender_info = SenderInfo {
        user_id,
        nickname,
        card,
        sex: None,
        age: None,
        area: None,
        level: None,
        role: None,
        title: None,
    };

    let msg_val = if let Some(v) = ret.data.get("message") {
        v
    } else {
        warn!("get_msg returned no message field for reply {}", reply_id);
        return None;
    };

    let orig_msg = match serde_json::from_value::<Message>(msg_val.clone()) {
        Ok(m) => m,
        Err(e) => {
            warn!(
                "failed to parse get_msg message for reply {}: {}",
                reply_id, e
            );
            return None;
        }
    };

    let time_opt = ret.data.get("time").and_then(|v| v.as_i64());

    Some((sender_info, orig_msg, time_opt))
}

/// 清空当前积累的消息，调用 Dify，并在成功时回复且持久化会话 ID；失败则回填消息。
/// reply_msg_id 用于在被 @ 触发时给回复消息加上引用。
async fn clear_send_and_reply(
    bot: &kovi::RuntimeBot,
    reply_locks: Arc<Mutex<HashMap<i64, Arc<Mutex<()>>>>>,
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

    // Prepare per-group reply lock handle (lock only during send phase)
    let group_lock_arc = {
        let mut guard = reply_locks.lock().await;
        guard
            .entry(group_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    };
    let packaged = package_messages(data_path, img_path, &msgs_snapshot, cfg.vision.as_ref()).await;
    debug!("dify: packaged length={} chars", packaged.len());
    debug!("dify: packaged={:#?}", packaged);

    let attempts = (1 + cfg.dify_retry_times as usize).max(1);
    let mut last_err: Option<String> = None;
    let re = Regex::new(r"([，。？！]|\n+)").unwrap();
    let at_re = Regex::new(r"@\((\d+)\)").unwrap();

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
                let mut answer = resp.answer;
                let new_cid = resp.base.conversation_id.clone();
                info!(
                    "dify: got answer (attempt {}/{}), new_cid_set={} len={}",
                    attempt,
                    attempts,
                    new_cid.is_some(),
                    answer.len()
                );

                if answer.trim().is_empty() {
                    answer = "bot选择沉默。".to_string();
                }

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

                // Split and send (optional)
                let mut segments = Vec::new();
                if cfg.enable_split_messages {
                    // Regex to capture the delimiter as well
                    // Treat newlines as explicit separators regardless of limit
                    let answer_trimmed = answer.trim_matches(|c: char| c == '\r' || c == '\n');
                    let mut start = 0;
                    let max_segments = cfg.max_split_segments.max(1);

                    for mat in re.find_iter(answer_trimmed) {
                        let delimiter = mat.as_str();
                        let is_newline = delimiter.contains('\n');

                        // Check limit only for soft splits (non-newlines)
                        if !is_newline && segments.len() >= max_segments - 1 {
                            continue;
                        }

                        let content = &answer_trimmed[start..mat.start()];
                        let mut s = content.to_string();

                        // If delimiter is not a comma and not newline, keep it attached.
                        if !is_newline && delimiter != "，" {
                            s.push_str(delimiter);
                        }

                        if !s.trim().is_empty() {
                            segments.push(s);
                        }
                        start = mat.end();
                    }

                    // Add the remaining part as the last segment
                    if start < answer_trimmed.len() {
                        let s = &answer_trimmed[start..];
                        if !s.trim().is_empty() {
                            segments.push(s.to_string());
                        }
                    }

                    // Fallback single segment
                    if segments.is_empty() && !answer_trimmed.is_empty() {
                        segments.push(answer_trimmed.to_string());
                    }
                } else {
                    segments.push(answer);
                }

                let total_segments = segments.len();
                // Lock only for the send loop to avoid interleaving between concurrent replies
                let _reply_guard = group_lock_arc.lock().await;
                for (i, segment) in segments.into_iter().enumerate() {
                    let char_count = segment.chars().count();
                    let delay_per_char = if cfg.delay_per_char < 0.0 {
                        0.0
                    } else {
                        cfg.delay_per_char
                    };
                    let duration = if cfg.enable_split_messages {
                        Duration::from_secs_f64(match char_count as f64 * delay_per_char {
                            u if u > 1.5 => 1.5,
                            u => u,
                        })
                    } else {
                        Duration::from_secs(0)
                    };

                    debug!(
                        "sending segment {}/{} len={} delay={:?}",
                        i + 1,
                        total_segments,
                        char_count,
                        duration
                    );
                    kovi::tokio::time::sleep(duration).await;

                    let mut msg = build_message_with_mentions(&segment, &at_re);
                    if let Some(reply_id) = reply_msg_id
                        && i == 0
                    {
                        msg = msg.add_reply(reply_id);
                    }

                    bot.send_group_msg(group_id, msg);
                }
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

fn build_message_with_mentions(text: &str, at_re: &Regex) -> Message {
    let mut msg = Message::default();
    let mut last = 0;

    for caps in at_re.captures_iter(text) {
        let Some(mat) = caps.get(0) else {
            continue;
        };
        let Some(id) = caps.get(1).map(|v| v.as_str()) else {
            continue;
        };

        let before = &text[last..mat.start()];
        if !before.is_empty() {
            msg = msg.add_text(before);
        }
        msg = msg.add_at(id);
        last = mat.end();
    }

    let rest = &text[last..];
    if !rest.is_empty() {
        msg = msg.add_text(rest);
    }

    msg
}

async fn fetch_reply_content(bot: &kovi::RuntimeBot, reply_id: i32) -> Option<(String, String)> {
    let ret = match bot.get_msg(reply_id).await {
        Ok(v) => v,
        Err(e) => {
            warn!("get_msg failed for reply {}: {:?}", reply_id, e);
            return None;
        }
    };

    let sender = if let Some(v) = ret.data.get("sender") {
        v
    } else {
        warn!("get_msg returned no sender field for reply {}", reply_id);
        return None;
    };

    let sender_name = if let Some(v) = sender.get("nickname") {
        if let Some(v) = v.as_str() {
            v.to_string()
        } else {
            warn!(
                "get_msg returned non-string nickname for reply {}",
                reply_id
            );
            return None;
        }
    } else {
        warn!("get_msg returned no name field for reply {}", reply_id);
        return None;
    };

    let sender_id = if let Some(v) = sender.get("user_id") {
        if let Some(v) = v.as_i64() {
            v
        } else {
            warn!(
                "get_msg returned non-integer user_id for reply {}",
                reply_id
            );
            return None;
        }
    } else {
        warn!("get_msg returned no id field for reply {}", reply_id);
        return None;
    };

    let msg_val = if let Some(v) = ret.data.get("message") {
        v
    } else {
        warn!("get_msg returned no message field for reply {}", reply_id);
        return None;
    };

    match serde_json::from_value::<Message>(msg_val.clone()) {
        Ok(orig_msg) => Some((
            format!("{}({})", sender_name, sender_id),
            orig_msg.to_human_string(),
        )),
        Err(e) => {
            warn!(
                "failed to parse get_msg message for reply {}: {}",
                reply_id, e
            );
            None
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

    let msg_list = Arc::new(Mutex::new(initial_group_map));
    let last_seen = Arc::new(Mutex::new(HashMap::new()));
    let watching = Arc::new(Mutex::new(HashSet::new()));
    let mention_waiting = Arc::new(Mutex::new(HashMap::new()));
    let reply_locks = Arc::new(Mutex::new(HashMap::new()));

    let ctx = AppContext {
        msg_list,
        last_seen,
        watching,
        mention_waiting,
        reply_locks,
        config,
        data_path,
        img_path,
        runtime_bot,
        self_id,
    };

    plugin::on_group_msg(move |event| {
        let ctx = ctx.clone();
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
            let msg = event.message.clone();

            ctx.handle_group_msg(group_id, receive_time, sender, msg_id, msg)
                .await;
        }
    });
}
