use std::path::PathBuf;

use kovi::bot::event::{Sender, Sex};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SenderInfo {
    pub user_id: i64,
    pub nickname: Option<String>,
    pub card: Option<String>,
    pub sex: Option<String>, // "male" / "female"
    pub age: Option<i32>,
    pub area: Option<String>,
    pub level: Option<String>,
    pub role: Option<String>,
    pub title: Option<String>,
}

impl From<&Sender> for SenderInfo {
    fn from(s: &Sender) -> Self {
        let sex = match s.sex {
            Some(Sex::Male) => Some("male".to_string()),
            Some(Sex::Female) => Some("female".to_string()),
            None => None,
        };

        SenderInfo {
            user_id: s.user_id,
            nickname: s.nickname.clone(),
            card: s.card.clone(),
            sex,
            age: s.age,
            area: s.area.clone(),
            level: s.level.clone(),
            role: s.role.clone(),
            title: s.title.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MyMsgContent {
    Text(String),
    Img(PathBuf),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MyMsg {
    pub group_id: i64,
    pub time: i64,
    pub sender: SenderInfo,
    pub msg_id: i32,
    pub content: MyMsgContent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MyMsgList {
    pub messages: Vec<MyMsg>,
    pub conversation_id: Option<String>,
}
