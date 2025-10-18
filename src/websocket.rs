use actix::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Message)]
#[rtype(result = "()")]
pub struct WsMessage(pub String);

#[derive(Message)]
#[rtype(result = "()")]
pub struct Connect {
    pub addr: Recipient<WsMessage>,
    pub room_id: String,
    pub user_id: i32,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct Disconnect {
    pub room_id: String,
    pub user_id: i32,
}

#[derive(Message, Serialize, Deserialize, Clone)]
#[rtype(result = "()")]
pub struct ChatMessage {
    pub room_id: String,
    pub user_id: i32,
    pub message_type: String,
    pub content: Option<String>,
    pub file_url: Option<String>,
    pub file_name: Option<String>,
    pub file_size: Option<i32>,
    pub retention_hours: i32,
}

pub struct ChatServer {
    sessions: HashMap<String, HashMap<i32, Recipient<WsMessage>>>,
}

impl ChatServer {
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
        }
    }

    pub fn send_message(&self, room_id: &str, message: &str) {
        if let Some(sessions) = self.sessions.get(room_id) {
            for recipient in sessions.values() {
                let _ = recipient.do_send(WsMessage(message.to_owned()));
            }
        }
    }
}

impl Actor for ChatServer {
    type Context = Context<Self>;
}

impl Handler<Connect> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: Connect, _: &mut Context<Self>) -> Self::Result {
        self.sessions
            .entry(msg.room_id.clone())
            .or_insert_with(HashMap::new)
            .insert(msg.user_id, msg.addr);
    }
}

impl Handler<Disconnect> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) -> Self::Result {
        if let Some(sessions) = self.sessions.get_mut(&msg.room_id) {
            sessions.remove(&msg.user_id);
            if sessions.is_empty() {
                self.sessions.remove(&msg.room_id);
            }
        }
    }
}

impl Handler<ChatMessage> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: ChatMessage, _: &mut Context<Self>) -> Self::Result {
        let message_json = serde_json::to_string(&msg).unwrap();
        self.send_message(&msg.room_id, &message_json);
    }
}
