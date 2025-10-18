-- 房间表
CREATE TABLE rooms (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    max_users INTEGER DEFAULT 100,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 用户表（简化版）
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    display_name VARCHAR(100),
    avatar_url TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 消息表
CREATE TABLE messages (
    id SERIAL PRIMARY KEY,
    room_id VARCHAR(50) REFERENCES rooms(id) ON DELETE CASCADE,
    user_id INTEGER REFERENCES users(id),
    message_type VARCHAR(20) NOT NULL CHECK (
        message_type IN ('text', 'image', 'file', 'system')
    ),
    content TEXT,
    file_url TEXT,
    file_name VARCHAR(255),
    file_size INTEGER,
    retention_hours INTEGER DEFAULT 24,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE
);

-- 房间用户关联表（记录用户在房间中的状态）
CREATE TABLE room_users (
    id SERIAL PRIMARY KEY,
    room_id VARCHAR(50) REFERENCES rooms(id) ON DELETE CASCADE,
    user_id INTEGER REFERENCES users(id),
    joined_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_read_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(room_id, user_id)
);

-- 创建索引
CREATE INDEX idx_messages_room_created ON messages(room_id, created_at DESC);

CREATE INDEX idx_messages_expires ON messages(expires_at)
WHERE
    expires_at IS NOT NULL;

CREATE INDEX idx_room_users_room ON room_users(room_id);