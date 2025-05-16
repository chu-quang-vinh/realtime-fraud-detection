-- Tạo bảng lưu trữ giao dịch
CREATE TABLE IF NOT EXISTS transactions_history (
    transaction_id VARCHAR(255) PRIMARY KEY,
    user_id INTEGER NOT NULL,
    amount DECIMAL(20, 2) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    timestamp BIGINT NOT NULL,
    datetime TIMESTAMP,
    location VARCHAR(255),
    merchant_id INTEGER,
    is_suspicious_rule BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tạo index cho các trường thường xuyên truy vấn
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions_history(datetime);
CREATE INDEX IF NOT EXISTS idx_transactions_suspicious ON transactions_history(is_suspicious_rule);
CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions_history(user_id);