CREATE DATABASE screenerusers;
\c screenerusers
CREATE TABLE bot_user(
id SERIAL PRIMARY KEY,
chat_id INT,
is_sub BOOLEAN);