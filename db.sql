CREATE DATABASE social;

USE social;

CREATE TABLE user (
    user_id INT PRIMARY KEY,
    name VARCHAR(45),
    surname VARCHAR(45),
    identity_card VARCHAR(45)
);

CREATE TABLE comment (
    comment_id INT,
    text VARCHAR(255),
    created_at DATETIME,
    user_id INT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES user(user_id)
);