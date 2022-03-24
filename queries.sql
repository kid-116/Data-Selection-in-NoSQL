-- (1)
SELECT name, surname 
FROM user JOIN comment USING (user_id);

-- (2)
SELECT * FROM user
JOIN comment USING (user_id)
WHERE text LIKE "%today%";

-- (3)
SELECT
    name,
    surname,
    created_at
FROM user JOIN comment USING (user_id)
WHERE
    created_at >= CURRENT_DATE AND
    created_at < DATE_ADD(CURRENT_DATE, INTERVAL 1 DAY);