
USE `sample_db`;

CREATE TABLE `roles`(
    `id`       INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `rolename` VARCHAR(64) NOT NULL
);


CREATE TABLE `users`(
    `id`       INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `role_id`  INT NOT NULL,
    `username` VARCHAR(64) NOT NULL,
    `age`      INT NOT NULL
);

insert into `roles`(`id`, `rolename`) values(1, 'normal'),(2, 'admin');
insert into `users`(`id`, `role_id`, `username`, `age`) values 
(1, 1, 'Default User', 20),
(2, 2, 'Administrator', 30),
(3, 1, 'Mark Hoge', 25);
