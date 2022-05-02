
USE `testdb`;

DROP TABLE IF EXISTS `categories`;
CREATE TABLE `categories`(
    `id` INT NOT NULL PRIMARY KEY,
    `name` VARCHAR(64) NOT NULL
);

DROP TABLE IF EXISTS `products`;
CREATE TABLE `products`(
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `category_id` INT NOT NULL REFERENCES `categories`(`id`),
    `name` VARCHAR(64) NOT NULL,
    `price` INT
);

DROP TABLE IF EXISTS `users`;
CREATE TABLE `users`(
    `id` INT NOT NULL PRIMARY KEY,
    `name` VARCHAR(64) NOT NULL,
    `registered_dt` DATETIME
);

DROP TABLE IF EXISTS `user_sales`;
CREATE TABLE `user_sales`(
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `user_id` INT NOT NULL REFERENCES `users`(`id`),
    `datetime` DATETIME
);

DROP TABLE IF EXISTS `user_sale_products`;
CREATE TABLE `user_sale_products`(
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `user_sale_id` INT NOT NULL REFERENCES `user_sales`(`id`),
    `product_id` INT NOT NULL REFERENCES `products`(`id`),
    `price` INT NOT NULL,
    `count` INT NOT NULL
);


INSERT INTO `categories` VALUES
    (1, 'Desktop Computer'),
    (2, 'Laptop Computer'),
    (3, 'Display'),
    (4, 'Keyboard'),
    (5, 'Mouse'),
    (6, 'Cables');

INSERT INTO `products`(`id`, `category_id`, `name`, `price`) VALUES
    ( 1, 1, 'Office PC #01 CPU 2cores, RAM 4GB, SSD 256GB', 60000),
    ( 2, 1, 'Office PC #02 CPU 4cores, RAM 8GB, SSD 256GB', 75000),
    ( 3, 1, 'Office PC #03 CPU 6cores, RAM 8GB, SSD 1TB', 89000),
    ( 4, 1, 'Gaming PC #01 CPU 16cores, GPU 3000, RAM 16GB, SSD 2TB', 140000),
    ( 5, 1, 'Gaming PC #02 CPU 8cores, GPU 2000, RAM 8GB, SSD 1TB', 120000),
    ( 6, 1, 'Gaming PC #03 CPU 4cores, GPU 1000, RAM 8GB, SSD 512GB', 83000),
    ( 7, 2, 'Notebook CPU 2cores, RAM 4GB, SSD 128GB', 65000),
    ( 8, 2, 'Super Notebook CPU 4cores, RAM 8GB, SSD 512GB', 95000),
    ( 9, 2, 'High Notebook CPU 4cores, RAM 8GB, SSD 256GB', 79000),
    (10, 3, 'Display 24-inch Full-HD', 20000),
    (11, 3, 'Display 26-inch Full-HD', 25000),
    (12, 3, 'Display 27-inch Full-HD', 30000),
    (13, 3, 'Display 28-inch Full-HD', 32000),
    (14, 3, 'Display 26-inch 4K', 30000),
    (15, 3, 'Display 28-inch 4K', 40000),
    (16, 3, 'Display 30-inch 4K', 50000),
    (17, 4, 'Lowcost Keyboard', 2000),
    (18, 4, 'Silver Keyboard', 9000),
    (19, 4, 'Red Keyboard', 7000),
    (20, 4, 'Blue Keyboard', 8000),
    (21, 4, 'Wireless Keyboard', 5000),
    (22, 5, 'Gaming Mouse', 6500),
    (23, 5, 'Lowcost Mouse', 1500),
    (24, 5, 'Smart Mouse', 3200),
    (25, 5, 'Wireless Mouse', 4000),
    (26, 6, 'Power cable', 800),
    (27, 6, 'Display cable 1m', 1500),
    (28, 6, 'Display cable 2m', 1800),
    (29, 6, 'USB cable 1m', 400),
    (30, 6, 'USB cable 2m', 600);

INSERT INTO `users`(`id`, `name`) VALUES
    ( 1, 'Admin'),
    ( 2, 'Bob'),
    ( 3, 'UserC'),
    ( 4, 'Darrr25'),
    ( 5, 'Emi'),
    ( 6, 'F-user'),
    ( 7, 'Gone'),
    ( 8, 'Hooly'),
    ( 9, 'user-I'),
    (10, 'John'),
    (11, 'Kebin'),
    (12, 'Tom'),
    (13, 'Abceg'),
    (14, 'Qwerty');

INSERT INTO `user_sales`(`id`, `user_id`, `datetime`) VALUES
    ( 1,  4, '2022-04-05 12:34:56'),
    ( 2,  7, '2022-04-08 09:41:16'),
    ( 3, 13, '2022-04-12 03:13:21'),
    ( 4, 13, '2022-04-13 04:03:25'),
    ( 5,  9, '2022-04-15 02:05:09'),
    ( 6,  4, '2022-04-15 12:01:39'),
    ( 7,  5, '2022-04-15 15:41:29'),
    ( 8,  8, '2022-04-17 14:15:42'),
    ( 9, 13, '2022-04-17 14:59:23'),
    (10,  7, '2022-04-18 17:36:32'),
    (11, 10, '2022-04-21 12:53:17');

INSERT INTO `user_sale_products`(`user_sale_id`, `product_id`, `price`, `count`) VALUES
    (1, 5, 118000, 1),
    (1, 22, 6500, 1),
    (2, 2, 78000, 1),
    (2, 11, 25000, 1),
    (2, 23, 1500, 1),
    (2, 30, 600, 2),
    (3, 12, 24000, 1),
    (3, 27, 1500, 3),
    (3, 23, 1600, 1),
    (3, 30, 600, 2),
    (4, 19, 6600, 1),
    (5, 14, 35000, 2),
    (6, 30, 550, 4),
    (6, 27, 1550, 1),
    (7, 3, 89000, 1);
