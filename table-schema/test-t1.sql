CREATE TABLE `test`.`t1` (
  `a` bigint(20) NOT NULL,
  `b` bigint(20) NOT NULL,
  `c` varchar(32) NOT NULL,
  `d` varchar(128) NOT NULL,
  `e` varchar(128) NOT NULL,
  UNIQUE KEY `uk` (`a`,`b`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
