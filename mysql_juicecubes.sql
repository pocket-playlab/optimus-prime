CREATE TABLE `items` (
  `item_id` mediumint(9) NOT NULL AUTO_INCREMENT,
  `item_name` varchar(255) NOT NULL,
  `item_price` mediumint(9) NOT NULL,
  PRIMARY KEY (`item_id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=latin1;

--
-- Dumping data for table `items`
--

LOCK TABLES `items` WRITE;
/*!40000 ALTER TABLE `items` DISABLE KEYS */;
INSERT INTO `items` VALUES (1,'itemA',100),(2,'itemB',200),(3,'itemC',2990);
/*!40000 ALTER TABLE `items` ENABLE KEYS */;
UNLOCK TABLES;
