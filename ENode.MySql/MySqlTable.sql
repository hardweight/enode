-- MySQL dump 10.13  Distrib 5.6.27, for osx10.10 (x86_64)
--
-- Host: localhost    Database: enode
-- ------------------------------------------------------
-- Server version	5.6.27

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `Command`
--

DROP TABLE IF EXISTS `Command`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Command` (
  `Sequence` bigint(20) NOT NULL AUTO_INCREMENT,
  `CommandId` varchar(36) NOT NULL,
  `AggregateRootId` varchar(36) DEFAULT NULL,
  `MessagePayload` longtext,
  `MessageTypeName` varchar(255) DEFAULT NULL,
  `CreatedOn` datetime DEFAULT NULL,
  PRIMARY KEY (`CommandId`),
  UNIQUE KEY `Sequence_UNIQUE` (`Sequence`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `EventStream`
--

DROP TABLE IF EXISTS `EventStream`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `EventStream` (
  `Sequence` bigint(20) NOT NULL AUTO_INCREMENT,
  `AggregateRootTypeName` varchar(255) DEFAULT NULL,
  `AggregateRootId` varchar(36) NOT NULL,
  `Version` int(11) NOT NULL,
  `CommandId` varchar(36) DEFAULT NULL,
  `CreatedOn` datetime DEFAULT NULL,
  `Events` longtext,
  PRIMARY KEY (`AggregateRootId`,`Version`),
  UNIQUE KEY `Sequence_UNIQUE` (`Sequence`),
  UNIQUE KEY `IX_EventStream_AggId_CommandId` (`AggregateRootId`,`CommandId`)
) ENGINE=InnoDB AUTO_INCREMENT=35 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `LockKey`
--

DROP TABLE IF EXISTS `LockKey`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `LockKey` (
  `Name` varchar(128) NOT NULL,
  PRIMARY KEY (`Name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;



--
-- Table structure for table `MessageHandleRecord`
--

DROP TABLE IF EXISTS `MessageHandleRecord`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `MessageHandleRecord` (
  `Sequence` bigint(20) NOT NULL AUTO_INCREMENT,
  `MessageId` varchar(36) NOT NULL,
  `HandlerTypeName` varchar(255) NOT NULL,
  `MessageTypeName` varchar(255) DEFAULT NULL,
  `AggregateRootTypeName` varchar(255) DEFAULT NULL,
  `AggregateRootId` varchar(36) DEFAULT NULL,
  `Version` int(11) DEFAULT NULL,
  `CreatedOn` datetime DEFAULT NULL,
  PRIMARY KEY (`MessageId`,`HandlerTypeName`),
  UNIQUE KEY `Sequence_UNIQUE` (`Sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `SequenceMessagePublishedVersion`
--

DROP TABLE IF EXISTS `SequenceMessagePublishedVersion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SequenceMessagePublishedVersion` (
  `Sequence` bigint(20) NOT NULL AUTO_INCREMENT,
  `ProcessorName` varchar(128) NOT NULL,
  `AggregateRootTypeName` varchar(255) DEFAULT NULL,
  `AggregateRootId` varchar(36) NOT NULL,
  `PublishedVersion` int(11) NOT NULL,
  `CreatedOn` datetime DEFAULT NULL,
  PRIMARY KEY (`ProcessorName`,`AggregateRootId`,`PublishedVersion`),
  UNIQUE KEY `Sequence_UNIQUE` (`Sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ThreeMessageHandleRecord`
--

DROP TABLE IF EXISTS `ThreeMessageHandleRecord`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ThreeMessageHandleRecord` (
  `Sequence` bigint(20) NOT NULL AUTO_INCREMENT,
  `MessageId1` varchar(36) NOT NULL,
  `MessageId2` varchar(36) NOT NULL,
  `MessageId3` varchar(36) NOT NULL,
  `HandlerTypeName` varchar(255) NOT NULL,
  `Message1TypeName` varchar(255) DEFAULT NULL,
  `Message2TypeName` varchar(255) DEFAULT NULL,
  `Message3TypeName` varchar(255) DEFAULT NULL,
  `AggregateRootTypeName` varchar(255) DEFAULT NULL,
  `AggregateRootId` varchar(36) DEFAULT NULL,
  `Version` int(11) DEFAULT NULL,
  `CreatedOn` datetime DEFAULT NULL,
  PRIMARY KEY (`MessageId1`,`MessageId2`,`MessageId3`,`HandlerTypeName`),
  UNIQUE KEY `Sequence_UNIQUE` (`Sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `TwoMessageHandleRecord`
--

DROP TABLE IF EXISTS `TwoMessageHandleRecord`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TwoMessageHandleRecord` (
  `Sequence` bigint(20) NOT NULL AUTO_INCREMENT,
  `MessageId1` varchar(36) NOT NULL,
  `MessageId2` varchar(36) NOT NULL,
  `HandlerTypeName` varchar(255) NOT NULL,
  `Message1TypeName` varchar(255) DEFAULT NULL,
  `Message2TypeName` varchar(255) DEFAULT NULL,
  `AggregateRootTypeName` varchar(255) DEFAULT NULL,
  `AggregateRootId` varchar(36) DEFAULT NULL,
  `Version` int(11) DEFAULT NULL,
  `CreatedOn` datetime DEFAULT NULL,
  PRIMARY KEY (`MessageId1`,`MessageId2`,`HandlerTypeName`),
  UNIQUE KEY `IX_MessageId1_MessageId2_HandlerTypeName` (`MessageId1`,`MessageId2`,`HandlerTypeName`),
  UNIQUE KEY `Sequence_UNIQUE` (`Sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2016-02-26 22:05:52
