-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1:3380
-- Generation Time: Mar 19, 2025 at 04:22 AM
-- Server version: 10.4.32-MariaDB
-- PHP Version: 8.0.30

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `drs`
--

-- --------------------------------------------------------

--
-- Table structure for table `debt_cust_detail`
--

CREATE TABLE `debt_cust_detail` (
  `CUSTOMER_KEY` int(11) DEFAULT NULL,
  `ACCOUNT_KEY` int(11) DEFAULT NULL,
  `CUSTOMER_REF` varchar(30) DEFAULT NULL,
  `ACCOUNT_NUM` varchar(20) DEFAULT NULL,
  `ASSET_ID` varchar(15) DEFAULT NULL,
  `INTEGRATION_ID` varchar(30) DEFAULT NULL,
  `BSS_PRODUCT_ID` int(11) DEFAULT NULL,
  `PROMOTION_INTEG_ID` varchar(30) DEFAULT NULL,
  `OSS_SERVICE_ABBREVIATION` varchar(20) DEFAULT NULL,
  `ASSET_CREATED_DTM` date DEFAULT NULL,
  `PRODUCT_NAME` varchar(100) DEFAULT NULL,
  `ASSET_STATUS` varchar(30) DEFAULT NULL,
  `LAST_ORDER_COMPLETED_DTM` date DEFAULT NULL,
  `CONTACT_PERSON` varchar(100) DEFAULT NULL,
  `CONTACT_PHONE` varchar(40) DEFAULT NULL,
  `TECNICAL_CONTACT_EMAIL` varchar(350) DEFAULT NULL,
  `ASSET_ADDRESS` varchar(300) DEFAULT NULL,
  `RTOM` varchar(50) DEFAULT NULL,
  `LEA_CODE` varchar(30) DEFAULT NULL,
  `BSS_PRODUCT_SEQ` int(11) DEFAULT NULL,
  `GL_SEGMENT` varchar(40) DEFAULT NULL,
  `GL_SUBSEGMENT` varchar(255) DEFAULT NULL,
  `CUSTOMER_TYPE_ID` int(11) DEFAULT NULL,
  `CUSTOMER_TYPE_CAT` varchar(30) DEFAULT NULL,
  `CUSTOMER_TYPE` varchar(30) DEFAULT NULL,
  `ACC_ACCOUNT_KEY` int(11) DEFAULT NULL,
  `ACC_CUSTOMER_KEY` int(11) DEFAULT NULL,
  `ACC_ACCOUNT_NUM` varchar(20) DEFAULT NULL,
  `ACC_ACCOUNT_NAME` varchar(120) DEFAULT NULL,
  `ACCOUNT_STATUS_CRM` varchar(30) DEFAULT NULL,
  `ACCOUNT_STATUS_BSS` varchar(2) DEFAULT NULL,
  `ACCOUNT_START_DTM_CRM` date DEFAULT NULL,
  `ACCOUNT_START_DTM_BSS` date DEFAULT NULL,
  `ACCOUNT_EFFECTIVE_DTM_BSS` date DEFAULT NULL,
  `NAME` varchar(100) DEFAULT NULL,
  `TITLE` varchar(15) DEFAULT NULL,
  `INITIALS` varchar(120) DEFAULT NULL,
  `JOB_TITLE` varchar(75) DEFAULT NULL,
  `FIRST_NAME` varchar(50) DEFAULT NULL,
  `MID_NAME` varchar(100) DEFAULT NULL,
  `LAST_NAME` varchar(100) DEFAULT NULL,
  `NAME_POST_FIX` varchar(120) DEFAULT NULL,
  `NAME_POST_FIX_UPPER` varchar(120) DEFAULT NULL,
  `ADDR_LINE_1` varchar(200) DEFAULT NULL,
  `ADDR_LINE_2` varchar(100) DEFAULT NULL,
  `ADDR_LINE_3` varchar(100) DEFAULT NULL,
  `ADDR_LINE_4` varchar(100) DEFAULT NULL,
  `ADDR_LINE_5` varchar(100) DEFAULT NULL,
  `ADDR_FULL` varchar(300) DEFAULT NULL,
  `NIC` varchar(20) DEFAULT NULL,
  `PASSPORT` varchar(30) DEFAULT NULL,
  `SEX` varchar(30) DEFAULT NULL,
  `ZIP_CODE` varchar(30) DEFAULT NULL,
  `CITY` varchar(50) DEFAULT NULL,
  `DISTRICT` varchar(50) DEFAULT NULL,
  `PROVINCE` varchar(50) DEFAULT NULL,
  `COUNTRY` varchar(30) DEFAULT NULL,
  `COUNTRY_ID` int(11) DEFAULT NULL,
  `EMAIL` varchar(350) DEFAULT NULL,
  `MOBILE_CONTACT` varchar(40) DEFAULT NULL,
  `WORK_CONTACT` varchar(40) DEFAULT NULL,
  `DAYTIME_CONTACT_TEL` varchar(20) DEFAULT NULL,
  `EVENING_CONTACT_TEL` varchar(20) DEFAULT NULL,
  `MOBILE_CONTACT_TEL` varchar(20) DEFAULT NULL,
  `DAYTIME_EXTENSION` varchar(10) DEFAULT NULL,
  `EVENING_EXTENSION` varchar(10) DEFAULT NULL,
  `CONTACT_SEQ` int(11) DEFAULT NULL,
  `ADDRESS_SEQ` int(11) DEFAULT NULL,
  `CUSTOMER_SEGMENT_ID` varchar(20) DEFAULT NULL,
  `CUSTOMER_SEGMENT_DESC` varchar(40) DEFAULT NULL,
  `BILLING_CETNER_CODE` varchar(10) DEFAULT NULL,
  `BILLING_CENTER_NAME` varchar(30) DEFAULT NULL,
  `COST_CENTER_CODE` varchar(30) DEFAULT NULL,
  `COST_CENTER_NAME` varchar(30) DEFAULT NULL,
  `CURRENCY_CODE` varchar(20) DEFAULT NULL,
  `LANGUAGE_CODE` varchar(15) DEFAULT NULL,
  `CPS_ID` int(11) DEFAULT NULL,
  `CPS_DESC` varchar(30) DEFAULT NULL,
  `COMPANY_NAME` varchar(100) DEFAULT NULL,
  `INVOICING_COMPANY_ID` int(11) DEFAULT NULL,
  `INVOICING_COMPANY` varchar(30) DEFAULT NULL,
  `BILL_PERIOD` int(11) DEFAULT NULL,
  `DEPOSIT_AMT` decimal(18,2) DEFAULT NULL,
  `TOTAL_BILLED` decimal(18,2) DEFAULT NULL,
  `TOTAL_PAID` decimal(18,2) DEFAULT NULL,
  `OUTSTANDING` decimal(18,2) DEFAULT NULL,
  `LAST_BILL_DTM` date DEFAULT NULL,
  `NEXT_BILL_DTM` date DEFAULT NULL,
  `TAX_INCLUSIVE_BOO` varchar(1) DEFAULT NULL,
  `TERMINATION_DAT` date DEFAULT NULL,
  `TERMINATION_REASON_ID` int(11) DEFAULT NULL,
  `LAST_BILL_SEQ` int(11) DEFAULT NULL,
  `CREDIT_CLASS_ID` int(11) DEFAULT NULL,
  `CREDIT_CLASS_NAME` varchar(60) DEFAULT NULL,
  `BILLING_CONTACT_SEQ` int(11) DEFAULT NULL,
  `BILL_HANDLING_CODE` varchar(10) DEFAULT NULL,
  `LAST_PAYMENT_DAT` date DEFAULT NULL,
  `LAST_PAYMENT_MNY` decimal(18,2) DEFAULT NULL,
  `PRICE_LIST_DESC` varchar(50) DEFAULT NULL,
  `INVOICING_MEDIA_TYPE` varchar(30) DEFAULT NULL,
  `INVOICING_BILL_MOD2` varchar(30) DEFAULT NULL,
  `INVOICING_BILL_MOD3` varchar(30) DEFAULT NULL,
  `CUSTOMER_CATEGORY` int(11) DEFAULT NULL,
  `DOMAIN_ID` int(11) DEFAULT NULL,
  `REAL_TIME_COMMUNICATION` varchar(20) DEFAULT NULL,
  `EXCHANGE_CODE` varchar(20) DEFAULT NULL,
  `OLD_ACCOUNT_NO` varchar(40) DEFAULT NULL,
  `OLD_CUSTOMER_NO` varchar(40) DEFAULT NULL,
  `LANG_CODE` varchar(3) DEFAULT NULL,
  `WRITEOFF_STATUS` varchar(1) DEFAULT NULL,
  `MCG_ID` varchar(20) DEFAULT NULL,
  `BILL_CYCLE` varchar(3) DEFAULT NULL,
  `ADDRESS_NAME_N_REQIURED` varchar(20) DEFAULT NULL,
  `LOAD_DATE` date DEFAULT NULL,
  `LOAD_USER` varchar(128) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `debt_cust_detail`
--

INSERT INTO `debt_cust_detail` (`CUSTOMER_KEY`, `ACCOUNT_KEY`, `CUSTOMER_REF`, `ACCOUNT_NUM`, `ASSET_ID`, `INTEGRATION_ID`, `BSS_PRODUCT_ID`, `PROMOTION_INTEG_ID`, `OSS_SERVICE_ABBREVIATION`, `ASSET_CREATED_DTM`, `PRODUCT_NAME`, `ASSET_STATUS`, `LAST_ORDER_COMPLETED_DTM`, `CONTACT_PERSON`, `CONTACT_PHONE`, `TECNICAL_CONTACT_EMAIL`, `ASSET_ADDRESS`, `RTOM`, `LEA_CODE`, `BSS_PRODUCT_SEQ`, `GL_SEGMENT`, `GL_SUBSEGMENT`, `CUSTOMER_TYPE_ID`, `CUSTOMER_TYPE_CAT`, `CUSTOMER_TYPE`, `ACC_ACCOUNT_KEY`, `ACC_CUSTOMER_KEY`, `ACC_ACCOUNT_NUM`, `ACC_ACCOUNT_NAME`, `ACCOUNT_STATUS_CRM`, `ACCOUNT_STATUS_BSS`, `ACCOUNT_START_DTM_CRM`, `ACCOUNT_START_DTM_BSS`, `ACCOUNT_EFFECTIVE_DTM_BSS`, `NAME`, `TITLE`, `INITIALS`, `JOB_TITLE`, `FIRST_NAME`, `MID_NAME`, `LAST_NAME`, `NAME_POST_FIX`, `NAME_POST_FIX_UPPER`, `ADDR_LINE_1`, `ADDR_LINE_2`, `ADDR_LINE_3`, `ADDR_LINE_4`, `ADDR_LINE_5`, `ADDR_FULL`, `NIC`, `PASSPORT`, `SEX`, `ZIP_CODE`, `CITY`, `DISTRICT`, `PROVINCE`, `COUNTRY`, `COUNTRY_ID`, `EMAIL`, `MOBILE_CONTACT`, `WORK_CONTACT`, `DAYTIME_CONTACT_TEL`, `EVENING_CONTACT_TEL`, `MOBILE_CONTACT_TEL`, `DAYTIME_EXTENSION`, `EVENING_EXTENSION`, `CONTACT_SEQ`, `ADDRESS_SEQ`, `CUSTOMER_SEGMENT_ID`, `CUSTOMER_SEGMENT_DESC`, `BILLING_CETNER_CODE`, `BILLING_CENTER_NAME`, `COST_CENTER_CODE`, `COST_CENTER_NAME`, `CURRENCY_CODE`, `LANGUAGE_CODE`, `CPS_ID`, `CPS_DESC`, `COMPANY_NAME`, `INVOICING_COMPANY_ID`, `INVOICING_COMPANY`, `BILL_PERIOD`, `DEPOSIT_AMT`, `TOTAL_BILLED`, `TOTAL_PAID`, `OUTSTANDING`, `LAST_BILL_DTM`, `NEXT_BILL_DTM`, `TAX_INCLUSIVE_BOO`, `TERMINATION_DAT`, `TERMINATION_REASON_ID`, `LAST_BILL_SEQ`, `CREDIT_CLASS_ID`, `CREDIT_CLASS_NAME`, `BILLING_CONTACT_SEQ`, `BILL_HANDLING_CODE`, `LAST_PAYMENT_DAT`, `LAST_PAYMENT_MNY`, `PRICE_LIST_DESC`, `INVOICING_MEDIA_TYPE`, `INVOICING_BILL_MOD2`, `INVOICING_BILL_MOD3`, `CUSTOMER_CATEGORY`, `DOMAIN_ID`, `REAL_TIME_COMMUNICATION`, `EXCHANGE_CODE`, `OLD_ACCOUNT_NO`, `OLD_CUSTOMER_NO`, `LANG_CODE`, `WRITEOFF_STATUS`, `MCG_ID`, `BILL_CYCLE`, `ADDRESS_NAME_N_REQIURED`, `LOAD_DATE`, `LOAD_USER`) VALUES
(1281, 1281, 'CR000000361', '0000003614', '1-2RDZ-341', '1-2RDZ-341', 1529, '1-2VSX-1861', 'V-VOICE FTTH', '2018-11-10', 'V_SLT Voice Service', 'Inactive', '2024-12-11', 'Mrs Irene Kelart', '0779066274', NULL, '-, Charlie Hill, -, -, Avissawella, Colombo, Western, Sri Lanka, 10700', 'RTO - HO', 'AW', 35, 'Sales', 'Retail', 1, 'Individual', 'Individual-Residential', 1281, 1281, '0000003614', NULL, 'Active', 'OK', '1997-01-02', '2024-12-30', '1997-01-02', 'Mrs Irene Kelart', 'Mrs', NULL, 'Not gathered', '-', 'Mrs Irene Kelart', 'Irene Kelart', '.', '.', '-', 'Charlie Hill', '-', '-', NULL, '-, Charlie Hill, -, -, Avissawella, Colombo, Western, Sri Lanka, 10700', NULL, NULL, 'Female', '10700', 'Avissawella', 'Colombo', 'Western', 'Sri Lanka', 101, NULL, '0779066274', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '05', 'FTTH', 'AW', 'AWISSAWELLA', '1598', '1598', 'LKR', 'ENU', 9, 'Customer-Non VAT Registered', NULL, 1, 'SLT', 1, 0.00, 542539.10, 510335.00, 32204.10, '2025-02-01', '2025-03-01', 'F', NULL, NULL, 258, 1, 'Residential - Low', 2, '01', '2023-09-12', 8630000.00, 'SLT Master Price List', 'E-Statement by SMS', NULL, NULL, NULL, 515953074, 'S', 'AW', '361', '361', 'ENG', '0', NULL, NULL, NULL, '2025-02-22', 'Administrator'),
(1291, 1291, 'CR000000374', '0000003746', '1-2OPD-327', '1-2OPD-327', 1604, '1-DQLZNH6', 'E-IPTV FTTH', '2017-02-22', 'E_SLT Peo TV Service', 'Inactive', '2024-04-01', 'Mrs B . G . S . Ganegoda', '0773320486', '0703520449', '202/18, Manamendra Mw, , Ukwattha, Avissawella, Colombo, Western, Sri Lanka, 10700', 'RTO - HO', 'AW', 23, 'Sales', 'Retail', 1, 'Individual', 'Individual-Residential', 1291, 1291, '0000003746', NULL, 'Active', 'OK', '1987-06-23', '2011-04-01', '1987-06-23', 'Mrs B . G . S . Ganegoda', 'Mrs', 'B.G.S.', 'Not gathered', '-', 'Mrs B . G . S . Ganegoda', 'Ganegoda', '465630922V', '465630922V', '202/2', 'Manamendra Mawatha', '-', '-', NULL, '202/2, Manamendra Mawatha, -, -, Avissawella, Colombo, Western, Sri Lanka, 10700', NULL, NULL, 'Female', '10700', 'Avissawella', 'Colombo', 'Western', 'Sri Lanka', 101, NULL, '0773320486', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '05', 'FTTH', 'AW', 'AWISSAWELLA', '1598', '1598', 'LKR', 'ENU', 9, 'Customer-Non VAT Registered', NULL, 1, 'SLT', 1, 0.00, 390202.64, 376244.00, 13958.64, '2024-08-01', '2024-09-01', 'F', NULL, NULL, 255, 1, 'Residential - Low', 2, '01', '2023-11-30', 5000000.00, 'SLT Master Price List', 'Hard Copy', NULL, NULL, NULL, 390622801, 'S', 'AW', '374', '374', 'ENG', '0', NULL, NULL, NULL, '2025-02-22', 'Administrator'),
(1291, 1291, 'CR000000374', '0000003746', '1-2OPD-327', '1-2OPD-327', 1604, '1-DQLZNH6', 'E-IPTV FTTH', '2017-02-22', 'E_SLT Peo TV Service', 'Inactive', '2024-04-01', 'Mrs B . G . S . Ganegoda', '0773320486', '0703520449', '202/18, Manamendra Mw, , Ukwattha, Avissawella, Colombo, Western, Sri Lanka, 10700', 'RTO - HO', 'AW', 23, 'Sales', 'Retail', 1, 'Individual', 'Individual-Residential', 1291, 1291, '0000003746', NULL, 'Active', 'OK', '1987-06-23', '2011-04-01', '1987-06-23', 'Mrs B . G . S . Ganegoda', 'Mrs', 'B.G.S.', 'Not gathered', '-', 'Mrs B . G . S . Ganegoda', 'Ganegoda', '465630922V', '465630922V', '202/2', 'Manamendra Mawatha', '-', '-', NULL, '202/2, Manamendra Mawatha, -, -, Avissawella, Colombo, Western, Sri Lanka, 10700', NULL, NULL, 'Female', '10700', 'Avissawella', 'Colombo', 'Western', 'Sri Lanka', 101, NULL, '0773320486', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '05', 'FTTH', 'AW', 'AWISSAWELLA', '1598', '1598', 'LKR', 'ENU', 9, 'Customer-Non VAT Registered', NULL, 1, 'SLT', 1, 0.00, 390202.64, 376244.00, 13958.64, '2024-08-01', '2024-09-01', 'F', NULL, NULL, 255, 1, 'Residential - Low', 2, '01', '2023-11-30', 5000000.00, 'SLT Master Price List', 'Hard Copy', NULL, NULL, NULL, 390622801, 'S', 'AW', '374', '374', 'ENG', '0', NULL, NULL, NULL, '2025-02-22', 'Administrator'),
(1291, 1291, 'CR000000374', '0000003746', '1-2X4Z-505', '1-2X4Z-505', 1612, '1-DQLZNH6', 'AB-FTTH', '2016-10-05', 'AB Fiber Access Bearer', 'Inactive', '2024-04-01', 'Mrs B . G . S . Ganegoda', '0773320486', '0703520449', '202/18, Manamendra Mw, , Ukwattha, Avissawella, Colombo, Western, Sri Lanka, 10700', 'RTO - HO', 'AW', 20, 'Sales', 'Retail', 1, 'Individual', 'Individual-Residential', 1291, 1291, '0000003746', NULL, 'Active', 'OK', '1987-06-23', '2011-04-01', '1987-06-23', 'Mrs B . G . S . Ganegoda', 'Mrs', 'B.G.S.', 'Not gathered', '-', 'Mrs B . G . S . Ganegoda', 'Ganegoda', '465630922V', '465630922V', '202/2', 'Manamendra Mawatha', '-', '-', NULL, '202/2, Manamendra Mawatha, -, -, Avissawella, Colombo, Western, Sri Lanka, 10700', NULL, NULL, 'Female', '10700', 'Avissawella', 'Colombo', 'Western', 'Sri Lanka', 101, NULL, '0773320486', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '05', 'FTTH', 'AW', 'AWISSAWELLA', '1598', '1598', 'LKR', 'ENU', 9, 'Customer-Non VAT Registered', NULL, 1, 'SLT', 1, 0.00, 390202.64, 376244.00, 13958.64, '2024-08-01', '2024-09-01', 'F', NULL, NULL, 255, 1, 'Residential - Low', 2, '01', '2023-11-30', 5000000.00, 'SLT Master Price List', 'Hard Copy', NULL, NULL, NULL, 390622801, 'S', 'AW', '374', '374', 'ENG', '0', NULL, NULL, NULL, '2025-02-22', 'Administrator'),
(1312, 1312, 'CR000000399', '0000003998', '1-2S4Q-1547', '1-2S4Q-1547', 1529, '1-2TNT-1896', 'V-VOICE FTTH', '2015-08-31', 'V_SLT Voice Service', 'Inactive', '2021-04-06', 'Mr D P Saputhantri', '0773493459', NULL, '143/3, Manamendra Mawatha, -, -, Avissawella, Colombo, Western, Sri Lanka, 10700', 'RTO - AW', 'AW', 40, 'Sales', 'Retail', 1, 'Individual', 'Individual-Residential', 1312, 1312, '0000003998', NULL, 'Active', 'OK', '1980-02-08', '2019-09-30', '1980-02-08', 'Mr D P Saputhantri', 'Mr', 'DP', 'Not gathered', '-', 'Mr D P Saputhantri', 'Saputhantri', ' ', ' ', '143/13', 'Manamendra Mw', 'Rathnapura Road', '-', NULL, '143/13, Manamendra Mw, Rathnapura Road, -, Avissawella, Colombo, Western, Sri Lanka, 10700', NULL, NULL, 'Male', '10700', 'Avissawella', 'Colombo', 'Western', 'Sri Lanka', 101, 'gayansapu@sltnet.lk', '0773493459', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '05', 'FTTH', 'AW', 'AWISSAWELLA', '1598', '1598', 'LKR', 'ENU', 9, 'Customer-Non VAT Registered', NULL, 1, 'SLT', 1, 0.00, 614396.80, 614396.80, 0.00, '2025-02-01', '2025-03-01', 'F', NULL, NULL, 256, 5, 'Residential - Medium', 3, '01', '2025-02-13', 770780.00, 'SLT Master Price List', 'Prestige-Post', NULL, NULL, NULL, 545752830, 'S', 'AW', '399', '399', 'ENG', '0', NULL, NULL, NULL, '2025-02-22', 'Administrator');
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
