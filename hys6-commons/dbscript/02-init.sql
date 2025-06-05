-- keytable
delete from keytable;
insert into keytable (key_name, key_value)
values  ('roleid', 20),
        ('batchno', 20),
        ('hrds', 20),
        ('paldbcount', 0),
        ('i001', 10),
        ('tellers', 435);

delete from KEYTABLE_SNOWFLAKE;
insert into keytable_snowflake (project_id, datacenter_id, machine_id)
values  ('a', 0, 1),
        ('b', 0, 2),
        ('c', 0, 3),
        ('d', 0, 4),
        ('e', 0, 5),
        ('f', 0, 6),
        ('g', 0, 7),
        ('h', 0, 8),
        ('i', 0, 9),
        ('j', 0, 10),
        ('k', 0, 11),
        ('l', 0, 12),
        ('m', 0, 13),
        ('n', 0, 14),
        ('o', 0, 15),
        ('p', 0, 16),
        ('q', 0, 17),
        ('r', 0, 18),
        ('s', 0, 19),
        ('t', 0, 20),
        ('u', 0, 21),
        ('v', 0, 22),
        ('w', 0, 23),
        ('x', 0, 24),
        ('y', 0, 25),
        ('z', 0, 26),
        ('control', 0, 27),
        ('trigger', 0, 28),
        ('process', 0, 29),;

delete from etl_para;
INSERT INTO etl_para (etl_sys_id, para_cd, para_val, para_type, para_desc) VALUES (1000000000000000000, '#txdate', '#txdate', 'param', '当前跑批日,格式yyyyMMdd');
INSERT INTO etl_para (etl_sys_id, para_cd, para_val, para_type, para_desc) VALUES (1000000000000000000, '#txdate_next', '#txdate_next', 'param', '后一跑批日,格式yyyyMMdd');
INSERT INTO etl_para (etl_sys_id, para_cd, para_val, para_type, para_desc) VALUES (1000000000000000000, '#txdate_pre', '#txdate_pre', 'param', '前一跑批日,格式yyyyMMdd');


delete from role_menu;
insert into role_menu (role_id, menu_id, remark)
values  (1000, 10300, null),
        (1000, 10100, null),
        (1000, 10200, null),
        (1000, 10101, null),
        (1000, 10500, null),
        (1000, 10102, null),
        (1000, 10600, null),
        (1000, 10700, null),
        (1000, 10701, null),
        (1000, 10702, null),
        (1000, 10400, null),
        (1001, 20100, null),
        (1001, 20200, null),
        (1001, 20300, null),
        (1001, 20400, null),
        (1001, 20101, null),
        (1001, 20102, null),
        (1001, 20201, null),
        (1001, 20401, null),
        (2001, 30100, null),
        (2001, 30101, null),
        (2001, 30102, null),
        (2001, 30103, null),
        (2001, 30200, null),
        (2001, 30201, null),
        (2001, 30202, null),
        (2001, 30300, null),
        (2001, 30301, null),
        (2001, 30302, null),
        (2001, 30400, null),
        (2001, 30500, null),
        (2001, 30501, null),
        (2001, 30502, null),
        (2001, 30600, null),
        (2001, 30700, null),
        (2001, 30701, null),
        (2001, 30702, null),
        (2001, 30800, null),
        (2001, 30801, null),
        (2001, 30802, null),
        (2001, 30804, null),
        (2001, 31700, null),
        (2001, 31701, null),
        (2001, 31702, null);

delete from component_menu;
insert into component_menu (menu_id, menu_name, menu_desc, menu_level, parent_id, menu_path, menu_type, comp_id, menu_remark)
values  (10100, '用户管理', '/A/userManagement', '1', 0, '/A/userManagement', '00', 'A000', 'el-icon-user-solid'),
        (10101, '系统用户', '/A/sysUser', '2', 10100, '/A/sysUser', '00', 'A006', 'el-icon-user-solid'),
        (10102, '系统角色', '/A/sysRole', '2', 10100, '/A/sysRole', '00', 'A007', 'el-icon-user-solid'),
        (10200, '部门管理', '/A/departmentalList', '1', 0, '/A/departmentalList', '00', 'A000', 'el-icon-s-data'),
        (10300, '系统参数', '/A/systemParameters', '1', 0, '/A/systemParameters', '00', 'A000', 'el-icon-s-cooperation'),
        (10400, '数据存储层', '/S/dataStoreLayer', '1', 0, '/S/dataStoreLayer', '00', 'S000', 'el-icon-search'),
        (10500, '码值维护', '/A/codeMaintenance', '1', 0, '/A/codeMaintenance', '00', 'A000', 'el-icon-folder-checked'),
        (10600, '日志审查', '/A/logReview', '1', 0, '/A/logReview', '00', 'A000', 'el-icon-document'),
        (10700, '数据整理', '/A/dataCollation', '1', 0, '/dataCollation', '00', 'A000', 'el-icon-data-line'),
        (10701, 'OCR跑批', '/A/ocrRunBatch', '2', 10700, '/A/ocrRunBatch', '00', 'A000', 'el-icon-video-play'),
        (10702, 'SOLR数据关联', '/A/solrDataAssociation', '2', 10700, '/A/solrDataAssociation', '00', 'A000', 'el-icon-folder-add'),
        (20100, '采集管理', '/B/collectionM', '1', 0, '/B/collectionM', '01', 'B000', 'el-icon-s-data'),
        (20101, '数据采集', '/B/dataCollectionM', '2', 20100, '/B/dataCollectionM', '01', 'B000', 'el-icon-s-cooperation'),
        (20102, '主题管理', '/B/topicalM', '2', 20100, '/B/topicalM', '01', 'B000', 'el-icon-monitor'),
        (20200, '分发管理', '/B/distributionM', '1', 0, '/B/distributionM', '01', 'B000', 'el-icon-c-scale-to-original'),
        (20201, '消费审核', '/B/consumptionAudit', '2', 20200, '/B/consumptionAudit', '01', 'B000', 'el-icon-user-solid'),
        (20300, '接口管理', '/G/interfaceManagement', '1', 0, '/G/interfaceManagement', '01', 'B000', 'el-icon-chat-line-round'),
        (20400, '补录管理', '/R/supplementaryM', '1', 0, '/R/supplementaryM', '01', 'R000', 'el-icon-reading'),
        (20401, '录入审批', '/R/dataSupplementApprove', '2', 20400, '/R/dataSupplementApprove', '01', 'R002', 'el-icon-search'),
        (30100, '采集操作', '/B/collectO', '1', 0, '/B/collectO', '02', 'B001', 'el-icon-user-solid'),
        (30101, '数据采集', '/B/dataCollectionO', '2', 30100, '/B/dataCollectionO', '02', 'B001', 'el-icon-chat-line-round'),
        (30102, '贴源登记', '/F/dataRegister', '2', 30100, '/F/dataRegister', '02', 'T000', 'el-icon-reading'),
        (30103, '数据接收', '/B/dataReception', '2', 30100, '/B/dataReception', '02', 'B001', 'el-icon-user-solid'),
        (30200, '资源管理', '/B/resourceM', '1', 0, '/B/resourceM', '02', 'B002', 'el-icon-files'),
        (30201, '文件管理', '/B/fileM', '2', 30200, '/B/fileM', '02', 'B002', 'el-icon-search'),
        (30202, '全文检索', '/B/fullTextSearch', '2', 30200, '/B/fullTextSearch', '02', 'B002', 'el-icon-user-solid'),
        (30300, '数据浏览', '/B/dataBrowsing', '1', 0, '/B/dataBrowsing', '02', 'B003', 'el-icon-share'),
        (30301, 'SQL控制台', '/Q/sqlConsole', '2', 30300, '/Q/sqlConsole', '02', 'B003', 'el-icon-search'),
        (30302, '数据血缘', '/Q/dataLineage', '2', 30300, '/Q/dataLineage', '02', 'B003', 'el-icon-receiving'),
        (30400, '作业调度', '/C/etlMage', '1', 0, '/C/etlMage', '02', 'C001', 'el-icon-menu'),
        (30500, '数据加工', '/H/dataProcessing', '1', 0, '/H/dataProcessing', '02', 'H000', 'el-icon-connection'),
        (30501, '加工配置', '/H/market', '2', 30500, '/H/market', '02', 'H001', 'el-icon-s-finance'),
        (30502, '版本管理', '/H/marketVersionManage', '2', 30500, '/H/marketVersionManage', '02', 'H002', 'el-icon-monitor'),
        (30600, '接口服务', '/G/interfaceService', '1', 0, '/G/interfaceService', '02', 'G000', 'el-icon-cpu'),
        (30700, '数据分发', '/B/distributionO', '1', 0, '/B/distributionO', '02', 'B004', 'el-icon-s-unfold'),
        (30701, '批数据分发', '/B/dataDistribution', '2', 30700, '/B/dataDistribution', '02', 'B004', 'el-icon-share'),
        (30702, '流数据分发', '/B/steamDataDistribution', '2', 30700, '/B/steamDataDistribution', '02', 'B004', 'el-icon-s-unfold'),
        (30800, '补录操作', '/R/supplementaryO', '1', 0, '/R/supplementaryM', '02', 'R000', 'el-icon-reading'),
        (30801, '数据录入', '/R/dataSupplementEntry', '2', 30800, '/R/dataSupplementEntry', '02', 'R002', 'el-icon-film'),
        (30802, '数据同步', '/R/dataSupplementSynchronization', '2', 30800, '/R/dataSupplementSynchronization', '02', 'R002', 'el-icon-s-platform'),
        (30804, '录入统计', '/R/dataSupplementStatistics', '2', 30800, '/R/dataSupplementStatistics', '02', 'R002', 'el-icon-receiving'),
        (30900, '数据安全', '/E/dataSecurityO', '1', 0, '/E/dataSecurityO', '02', 'E002', 'el-icon-s-unfold'),
        (30901, '脱敏引擎', '/E/desensitizationEngine', '2', 30900, '/E/desensitizationEngine', '02', 'E002', 'el-icon-reading'),
        (30902, '加密引擎', '/E/EncryptionEngine', '2', 30900, '/E/EncryptionEngine', '02', 'E002', 'el-icon-search'),
        (31000, '脚本资产', '/E/scriptAssetsO', '1', 0, '/E/scriptAssetsO', '02', 'E002', 'el-icon-s-cooperation'),
        (31001, '脚本管理', '/E/scriptManagement', '2', 31000, '/E/scriptManagement', '02', 'E002', 'el-icon-s-unfold'),
        (31100, '元数据管理', '/M', '1', 0, '/M', '02', 'M000', 'el-icon-menu'),
        (31101, '元数据采集', '/M/metaTask', '2', 31100, '/M/metaTask', '02', 'M001', 'el-icon-connection'),
        (31102, '元数据维护', '/M/metaObjInfo', '2', 31100, '/M/metaObjInfo', '02', 'M002', 'el-icon-s-finance'),
        (31200, '数据标准', '/K/K000', '1', 0, '/K/K000', '02', 'K000', 'el-icon-search'),
        (31201, '标准元管理', '/K/standardSource', '2', 31200, '/K/standardSource', '02', 'K001', 'el-icon-s-cooperation'),
        (31202, '标准元信息', '/K/standardCheck', '2', 31200, '/K/standardCheck', '02', 'K002', 'el-icon-s-unfold'),
        (31203, '标准代码管理', '/K/codeManage', '2', 31200, '/K/codeManage', '02', 'K003', 'el-icon-share'),
        (31204, '元数据落标', '/K/dataMark', '2', 31200, '/K/dataMark', '02', 'K004', 'el-icon-s-unfold'),
        (31205, '数据元落标检测', '/K/detection', '2', 31200, '/K/detection', '02', 'K005', 'el-icon-search'),
        (31206, '数据元落标分析', '/K/history', '2', 31200, '/K/history', '02', 'K006', 'el-icon-user-solid'),
        (31300, '数据质量', '/K/K100', '1', 0, '/K/K100', '02', 'K100', 'el-icon-share'),
        (31301, '规则结果', '/K/ruleResults', '2', 31300, '/K/ruleResults', '02', 'K101', 'el-icon-search'),
        (31302, '规则配置', '/K/ruleConfig', '2', 31300, '/K/ruleConfig', '02', 'K102', 'el-icon-receiving'),
        (31400, '任务管理', '/T', '1', 0, '/T', '02', 'T000', 'el-icon-monitor'),
        (31401, '业务需求管理', '/T/bizReq', '2', 31400, '/T/bizReq', '02', 'T001', 'el-icon-cpu'),
        (31402, '数据需求管理', '/T/dataReq', '2', 31400, '/T/dataReq', '02', 'T002', 'el-icon-s-unfold'),
        (31403, '任务开发', '/T/taskDev', '2', 31400, '/T/taskDev', '02', 'T003', 'el-icon-share'),
        (31404, '开发测试', '/T/taskTest', '2', 31400, '/T/taskTest', '02', 'T004', 'el-icon-s-unfold'),
        (31405, '要点管理', '/T/essentialsManag', '2', 31400, '/T/essentialsManag', '02', 'T005', 'el-icon-reading'),
        (31500, '资产管理', '/N', '1', 0, '/N', '02', 'N1000', 'el-icon-s-unfold'),
        (31501, '资产目录', '/N/N1001', '2', 31500, '/N/N1001', '02', 'N1001', 'el-icon-share'),
        (31502, '资产盘点', '/N/N1002', '2', 31500, '/N/N1002', '02', 'N1002', 'el-icon-s-unfold'),
        (31503, '资产检索', '/N/N1003', '2', 31500, '/N/N1003', '02', 'N1003', 'el-icon-reading'),
        (31504, '资产目录查询', '/N/N1004', '2', 31500, '/N/N1004', '02', 'N1004', 'el-icon-film'),
        (31600, '自主取数', '/J', '1', 0, '/J', '02', 'J000', 'el-icon-film'),
        (31601, '群组管理', '/J/J2006', '2', 31600, '/J/J2006', '02', 'J006', 'el-icon-s-platform'),
        (31602, '报表配置', '/J/J2001', '2', 31600, '/J/J2001', '02', 'J001', 'el-icon-receiving'),
        (31603, '参数配置', '/J/J2004', '2', 31600, '/J/J2004', '02', 'J002', 'el-icon-s-unfold'),
        (31604, '报表权限配置', '/J/J2003', '2', 31600, '/J/J2003', '02', 'J003', 'el-icon-reading'),
        (31605, '报表查询', '/J/J2002', '2', 31600, '/J/J2002', '02', 'J004', 'el-icon-search'),
        (31606, '参数查询', '/J/J2005', '2', 31600, '/J/J2005', '02', 'J005', 'el-icon-s-cooperation'),
        (31700, '数据可视化', '/V/dataVisualizationO', '1', 0, '/V/dataVisualizationO', '02', 'V002', 'el-icon-reading'),
        (31701, '数据报表', '/V/dataVisualization', '2', 31700, '/V/dataVisualization', '02', 'V002', 'el-icon-search'),
        (31702, '数据仪表盘', '/V/dashboardList', '2', 31700, '/V/dashboardList', '02', 'V002', 'el-icon-s-cooperation'),
        (31800, '指标管理', '/I', '1', 0, '/I', '02', 'I000', 'el-icon-s-platform'),
        (31801, '指标首页', '/I/I102', '2', 31800, '/I/I102', '02', 'I102', 'el-icon-receiving'),
        (31802, '主题管理', '/I/I004', '2', 31800, '/I/I004', '02', 'I004', 'el-icon-s-unfold'),
        (31803, '维度管理', '/I/I003', '2', 31800, '/I/I003', '02', 'I003', 'el-icon-reading'),
        (31804, '标签管理', '/I/I005', '2', 31800, '/I/I005', '02', 'I005', 'el-icon-search'),
        (31805, '度量管理', '/I/I302', '2', 31800, '/I/I302', '02', 'I302', 'el-icon-s-cooperation'),
        (31806, '指标窄表宽表信息登记', '/I/I301', '2', 31800, '/I/I301', '02', 'I301', 'el-icon-s-unfold'),
        (31807, '指标定义', '/I/I001', '2', 31800, '/I/I001', '02', 'I001', 'el-icon-reading'),
        (31808, '指标维护', '/I/I002', '2', 31800, '/I/I002', '02', 'I002', 'el-icon-search'),
        (31809, '衍生指标', '/I/I202', '2', 31800, '/I/I202', '02', 'I202', 'el-icon-s-cooperation'),
        (31810, '组合指标', '/I/I203', '2', 31800, '/I/I203', '02', 'I203', 'el-icon-s-unfold'),
        (31811, '指标查询', '/I/I101', '2', 31800, '/I/I101', '02', 'I101', 'el-icon-s-unfold'),
        (31812, '指标搜索', '/I/I103', '2', 31800, '/I/I103', '02', 'I103', 'el-icon-receiving'),
        (31813, '指标查询-旧', '/I/I303', '2', 31800, '/I/I303', '02', 'I303', 'el-icon-s-unfold'),
        (31814, '规则配置', '/I/I204', '2', 31800, '/I/I204', '02', 'I204', 'el-icon-reading');

delete from department_info;
INSERT INTO department_info (dep_id,dep_name,create_date,create_time,dep_remark) VALUES ('1000000001','第一部门','20160101','120500','');

--系统默认用户信息
delete from sys_user;
insert into sys_user (user_id, create_id, dep_id, role_id, user_name, user_password, user_email, user_mobile, login_ip, login_date, user_state, create_date, create_time, update_date, update_time, user_remark, token, valid_time, is_login, limitmultilogin)
values  (1000, 1000, 1000000001, 1000, '超级管理员', '1', 'ccc@vv.com', '1234567890', null, null, '1', '99991231', '000000', '        ', '      ', '超级管理员', '0', '0', '0', '1'),
        (1001, 1000, 1000000001, 1001, '全功能管理员', '1', 'ccc@vv.com', '13524568511', '', '        ', '1', '99991231', '000000', '20230529', '112724', '系统管理员', '0', '0', '0', '1'),
        (2001, 1000, 1000000001, 2001, '全功能操作员', '1', 'ccc@vv.com', '1234567890', null, null, '1', '99991231', '000000', '        ', '      ', '系统操作员', '0', '0', '0', '1');

delete from sys_role;
insert into sys_role (role_id, role_name, is_admin, role_remark)
values  (1000, '超级管理员', '00', ''),
        (1001, '管理员', '01', ''),
        (2001, '操作员', '02', '');

delete from database_info;
insert into database_info (database_name, database_remark)
values  ('ORACLE10G', 'ORACLE10G'),
        ('KYLIN', 'KYLIN'),
        ('APACHEDERBY', 'APACHEDERBY'),
        ('POSTGRESQL', 'POSTGRESQL'),
        ('TERADATA', 'TERADATA'),
        ('ORACLE9I', 'ORACLE9I'),
        ('INFORMATIC', 'INFORMATIC'),
        ('SQLSERVER', 'SQLSERVER'),
        ('MYSQL', 'MYSQL'),
        ('SYBASEASE12.5', 'SYBASEASE12.5'),
        ('ODPS', 'ODPS'),
        ('DB2_V1', 'DB2_V1'),
        ('HIVE', 'HIVE'),
        ('KINGBASE', 'KINGBASE'),
        ('GBASE', 'GBASE'),
        ('H2', 'H2');

delete from database_type_mapping;
insert into database_type_mapping (dtm_id, database_name1, database_type1, database_name2, database_type2, dtm_remark, is_default)
values  (2000000009, 'POSTGRESQL', 'NUMERIC()', 'ORACLE10G', 'NUMBER()', null, '1'),
        (8000000008, 'DB2_V1', 'VARCHAR()', 'MYSQL', 'DATE', '', '0'),
        (5000000016, 'POSTGRESQL', 'BPCHAR()', 'MYSQL', 'CHAR()', '', '0'),
        (5000000017, 'POSTGRESQL', 'INT8', 'MYSQL', 'BIGINT', '', '0'),
        (5000000005, 'POSTGRESQL', 'VARBIT()', 'DB2_V1', 'VARCHAR() FOR BIT DATA', '用于32,672字节以下的字符串', '0'),
        (8000000002, 'DB2_V1', 'VARCHAR()', 'HIVE', 'STRING', null, '0'),
        (8000000003, 'DB2_V1', 'DATE', 'HIVE', 'TIMESTAMP', null, '0'),
        (8000000004, 'DB2_V1', 'TIME', 'HIVE', 'TIMESTAMP', null, '0'),
        (8000000005, 'DB2_V1', 'CLOB', 'HIVE', 'STRING', null, '0'),
        (8000000006, 'DB2_V1', 'BLOB', 'HIVE', 'STRING', null, '0'),
        (8000000001, 'DB2_V1', 'INTEGER', 'HIVE', 'INT', null, '0'),
        (2000000006, 'POSTGRESQL', 'BPCHAR()', 'ORACLE10G', 'CHAR()', null, '0'),
        (5000000001, 'POSTGRESQL', 'BIGSERIAL', 'DB2_V1', 'BIGINT', null, '0'),
        (5000000002, 'POSTGRESQL', 'SERIAL8', 'DB2_V1', 'BIGINT', null, '0'),
        (5000000003, 'POSTGRESQL', 'BIT', 'DB2_V1', 'CHAR() FOR BIT DATA', null, '0'),
        (2000000001, 'POSTGRESQL', 'SMALLINT', 'ORACLE10G', 'NUMERIC()', null, '0'),
        (2000000004, 'POSTGRESQL', 'VARCHAR()', 'ORACLE10G', 'VARCHAR2()', null, '0'),
        (2000000005, 'POSTGRESQL', 'FLOAT4', 'ORACLE10G', 'NUMERIC()', null, '0'),
        (5000000007, 'POSTGRESQL', 'CHARACTER()', 'DB2_V1', 'CHAR()', null, '0'),
        (5000000014, 'POSTGRESQL', 'BPCHAR()', 'DB2_V1', 'CHAR()', '如果精度小于 31，那么使用NUMERIC。', '0'),
        (5000000006, 'POSTGRESQL', 'BYTEA', 'DB2_V1', 'BLOB', '用于 32K 和 2GB字节之间的数据', '0'),
        (6000000002, 'ORACLE10G', 'NUMBER()', 'DB2_V1', 'SMALLINT', null, '0'),
        (9000000013, 'MYSQL', 'TINYTEXT', 'ORACLE10G', 'CLOB', '', '0'),
        (6000000003, 'ORACLE10G', 'NUMBER()', 'DB2_V1', 'INTEGER', null, '0'),
        (6000000004, 'ORACLE10G', 'NUMBER()', 'DB2_V1', 'BIGINT', null, '0'),
        (6000000005, 'ORACLE10G', 'NUMBER()', 'DB2_V1', 'DECIMAL()', '双精度', '0'),
        (6000000006, 'ORACLE10G', 'NUMBER()', 'DB2_V1', 'DOUBLE', null, '0'),
        (7000000012, 'ORACLE10G', 'VARCHAR2()', 'HIVE', 'VARCHAR()', null, '0'),
        (7000000013, 'ORACLE10G', 'NVARCHAR2()', 'HIVE', 'VARCHAR()', null, '0'),
        (7000000014, 'ORACLE10G', 'BLOB', 'HIVE', 'STRING', null, '0'),
        (7000000015, 'ORACLE10G', 'NCLOB', 'HIVE', 'STRING', null, '0'),
        (5000000015, 'POSTGRESQL', 'TEXT', 'DB2_V1', 'CLOB', '大于32K，那么使用BLOB', '0'),
        (2000000002, 'POSTGRESQL', 'TEXT', 'ORACLE10G', 'STRING', null, '0'),
        (4000000013, 'POSTGRESQL', 'BPCHAR()', 'HIVE', 'CHAR()', null, '0'),
        (7000000004, 'ORACLE10G', 'NUMBER()', 'HIVE', 'INT', '长度5-10建议使用', '0'),
        (7000000005, 'ORACLE10G', 'NUMBER()', 'HIVE', 'SMALLINT', '长度小于5建议使用', '0'),
        (7000000006, 'ORACLE10G', 'NUMBER()', 'HIVE', 'DECIMAL()', '双精度建议使用', '0'),
        (7000000002, 'ORACLE10G', 'NUMBER()', 'HIVE', 'DOUBLE', null, '0'),
        (7000000007, 'ORACLE10G', 'NUMBER()', 'HIVE', 'VARCHAR()', null, '0'),
        (7000000008, 'ORACLE10G', 'FLOAT', 'HIVE', 'DOUBLE', null, '0'),
        (7000000009, 'ORACLE10G', 'DATE', 'HIVE', 'TIMESTAMP', null, '0'),
        (7000000011, 'ORACLE10G', 'CHAR()', 'HIVE', 'VARCHAR()', null, '0'),
        (7000000001, 'ORACLE10G', 'INTEGER', 'HIVE', 'DOUBLE', null, '0'),
        (7000000003, 'ORACLE10G', 'NUMBER()', 'HIVE', 'BIGINT', '长度10-18建议使用', '0'),
        (6000000001, 'ORACLE10G', 'VARCHAR2()', 'DB2_V1', 'VARCHAR()', null, '0'),
        (4000000012, 'POSTGRESQL', 'BPCHAR()', 'HIVE', 'VARCHAR()', null, '0'),
        (1100000024, 'SQLSERVER', 'BIT', 'ORACLE10G', 'NUMBER()', '', '0'),
        (9000000001, 'MYSQL', 'VARCHAR()', 'ORACLE10G', 'VARCHAR2()', '', '0'),
        (9000000002, 'MYSQL', 'DECIMAL()', 'ORACLE10G', 'NUMBER()', '', '0'),
        (9000000003, 'MYSQL', 'INTEGER', 'ORACLE10G', 'NUMBER()', '', '0'),
        (9000000004, 'MYSQL', 'FLOAT()', 'ORACLE10G', 'NUMBER()', '', '0'),
        (9000000005, 'MYSQL', 'DOUBLE()', 'ORACLE10G', 'NUMBER()', '', '0'),
        (9000000006, 'MYSQL', 'TINYINT', 'ORACLE10G', 'NUMBER()', '', '0'),
        (9000000007, 'MYSQL', 'SMALLINT', 'ORACLE10G', 'NUMBER()', '', '0'),
        (9000000008, 'MYSQL', 'MEDIUMINT', 'ORACLE10G', 'NUMBER()', '', '0'),
        (9000000009, 'MYSQL', 'INT', 'ORACLE10G', 'NUMBER()', '', '0'),
        (9000000010, 'MYSQL', 'BIGINT', 'ORACLE10G', 'NUMBER()', '', '0'),
        (9000000011, 'MYSQL', 'TIME', 'ORACLE10G', 'DATE', '', '0'),
        (9000000012, 'MYSQL', 'DATETIME', 'ORACLE10G', 'DATE', '', '0'),
        (1100000022, 'SQLSERVER', 'UNIQUEIDENTIFIER', 'ORACLE10G', 'CHAR()', '', '0'),
        (1100000023, 'SQLSERVER', 'XML', 'ORACLE10G', 'NCLOB', '', '0'),
        (9000000017, 'MYSQL', 'MEDIUMBLOB', 'ORACLE10G', 'BLOB', '', '0'),
        (9000000014, 'MYSQL', 'TEXT', 'ORACLE10G', 'CLOB', '', '0'),
        (9000000015, 'MYSQL', 'LONGTEXT', 'ORACLE10G', 'CLOB', '', '0'),
        (9000000016, 'MYSQL', 'TINYBLOB', 'ORACLE10G', 'BLOB', '', '0'),
        (5000000009, 'POSTGRESQL', 'FLOAT4', 'DB2_V1', 'REAL', null, '0'),
        (7000000016, 'ORACLE10G', 'VARCHAR2()', 'HIVE', 'VARCHAR()', null, '0'),
        (8000000007, 'DB2_V1', 'DOUBLE()', 'HIVE', 'DOUBLE', null, '0'),
        (2000000007, 'POSTGRESQL', 'INT4', 'ORACLE10G', 'INT', null, '0'),
        (2000000008, 'POSTGRESQL', 'INT4', 'ORACLE10G', 'INT', null, '0'),
        (1100000001, 'SQLSERVER', 'BIGINT', 'ORACLE10G', 'NUMBER()', '', '0'),
        (1100000002, 'SQLSERVER', 'SMALLINT', 'ORACLE10G', 'NUMBER()', '', '0'),
        (1100000003, 'SQLSERVER', 'TINYINT', 'ORACLE10G', 'NUMBER()', '', '0'),
        (1100000004, 'SQLSERVER', 'FLOAT()', 'ORACLE10G', 'NUMBER()', '', '0'),
        (1100000005, 'SQLSERVER', 'DECIMAL()', 'ORACLE10G', 'NUMBER()', '', '0'),
        (1100000006, 'SQLSERVER', 'VARCHAR()', 'ORACLE10G', 'VARCHAR2()', '', '0'),
        (9000000018, 'MYSQL', 'LONGBLOB', 'ORACLE10G', 'BLOB', '', '0'),
        (1100000007, 'SQLSERVER', 'NCHAR()', 'ORACLE10G', 'CHAR()', '', '0'),
        (1100000019, 'SQLSERVER', 'BINARY()', 'ORACLE10G', 'BLOB', '', '0'),
        (1100000020, 'SQLSERVER', 'VARBINARY()', 'ORACLE10G', 'BLOB', '', '0'),
        (2000000003, 'POSTGRESQL', 'INT8', 'ORACLE10G', 'NUMBER()', null, '0'),
        (1100000008, 'SQLSERVER', 'NVARCHAR', 'ORACLE10G', 'VARCHAR2()', '', '0'),
        (1100000009, 'SQLSERVER', 'TIME', 'ORACLE10G', 'DATE', '', '0'),
        (4000000001, 'POSTGRESQL', 'DATE()', 'HIVE', 'STRING', null, '0'),
        (4000000002, 'POSTGRESQL', 'TIMESTAMP()', 'HIVE', 'STRING', null, '0'),
        (4000000003, 'POSTGRESQL', 'TEXT', 'HIVE', 'STRING', null, '0'),
        (1100000010, 'SQLSERVER', 'DATETIME', 'ORACLE10G', 'DATE', '', '0'),
        (1100000011, 'SQLSERVER', 'DATETIME2', 'ORACLE10G', 'DATE', '', '0'),
        (4000000004, 'POSTGRESQL', 'INT4', 'HIVE', 'INT', null, '0'),
        (4000000005, 'POSTGRESQL', 'CHARACTER()', 'HIVE', 'VARCHAR()', null, '0'),
        (4000000006, 'POSTGRESQL', 'INTEGER', 'HIVE', 'INT', null, '0'),
        (4000000007, 'POSTGRESQL', 'INT8', 'HIVE', 'BIGINT', null, '0'),
        (1100000012, 'SQLSERVER', 'SMALLDATETIME', 'ORACLE10G', 'DATE', '', '0'),
        (4000000008, 'POSTGRESQL', 'NUMERIC()', 'HIVE', 'DECIMAL()', null, '0'),
        (1100000013, 'SQLSERVER', 'DATETIMEOFFSET', 'ORACLE10G', 'DATE', '', '0'),
        (4000000009, 'POSTGRESQL', 'NCHAR()', 'HIVE', 'VARCHAR()', null, '0'),
        (4000000010, 'POSTGRESQL', 'REAL', 'HIVE', 'FLOAT', null, '0'),
        (4000000011, 'POSTGRESQL', 'CHAR()', 'HIVE', 'VARCHAR()', null, '0'),
        (5000000008, 'POSTGRESQL', 'NUMERIC()', 'DB2_V1', 'DECIMAL()', null, '0'),
        (1100000014, 'SQLSERVER', 'TEXT', 'ORACLE10G', 'CLOB', '', '0'),
        (1100000015, 'SQLSERVER', 'NTEXT', 'ORACLE10G', 'CLOB', '', '0'),
        (1100000016, 'SQLSERVER', 'IMAGE', 'ORACLE10G', 'BLOB', '', '0'),
        (5000000010, 'POSTGRESQL', 'FLOAT4', 'DB2_V1', 'NUMERIC', null, '0'),
        (1100000017, 'SQLSERVER', 'MONEY', 'ORACLE10G', 'NUMBER()', '', '0'),
        (1100000018, 'SQLSERVER', 'SMALLMONEY', 'ORACLE10G', 'NUMBER()', '', '0'),
        (5000000011, 'POSTGRESQL', 'FLOAT4', 'DB2_V1', 'FLOAT', null, '0'),
        (5000000012, 'POSTGRESQL', 'FLOAT8', 'DB2_V1', 'NUMERIC', '如果精度小于 31，那么使用NUMERIC。', '0'),
        (5000000013, 'POSTGRESQL', 'INT8', 'DB2_V1', 'BIGINT', null, '0'),
        (1100000021, 'SQLSERVER', 'TIMESTAMP', 'ORACLE10G', 'TIMESTAMP', '', '0');

delete from default_length_mapping;
insert into default_length_mapping (database_name, column_type, column_length, dlm_remark)
values  ('DB2_V1', 'DOUBLE', '53', null),
        ('DB2_V1', 'BIGINT', '19', null),
        ('DB2_V1', 'CLOB', '1048576', null),
        ('DB2_V1', 'DATE', '10', null),
        ('DB2_V1', 'INTEGER', '10', null),
        ('DB2_V1', 'INT', '10', null),
        ('HIVE', 'INT', '32', null),
        ('HIVE', 'FLOAT', '24', null),
        ('HIVE', 'DOUBLE', '53', null),
        ('HIVE', 'STRING', '53', null),
        ('HIVE', 'BOOLEAN', '10', null),
        ('HIVE', 'BIGINT', '19', null),
        ('ORACLE10G', 'INT', '38', null),
        ('ORACLE10G', 'FLOAT', '126', null),
        ('ORACLE10G', 'BINARY_DOUBLE', '8', null),
        ('ORACLE10G', 'BINARY_FLOAT', '4', null),
        ('ORACLE10G', 'DATE', '7', null),
        ('ORACLE10G', 'CLOB', '4000', null),
        ('ORACLE10G', 'BLOB', '4000', null),
        ('POSTGRESQL', 'FLOAT4', '24', null),
        ('POSTGRESQL', 'FLOAT8', '53', null),
        ('POSTGRESQL', 'JSON', '2147483647', null),
        ('POSTGRESQL', 'INT4', '32', null),
        ('POSTGRESQL', 'INT8', '64', null),
        ('POSTGRESQL', 'DATE', '13', null),
        ('POSTGRESQL', 'BIGINT', '64', null),
        ('POSTGRESQL', 'INTEGER', '32', null),
        ('MYSQL', 'INT', '11', ''),
        ('MYSQL', 'INTEGER', '11', ''),
        ('MYSQL', 'BIGINT', '20', ''),
        ('MYSQL', 'SMALLINT', '6', ''),
        ('MYSQL', 'MEDIUMINT', '8', ''),
        ('MYSQL', 'TINYINT', '3', ''),
        ('SQLSERVER', 'UNIQUEIDENTIFIER', '38', ''),
        ('SQLSERVER', 'BIGINT', '20', ''),
        ('SQLSERVER', 'SMALLINT', '5', ''),
        ('SQLSERVER', 'TINYINT', '3', ''),
        ('SQLSERVER', 'INT', '10', ''),
        ('SQLSERVER', 'MONEY', '(20,3)', ''),
        ('SQLSERVER', 'SMALLMONEY', '(10,3)', ''),
        ('SQLSERVER', 'BIT', '1', ''),
        ('MYSQL', 'DATE', '32', '');

delete from interface_info;
INSERT INTO interface_info(interface_id, url, interface_name, interface_type, interface_state, interface_code, interface_note, user_id) VALUES (100, 'tableUsePermissions', '表使用权限查询接口', '1', '1', '01-100', null, 1001);
INSERT INTO interface_info(interface_id, url, interface_name, interface_type, interface_state, interface_code, interface_note, user_id) VALUES (101, 'generalQuery','单表普通查询接口', '1', '1', '01-101',NULL,  1001);
INSERT INTO interface_info(interface_id, url, interface_name, interface_type, interface_state, interface_code, interface_note, user_id) VALUES (102, 'tableStructureQuery', '表结构查询接口', '1', '1', '01-102', NULL, '1001');
INSERT INTO interface_info(interface_id, url, interface_name, interface_type, interface_state, interface_code, interface_note, user_id) VALUES (103, 'tableSearchGetJson', '表结构查询-获取json信息接口', '1', '1', '01-103', NULL, '1001');
INSERT INTO interface_info(interface_id, url, interface_name, interface_type, interface_state, interface_code, interface_note, user_id) VALUES (104, 'fileAttributeSearch', '文件属性搜索接口', '1', '1', '01-104', NULL, '1001');
INSERT INTO interface_info(interface_id, url, interface_name, interface_type, interface_state, interface_code, interface_note, user_id) VALUES (105, 'sqlInterfaceSearch', 'sql查询接口', '1', '1', '01-105', NULL, '1001');
INSERT INTO interface_info(interface_id, url, interface_name, interface_type, interface_state, interface_code, interface_note, user_id) VALUES (106, 'rowKeySearch', 'rowkey查询', '1', '1', '01-106', null, 1001);
INSERT INTO interface_info(interface_id, url, interface_name, interface_type, interface_state, interface_code, interface_note, user_id) VALUES (107, 'fullTextSearchApi', '全文检索', '1', '1', '01-107', null, 1001);
INSERT INTO interface_info(interface_id, url, interface_name, interface_type, interface_state, interface_code, interface_note, user_id) VALUES (108, 'unstructuredFileDownloadApi', '非结构化文件下载', '1', '1', '01-108', null, 1001);
INSERT INTO interface_info(interface_id, url, interface_name, interface_type, interface_state, interface_code, interface_note, user_id) VALUES (109, 'solrSearch', 'solr查询', '1', '1', '01-109', null, 1001);
INSERT INTO interface_info(interface_id, url, interface_name, interface_type, interface_state, interface_code, interface_note, user_id) VALUES (200, 'uuidDownload', 'UUID数据下载', '2', '1', '02-200', null, 1001);
INSERT INTO interface_info(interface_id, url, interface_name, interface_type, interface_state, interface_code, interface_note, user_id) VALUES (201, 'hbaseSolrQuery', 'Solr查询Hbase数据接口', '2', '1', '02-201', null, 1001);
INSERT INTO interface_info(interface_id, url, interface_name, interface_type, interface_state, interface_code, interface_note, user_id) VALUES (202, 'sqlExecute', 'SQL执行接口', '2', '1', '02-202', null, 1001);
INSERT INTO interface_info(interface_id, url, interface_name, interface_type, interface_state, interface_code, interface_note, user_id) VALUES (203, 'sqlQueryRelation', 'SQL血缘查询', '2', '1', '02-203', null, 1001);
INSERT INTO interface_info(interface_id, url, interface_name, interface_type, interface_state, interface_code, interface_note, user_id) VALUES (124, 'computerResourceInfo', '机器的资源使用信息', '4', '1', '01-144', NULL, 1001);

-- 数据管控,规则校验定义
-- delete from dq_rule_def;
-- insert into dq_rule_def (case_type, case_type_desc, index_desc1, index_desc2, index_desc3, remark)
-- values  ('COL ENUM', '字段枚举检测', '不在范围内的记录数', '检查总记录数', '', '检测目标表名的 目标表关键字段是否在清单值域 内，格式转义需用户直接转义'),
--       ('COL FK', '字段外键检测', '外键不存在的记录数', '检查总记录数', '', '检测目标表名的 目标表关键字段(，分割多个字段)是否在 比对表表名 的 比对表关键字段 存在 '),
--      ('COL NAN', '字段非空', '空的记录数', '检查总记录数', '', '检测目标表名的 目标表关键字段是否非空'),
--      ('COL PK', '字段主键检测', '主键重复的记录数', '检查总记录数', '', '检测目标表名的 目标表关键字段(，分割多个字段)是否为主键唯一 '),
--      ('COL RANG', '字段范围检测', '不在范围内的记录数', '检查总记录数', '', '检测目标表名的 目标表关键字段是否在【范围区间的最小值，范围区间的最大值】内，格式转义需用户直接转义'),
--      ('COL REGULAR', '字段正则表达式', '不在范围内的记录数', '检查总记录数', '', '检测目标表名的 目标表关键字段是否符合在清单值域 内字段正则表达式'),
--      ('SQL', '指定sql', '自定义', '自定义', '', '检测指定SQL 的sql规则'),
--      ('TAB NAN', '表非空', '', '', '', '检测目标表名是否非空'),
--      ('TOTAL SCORE', '总分校验检查', '不相等的记录数', '检查总记录数', '', '检测目标表名的 目标表关键字段的值，与对标表字段sum后的值进行对比');

-- 数据质量
-- delete from dq_help_info;
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b1', '业务指标代码', '新增业务指标时，用户可自定义，如acc_nav。新增完成后不可修改。');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b10', '业务口径描述', '从业务层面来描述该业务指标');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b11', '技术口径描述', '从技术层面来描述该业务指标');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b12', '指标单位代码', '此为回显状态');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b13', '指标单位名', '用户可自定义指标所用单位');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b14', '参数代码', '用户可从下拉框中选择对应的参数代码');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b15', '参数名', '再选择参数代码后，会默认得到一个参数名。用户也可自定义');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b16', '参数描述', '用户可自定义');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b17', '顺序号', '用户可自定义');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b18', '区分技术指标', '当指标参数下所有参数都为否，则业务指标和技术指标的关联类型为一对一关系。当指标参数下有一个参数为是（最多一个参数为是），则业务指标和技术指标的关联类型为一对多关系。');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b19', '输入类型', '用户可从下拉框中选择对应的输入类型');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b2', '指标名称', '用户可自定义，如累计单位净值');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b20', '输入检验正则式', '用户可自定义正则表达式，右侧的按钮可以检查输入值是否符合正则表达式。');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b21', '可选项提供方式', '用户可从下拉框中选择对应的可选项提供方式');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b22', '对应维度类型代码', '用户可从下拉框中选择对应维度类型代码');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b23', '维度可选层级', '用户可自定义');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b24', '技术指标代码', '用户可选择对应的技术指标来关联');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b25', '关联类型', '该输入框为回显状态，回显的值根据指标参数列表中是否区分技术指标来决定如果区分技术指标都为否，则该输入框回显一对一如果区分技术指标有一个值为是，则该输入框回显参数');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b26', '关联参数代码', '该输入框为回显状态，回显的值根据是否指标参数列表中是否区分技术指标来决定如果区分技术指标都为否，则该输入框的回显值为空如果区分技术指标有一个值为是，则该输入框调用该行的参数代码');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b27', '关联参数值', '根据关联类型的值而改变,如果关联类型为参数时，则该输入框可自定义,如果关联类型为一对一时，则该输入框为回显状态，且回显值为空');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b28', '上级指标代码', '该指标代码所对应上级指标代码');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b29', '指标级别', '该指标代码所对应指标级别');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b3', '英文全称', '用户可自定义，该指标代码的英文全称');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b4', '英文简称', '用户可自定义，该指标代码的英文简称');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b5', '中文拼音首字母', '用户可自定义，该指标名称的中文拼音首字母');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b6', '函数格式', '用户可以按格式fgf_XXX来自定义,如fgf_acc_nav');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b7', '保存精度', '原来记录小数位后位数');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b8', '业务指标的指标主体', '此为回显状态，根据其关联的技术指标的指标主体来回显，可以有多个指标主体');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('b9', '指标单位', '用来记录指标所用单位');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('Catalg', 'DQC服务器上作业程序目录', '请填写DQC服务器上作业程序所在的路径。格式如/etl/DQC/BIN。');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('cfgInfo', '系统配置的相关信息', '用户可自定义，填写该系统配置的相关信息。');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('crrsDmns', '对应维度类型代码', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('d1', '维度类型代码', '新增维度时，用户可自定义，如d_assc_prmt。新增完成后不可修改。');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('d10', '检验正则式', '检验正则式');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('d11', '排序字段', '用户可输入字段名，会按照该字段名排序');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('d12', '维度备注', '用户可自定义，对维度添加一些备注信息');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('d13', '存储schema', '此处为回显状态');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('d14', '存储表', '用户可自定义。建议填写数据库中存储schema下的表名，否则校验会失败');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('d2', '维度类型描述', '用户可自定义，如关联参数');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('d3', '数据格式', '可选择日期、数字、整数、文本');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('d4', '历史记录方法', '可选择无历史、时间戳、拉链。无历史：代表无历史记录方法时间戳：代表历史中某一个时间点,拉链：代表历史中某一段时间，即有开始时间和结束时间。此选项会影响存储表信息校验的字段');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('d5', '是否为技术维度', '选择该维度是否为技术维度');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('d6', '输入类型', '可选择日期、数字、下拉、文本');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('d7', '可选项提供方式', '只有当输入类型选择下拉时，才可选择。可选择枚举、服务。枚举：代表一次性给予相关数据。服务：代表多次给予相关数据。');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('d8', '是否缓存', '只有当输入类型选择下拉时，才可选择。');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('d9', '默认值', '该下拉框中的值取自于页面最下面列表中的维度值详细，如果列表中没有维度值，则下拉框显示未找到结果');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('dAls', '维度别名', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('dbNm', '数据库名称', '请填写数据库中对应的名称');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('dbPort', '数据库的端口号', '请填写数据库的端口号，如3306');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('dbType', '数据库类型', '支持MYSQL,ORACLE,VER');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('dDf', '默认值', '内容3');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('dDsc', '维度类型描述', '内容2');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('dispTime', '设定调度触发时间', '当调度触发方式为定时触发时，可用。 格式为HH:MM:SS，例如19:20:30');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('dispType', '选择调度触发方式', '目前支持2种调度触发方式：依赖触发，定时触发,依赖触发：在上游作业完成之后立刻触发.定时触发：在上游作业完成之后，等到了“调度触发时间”所设定的时间时，触发');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('dmnsRmrk', '维度备注', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('dReg', '输入检验正则式', '内容4');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('dSca', '存储schema', '内容5');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('dTab', '存储表', 'dfgdfgdfgdfgdfhdh');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('dTyp', '维度类型代码', '内容1');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('errSql', '查询问题明细的sql语句', '检测sql不通过时最关心的数据明细，如:SELECT  DISTINCT WIND_CODE,INDICATOR_NAMEFROM  SFULL.S34_FUND_MOMENT_INDICATOR');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('etlCd', '调度系统', '请填写相应的调度系统，如EDW');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('etlIp', 'DQC服务器数据库的IP地址', '请填写DQC服务器数据库的IP地址。格式如192.168.30.9');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('etlJob', '调度作业名', '用户可自定义，如EDW_BKUP_SCEMA_APDSP');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('etlpass', 'DQC服务器数据库的用户名和密码', '请填写DQC服务器数据库的用户名和密码');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('etlPort', 'DQC服务器数据库的端口号', '请填写DQC服务器数据库的端口号，如22');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('etlUser', 'DQC服务器数据库的用户名和密码', '请填写DQC服务器数据库的用户名和密码');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('fields', '需要检测的关键字段', '用户可以填写一个或者多个关键字段，多个关键字段请用，隔开。格式如Pty_Id,Pty_Typ_Cd,ETL_Src_Tab');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('firstCN', '中文拼音首字母', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('flag', '定义规则级别', '分为警告和严重级别。如果规则为警告级别，处理当前作业时，只会返回脚本运行结果，如果运行结果是成功，则继续执行后续依赖作业。如果规则为严重级别，处理当前作业时，检查结果有异常，无论运行结果是成功还是失败，都会停止后续依赖作业。');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('index1', '描述检测指标1的含义', '只有当规则类型选择指定sql/指定sql文件/字段正则表达式时可用，用户可以填写相关内容来描述sql语句中第二个返回值的含义');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('index2', '描述检测指标2的含义', '只有当规则类型选择指定sql/指定sql文件/字段正则表达式时可用，用户可以填写相关内容来描述sql语句中第三个返回值的含义');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('index3', '描述检测指标3的含义', '只有当规则类型选择指定sql/指定sql文件/字段正则表达式时可用，用户可以填写相关内容来描述sql语句中第四个返回值的含义');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('ip', '数据库的IP地址', '请填写数据库的IP地址，格式如192.168.40.114');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('jobType', '作业程序类型', '支持2种作业程序类型。PERL：常见的PERL类型WF： WF作业类型，判断文件是否存在');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('limit', '检查表名时限定条件', '检查表名时，用户可以填写一些限定条件。格式如ETL_Upd_Date=''#{TX_DATE}''');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('listVals', '设定一个清单作为值域范围', '必填项。当规则类型选择字段枚举检测或字段正则表达式时可用。用户可以填写文本，日期或者数字作为值域范围；文本需要加单引号，如''A08'',''A02'',''A11'',''A10''日期也需要加单引号，如''2017-07-28'',''2017-07-08''数字不需要加单引号，如1,23,567');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('log', 'DQC服务器上作业日志目录', '请填写DQC服务器上作业日志所在的路径。格式如/etl/DQC/ERR_DATA');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('logDic', '作业日志目录', '请填写对应作业日志所在的路径，并且其子文件夹按日期排序，日期必须使用变量名，其余可使用变量名也可使用具体数值。格式如!{ETLLOG}@#{txdate}@!{XX}或者/etl/ETL/LOG/@#{txdate}@/');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('max', '设定范围区间的最大值', '只有当规则类型选择字段范围检测/表数据量增量时可用，用户可以填写数值来设定范围区间最大值，如30001231');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('min', '设定范围区间的最小值', '只有当规则类型选择字段范围检测/表数据量增量时可用，用户可以填写数值来设定范围区间最小值，如20100101');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('mnBdyCd', '指标主体代码', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('mnBdyDsc', '指标主体名称', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('offset', '根据调度频率填写具体的时间', '必填项。此输入框与调度频率有关。填写规则如下：1）默认值0；2）天时，忽略此字段值；3）月时，0-表示月初，正N-表示本月第N天，负N-表示本月倒数第N天；4）周、年时与月相同；例如，调度频率选择月时，调度时间位移填写5，则表示每月第5天进行调度');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('opField', '和当前检测的关键字段做比对', '用户可以填写一个或者多个关键字段，多个关键字段请用，隔开。格式如Pty_Id,Pty_Typ_Cd,ETL_Src_Tab');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('opTab', '和当前检测的表做比对', '只有当规则类型选择字段外键检测时可用，用户可以变量名加表名的形式填写。格式如#{PDATA}.T02_PARTY');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('optnLvl', '维度可选层级', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('ordrClmn', '排序字段', '按SQL Order By 语法格式存放');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('password', '数据库的用户名和密码', '请填写数据库的用户名和密码');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('phyFld', '物理表字段名', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('prity', '作业的优先级设定', '数字越大，优先级越高。');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('prmtCd', '关联参数代码', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('prmtDsc', '参数描述', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('prmtNm', '参数名称', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('proDic', '作业程序目录', '请填写对应作业程序所在的路径，可以填写变量名，也可以使用具体路径。格式如!{ETLBIN}或者/etl/ETL/BIN/');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('proName', '作业程序名', '请填写对应作业程序名称。格式如delhisdata.pl');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('proPara', '作业程序参数', '请填写对应作业程序参数，参数之间用@符号分隔。例如：aaa@1@2');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('regName', '定义规则名称', '用户可自定义，例如RN_COL_FK_001的主键规则');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('ruleSrc', '规则来源', '用户自定义，填写内容为：哪个人或者哪个部门提出了这条规则');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('ruleTag', '将规则分类并定义相关的标签', '用户可自定义，例如主键规则');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('ruleType', '选择规则类型', '支持10种规则类型。1. COL ENUM：字段枚举检测      字段级-枚举值校验。字段值在枚举列表中返回0，否则返回非0。2. COL FK：字段外键检测      字段级-外键校验。字段外键关系全部成立，则返回0，否则返回非0。3. COL NAN：字段非空      字段级-非空字段校验。字段中没有空值，则返回0，否则返回非0。4. COL PK：字段主键检测      字段级-主键校验。字段不存在重复，则返回0，否则返回非0。5. COL RANG：字段范围检测     字段级-范围校验。字段在指定范围，返回0，否则返回非0。6. COL REGULAR：字段正则表达式      字段级-正则表达式校验。字段内容符合正则表达式返回0，否则返回非0。7. SQL：指定sql      使用指定SQL，即specify_sql字段。用于比较简单的规则。8. SQL FILE：指定sql文件      使用指定SQL，即specify_sql字段，比较复杂的sql，填写sql文件路径。9. TAB NAN：表非空      表级-非空表校验。表为非空，则返回0，空返回其他值 10. TAB SIZE：表数据量增量     表级-表数据量校验。数据环比增量比例在范围区间内，则返回0，否则返回非0。');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s1', '数据集代码', '新增数据集时，用户可自定义，如fgf_hldn_pnsn_dtl。新增完成后不可修改。');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s10', '是否替换变量', '用户可从下拉框中选择');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s11', '保存精度', '用户可自定义，记录小数点后位数');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s12', '显示名称', '用户可自定义，为该字段的显示名称');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s13', '输入格式', '用户可从下拉框中选择Date, Number, Text');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s14', '显示格式', '用户可从下拉框中选择Date, Number, Text');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s15', '对应维度类型代码', '用户可从下拉框中选择对应维度类型代码');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s16', '维度可选层级', '用户可自定义');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s17', '代码转换', '用户可从下拉框中选择');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s18', '数据集代码', '此为回显状态');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s19', '上级指标代码', '该数据集代码所对应上级数据集代码');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s2', '数据集描述', '用户可自定义，如养老金持仓明细表');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s20', '指标级别', '该数据集代码所对应数据集层级');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s3', '过滤条件', 'sql 语法，不带where 关键字，会与其他过滤条件合并，例如：C1=''123''   AND (C2<=1  OR C3<>1)');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s4', '历史记录方法', 'Z拉链, 默认带查询条件  数据日期#{dt}           数据表必须有字段 ： strt_dt，end_dt           查询条件为：strt_dt<=#{dt} and end_dt>#{dt}N无历史,无特殊处理T时间戳 ,无特殊处理');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s5', '存储表', '用户可填写sql语句或者数据库对应表名');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s6', '字段名称', '用户需要填写存储表中填写sql语句或者数据库表所包含的字段');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s7', '顺序号', '用户可自定义');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s8', '查询字段', '选是，则会先在数据集操作界面中的查询字段列表下显示选否，则不显示');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('s9', '是否输出字段', '选是，则会先在数据集操作界面中的输出字段列表下显示选否，则不显示');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('scaduleF', '调度状态', '显示当前规则的调度状态');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('seqId', '顺序号', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('sql', '通过sql语句检测', '只有当规则类型选择指定sql可用，用户可以填写相关sql语句。sql语句要求： 1. 必须由2句sql构成,并以 ";" 分隔 2. 第一句sql(统计不在范围的记录数)的查询结果列必须包含COUNT(1) AS index1,第二句sql(检查记录总数)的查询结果列必须包含COUNT(1) AS index2 3. sql语句可以使用变量，变量需要事先在系统变量功能中进行配置。“#{TX_DATE10}”和“#{TX_DATE}”为系统保留的变量，分别表示10位和8位的批量日期，可直接引用。如：SELECT 1 , COUNT(1),count(1)+1，2 FROM  !{SFULL}.S34_FUND_MOMENT_INDICATOR WHERE TO_DATE(BATCH_DATE,''YYYYMMDD'')=DATE(''#{TX_DATE10}'') AND TO_DATE(BATCH_DATE,''YYYYMMDD'')=DATE(''#{TX_DATE}'')');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('subCd', '调度子系统的代码编号', '请填写对应的调度子系统的代码编号，如APDSP');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('sysId', '系统编号', '用户可自定义，例如MYS，ORA');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('sysNm', '系统名称', '用户可自定义，例如etl测试环境mysql库');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('sysNum', '给变量名赋值', '用户可自定义，例如FGKM注：不可填写特殊字符');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('sysVar', '定义变量名', '用户可自定义，不过需要按#{XXX}的形式命名，例如#{FGKM}注意：不可用#{TX_DATE10}和#{TX_DATE}');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t1', '技术指标代码', '新增指标时，用户可自定义。新增完成后，不可修改');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t10', '技术口径描述', '从技术层面来描述该业务指标');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t11', '技术含义SQL', '是否落地为否时填写填写指标生成的带参数的SQL代码,参数代码格式（#{参数代码}）');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t12', '备注', '用户可自定义');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t13', '存储schema', '此为回显状态');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t14', '存储表', '用户可自定义。建议填写数据库中存储schema下的表名，否则校验会失败');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t15', '指标存储字段', '用户可自定义。建议填写数据库中存储schema下表名中的字段名，否则校验会失败');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t16', '关联类型', '此为回显状态');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t17', '关联参数代码', '此为回显状态');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t18', '关联参数值', '此为回显状态');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t19', '指标主体代码', '此为回显状态');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t2', '指标名称', '用户可自定义，如持仓份额');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t20', '指标主体名称', '用户可自定义指标主体的名称');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t21', '维度别名', '用户可自定义');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t22', '物理表字段名', '用户可自定义');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t23', '对应参数代码', '用户可从下拉框中选择对应的参数代码');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t24', '对应维度类型代码', '用户可从下拉框中选择对应的维度类型代码');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t25', '维度可选层级', '用户可自定义');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t26', '上级指标代码', '该指标代码所对应上级指标代码');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t27', '指标级别', '该指标代码所对应指标级别');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t3', '业务归属部门', '用户可从下拉框中选择指标所属部门');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t4', '归属业务指标', '此为回显状态，显示该指标属于哪个业务指标。点击查看按钮，可查看业务指标的详细信息。该指标与归属业务指标关联关系可在业务指标关联的技术指标清单中编辑。');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t5', '指标来源', '用户可自定义，描述该指标来源于哪');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t6', '指标数据类型', '可选择日期，数字，整数，文本');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t7', '指标类型', '可选择基础和派生基础为直接从业务系统取数，派生为数据仓库加工');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t8', '是否落地', '选择是时，则校验存储表信息选择否时，则校验技术含义SQL');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('t9', '指标主体', '用户可选择对应的指标主体，技术指标只有一个指标主体。用户可新增和删除指标主体。');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('targTab', '需要检测的表名', '用户可以变量名加表名的形式填写，格式如!{PDATA}.T01_PARTY。注意：#{TX_DATE10}和#{TX_DATE}变量不可以被填写!');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('tCd', '技术指标代码', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('tDsc', '指标名称', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('tFld', '指标存储字段', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('time', '设定调度触发时间', '当调度触发方式为定时触发时，可用。格式为HH:MM:SS，例如19:20:30');
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('tLvl', '指标级别', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('tRmrk', '备注', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('tRngDc', '技术口径描述', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('tSQL', '技术含义SQL', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('tSrc', '指标来源', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('tUpCd', '上级指标代码', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('unitCd', '指标单位代码', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('unitDsc', '指标单位名称', null);
-- INSERT INTO dq_help_info (help_info_id, help_info_desc, help_info_dtl) VALUES ('usr', '数据库的用户名和密码', '请填写数据库的用户名和密码');
