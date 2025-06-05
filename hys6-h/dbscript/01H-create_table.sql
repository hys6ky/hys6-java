--集市分类信息
DROP TABLE IF EXISTS DM_CATEGORY ;
CREATE TABLE DM_CATEGORY(
CATEGORY_ID                                         BIGINT default 0 NOT NULL, --集市分类id
CATEGORY_NAME                                       VARCHAR(512) NOT NULL, --分类名称
CATEGORY_DESC                                       VARCHAR(200) NULL, --分类描述
CREATE_DATE                                         CHAR(8) NOT NULL, --创建日期
CREATE_TIME                                         CHAR(6) NOT NULL, --创建时间
CATEGORY_SEQ                                        VARCHAR(512) NULL, --分类序号
CATEGORY_NUM                                        VARCHAR(512) NOT NULL, --分类编号
CREATE_ID                                           BIGINT default 0 NOT NULL, --创建用户
PARENT_CATEGORY_ID                                  BIGINT default 0 NOT NULL, --集市分类id
DATA_MART_ID                                        BIGINT default 0 NOT NULL, --数据集市id
CONSTRAINT DM_CATEGORY_PK PRIMARY KEY (CATEGORY_ID)   );

--数据集市信息表
DROP TABLE IF EXISTS DM_INFO ;
CREATE TABLE DM_INFO(
DATA_MART_ID                                        BIGINT default 0 NOT NULL, --数据集市id
MART_NAME                                           VARCHAR(512) NOT NULL, --数据集市名称
MART_NUMBER                                         VARCHAR(512) NOT NULL, --数据库编号
MART_DESC                                           VARCHAR(512) NULL, --数据集市描述
MART_STORAGE_PATH                                   VARCHAR(512) NOT NULL, --数据集市存储路径
CREATE_DATE                                         CHAR(8) NOT NULL, --创建日期
CREATE_TIME                                         CHAR(6) NOT NULL, --创建时间
CREATE_ID                                           BIGINT default 0 NOT NULL, --用户ID
DM_REMARK                                           VARCHAR(512) NULL, --备注
CONSTRAINT DM_INFO_PK PRIMARY KEY (DATA_MART_ID)   );

--数据表信息
DROP TABLE IF EXISTS DM_DATATABLE ;
CREATE TABLE DM_DATATABLE(
DATATABLE_ID                                        BIGINT default 0 NOT NULL, --数据表id
DATA_MART_ID                                        BIGINT default 0 NULL, --数据集市id
DATATABLE_CN_NAME                                   VARCHAR(512) NOT NULL, --数据表中文名称
DATATABLE_EN_NAME                                   VARCHAR(512) NOT NULL, --数据表英文名称
DATATABLE_DESC                                      VARCHAR(512) NULL, --数据表描述
DATATABLE_CREATE_DATE                               CHAR(8) NOT NULL, --数据表创建日期
DATATABLE_CREATE_TIME                               CHAR(6) NOT NULL, --数据表创建时间
DATATABLE_DUE_DATE                                  CHAR(8) NOT NULL, --数据表到期日期
DDLC_DATE                                           CHAR(8) NOT NULL, --DDL最后变更日期
DDLC_TIME                                           CHAR(6) NOT NULL, --DDL最后变更时间
DATAC_DATE                                          CHAR(8) NOT NULL, --数据最后变更日期
DATAC_TIME                                          CHAR(6) NOT NULL, --数据最后变更时间
DATATABLE_LIFECYCLE                                 CHAR(1) NOT NULL, --数据表的生命周期
SORUCE_SIZE                                         DECIMAL(16,2) default 0 NOT NULL, --资源大小
ETL_DATE                                            CHAR(8) NOT NULL, --跑批日期
SQL_ENGINE                                          CHAR(1) NULL, --sql执行引擎
STORAGE_TYPE                                        CHAR(1) NOT NULL, --进数方式
TABLE_STORAGE                                       CHAR(1) NOT NULL, --数据表存储方式
REMARK                                              VARCHAR(6000) NULL, --备注
PRE_PARTITION                                       VARCHAR(512) NULL, --预分区
REPEAT_FLAG                                         CHAR(1) NOT NULL, --集市表是否可以重复使用
CATEGORY_ID                                         BIGINT default 0 NOT NULL, --集市分类id
CONSTRAINT DM_DATATABLE_PK PRIMARY KEY (DATATABLE_ID)   );

--数据表已选数据源信息
DROP TABLE IF EXISTS DM_DATATABLE_SOURCE ;
CREATE TABLE DM_DATATABLE_SOURCE(
OWN_DOURCE_TABLE_ID                                 BIGINT default 0 NOT NULL, --已选数据源表id
DATATABLE_ID                                        BIGINT default 0 NOT NULL, --数据表id
OWN_SOURCE_TABLE_NAME                               VARCHAR(512) NOT NULL, --已选数据源表名
SOURCE_TYPE                                         CHAR(3) NOT NULL, --数据来源类型
REMARK                                              VARCHAR(512) NULL, --备注
CONSTRAINT DM_DATATABLE_SOURCE_PK PRIMARY KEY (OWN_DOURCE_TABLE_ID)   );

--结果映射信息表
DROP TABLE IF EXISTS DM_ETLMAP_INFO ;
CREATE TABLE DM_ETLMAP_INFO(
ETL_ID                                              BIGINT default 0 NOT NULL, --表id
DATATABLE_ID                                        BIGINT default 0 NOT NULL, --数据表id
OWN_DOURCE_TABLE_ID                                 BIGINT default 0 NOT NULL, --已选数据源表id
TARGETFIELD_NAME                                    VARCHAR(512) NULL, --目标字段名称
SOURCEFIELDS_NAME                                   VARCHAR(512) NOT NULL, --来源字段名称
REMARK                                              VARCHAR(512) NULL, --备注
CONSTRAINT DM_ETLMAP_INFO_PK PRIMARY KEY (ETL_ID)   );

--数据源表字段
DROP TABLE IF EXISTS OWN_SOURCE_FIELD ;
CREATE TABLE OWN_SOURCE_FIELD(
OWN_FIELD_ID                                        BIGINT default 0 NOT NULL, --字段id
OWN_DOURCE_TABLE_ID                                 BIGINT default 0 NOT NULL, --已选数据源表id
FIELD_NAME                                          VARCHAR(512) NOT NULL, --字段名称
FIELD_TYPE                                          VARCHAR(512) NOT NULL, --字段类型
REMARK                                              VARCHAR(512) NULL, --备注
CONSTRAINT OWN_SOURCE_FIELD_PK PRIMARY KEY (OWN_FIELD_ID)   );

--数据操作信息表
DROP TABLE IF EXISTS DM_OPERATION_INFO ;
CREATE TABLE DM_OPERATION_INFO(
ID                                                  BIGINT default 0 NOT NULL, --信息表id
DATATABLE_ID                                        BIGINT default 0 NOT NULL, --数据表id
VIEW_SQL                                            VARCHAR(6000) NOT NULL, --预览sql语句
EXECUTE_SQL                                         VARCHAR(6000) NULL, --执行sql语句
SEARCH_NAME                                         VARCHAR(512) NULL, --join类型
REMARK                                              VARCHAR(512) NULL, --备注
VERSION_DATE                                        CHAR(8) NOT NULL, --版本日期
CONSTRAINT DM_OPERATION_INFO_PK PRIMARY KEY (ID)   );

--数据表字段信息
DROP TABLE IF EXISTS DATATABLE_FIELD_INFO ;
CREATE TABLE DATATABLE_FIELD_INFO(
DATATABLE_FIELD_ID                                  BIGINT default 0 NOT NULL, --数据表字段id
DATATABLE_ID                                        BIGINT default 0 NOT NULL, --数据表id
FIELD_CN_NAME                                       VARCHAR(512) NOT NULL, --字段中文名称
FIELD_EN_NAME                                       VARCHAR(512) NOT NULL, --字段英文名称
FIELD_TYPE                                          VARCHAR(30) NOT NULL, --字段类型
FIELD_DESC                                          VARCHAR(200) NULL, --字段描述
FIELD_PROCESS                                       CHAR(1) NOT NULL, --处理方式
PROCESS_MAPPING                                     VARCHAR(512) NULL, --映射规则mapping
GROUP_MAPPING                                       VARCHAR(200) NULL, --分组映射对应规则
FIELD_LENGTH                                        VARCHAR(200) NULL, --字段长度
FIELD_SEQ                                           BIGINT default 0 NOT NULL, --字段序号
REMARK                                              VARCHAR(6000) NULL, --备注
VERSION_DATE                                        CHAR(8) NOT NULL, --版本日期
CONSTRAINT DATATABLE_FIELD_INFO_PK PRIMARY KEY (DATATABLE_FIELD_ID)   );

--集市表前置后置作业
DROP TABLE IF EXISTS DM_RELEVANT_INFO ;
CREATE TABLE DM_RELEVANT_INFO(
REL_ID                                              BIGINT default 0 NOT NULL, --作业相关id
DATATABLE_ID                                        BIGINT default 0 NULL, --数据表id
PRE_WORK                                            VARCHAR(6500) NULL, --前置作业
POST_WORK                                           VARCHAR(6500) NULL, --后置作业
REL_REMARK                                          VARCHAR(512) NULL, --备注
CONSTRAINT DM_RELEVANT_INFO_PK PRIMARY KEY (REL_ID)   );

--数据字段存储关系表
DROP TABLE IF EXISTS DCOL_RELATION_STORE ;
CREATE TABLE DCOL_RELATION_STORE(
DSLAD_ID                                            BIGINT default 0 NOT NULL, --附加信息ID
COL_ID                                              BIGINT default 0 NOT NULL, --数据对应的字段
DATA_SOURCE                                         CHAR(1) NOT NULL, --存储层-数据来源
CSI_NUMBER                                          BIGINT default 0 NOT NULL, --序号位置
CONSTRAINT DCOL_RELATION_STORE_PK PRIMARY KEY (DSLAD_ID,COL_ID)   );

--数据表存储关系表
DROP TABLE IF EXISTS DTAB_RELATION_STORE ;
CREATE TABLE DTAB_RELATION_STORE(
DSL_ID                                              BIGINT default 0 NOT NULL, --存储层配置ID
TAB_ID                                              BIGINT default 0 NOT NULL, --数据对应表
DATA_SOURCE                                         CHAR(1) NOT NULL, --存储层-数据来源
IS_SUCCESSFUL                                       CHAR(3) default '104' NULL, --是否入库成功
CONSTRAINT DTAB_RELATION_STORE_PK PRIMARY KEY (DSL_ID,TAB_ID)   );

--carbondata预聚合信息表
DROP TABLE IF EXISTS CB_PREAGGREGATE ;
CREATE TABLE CB_PREAGGREGATE(
AGG_ID                                              BIGINT default 0 NOT NULL, --预聚合id
DATATABLE_ID                                        BIGINT default 0 NOT NULL, --数据表id
AGG_NAME                                            VARCHAR(512) NOT NULL, --预聚合名称
AGG_SQL                                             VARCHAR(512) NOT NULL, --预聚合SQL
AGG_DATE                                            CHAR(8) NOT NULL, --日期
AGG_TIME                                            CHAR(6) NOT NULL, --时间
AGG_STATUS                                          CHAR(3) default '105' NULL, --预聚合是否成功
REMARK                                              VARCHAR(512) NULL, --备注
CONSTRAINT CB_PREAGGREGATE_PK PRIMARY KEY (AGG_ID)   );

--数据存储层配置属性表
DROP TABLE IF EXISTS DATA_STORE_LAYER_ATTR ;
CREATE TABLE DATA_STORE_LAYER_ATTR(
DSLA_ID                                             BIGINT default 0 NOT NULL, --存储配置主键信息
STORAGE_PROPERTY_KEY                                VARCHAR(512) NOT NULL, --属性key
STORAGE_PROPERTY_VAL                                VARCHAR(512) NOT NULL, --属性value
IS_FILE                                             CHAR(1) NOT NULL, --是否为配置文件
DSLA_REMARK                                         VARCHAR(512) NULL, --备注
DSL_ID                                              BIGINT default 0 NOT NULL, --存储层配置ID
CONSTRAINT DATA_STORE_LAYER_ATTR_PK PRIMARY KEY (DSLA_ID)   );

--数据存储层配置表
DROP TABLE IF EXISTS DATA_STORE_LAYER ;
CREATE TABLE DATA_STORE_LAYER(
DSL_ID                                              BIGINT default 0 NOT NULL, --存储层配置ID
DSL_NAME                                            VARCHAR(512) NOT NULL, --配置属性名称
STORE_TYPE                                          CHAR(1) NOT NULL, --存储类型
IS_HADOOPCLIENT                                     CHAR(1) NOT NULL, --是否支持外部表
DATABASE_NAME                                       VARCHAR(512) NULL, --数据库名称
DSL_REMARK                                          VARCHAR(512) NULL, --备注
CONSTRAINT DATA_STORE_LAYER_PK PRIMARY KEY (DSL_ID)   );

--数据存储附加信息表
DROP TABLE IF EXISTS DATA_STORE_LAYER_ADDED ;
CREATE TABLE DATA_STORE_LAYER_ADDED(
DSLAD_ID                                            BIGINT default 0 NOT NULL, --附加信息ID
DSLA_STORELAYER                                     CHAR(2) NOT NULL, --配置附加属性信息
DSLAD_REMARK                                        VARCHAR(512) NULL, --备注
DSL_ID                                              BIGINT default 0 NOT NULL, --存储层配置ID
CONSTRAINT DATA_STORE_LAYER_ADDED_PK PRIMARY KEY (DSLAD_ID)   );

--数据表字段信息
DROP TABLE IF EXISTS DATATABLE_FIELD_INFO_VERSION ;
CREATE TABLE DATATABLE_FIELD_INFO_VERSION(
VERSION_ID                                          BIGINT default 0 NOT NULL, --版本主键ID
DATATABLE_FIELD_ID                                  BIGINT default 0 NOT NULL, --数据表字段id
DATATABLE_ID                                        BIGINT default 0 NOT NULL, --数据表id
FIELD_CN_NAME                                       VARCHAR(512) NOT NULL, --字段中文名称
FIELD_EN_NAME                                       VARCHAR(512) NOT NULL, --字段英文名称
FIELD_TYPE                                          VARCHAR(30) NOT NULL, --字段类型
FIELD_DESC                                          VARCHAR(200) NULL, --字段描述
FIELD_PROCESS                                       CHAR(1) NOT NULL, --处理方式
PROCESS_MAPPING                                     VARCHAR(512) NULL, --映射规则mapping
GROUP_MAPPING                                       VARCHAR(200) NULL, --分组映射对应规则
FIELD_LENGTH                                        VARCHAR(200) NULL, --字段长度
FIELD_SEQ                                           BIGINT default 0 NOT NULL, --字段序号
REMARK                                              VARCHAR(6000) NULL, --备注
VERSION_DATE                                        CHAR(8) NOT NULL, --版本日期
CONSTRAINT DATATABLE_FIELD_INFO_VERSION_PK PRIMARY KEY (VERSION_ID)   );

--数据操作信息表
DROP TABLE IF EXISTS DM_OPERATION_INFO_Version ;
CREATE TABLE DM_OPERATION_INFO_Version(
VERSION_ID                                          BIGINT default 0 NOT NULL, --版本主键ID
ID                                                  BIGINT default 0 NOT NULL, --信息表id
DATATABLE_ID                                        BIGINT default 0 NOT NULL, --数据表id
VIEW_SQL                                            VARCHAR(6000) NOT NULL, --预览sql语句
EXECUTE_SQL                                         VARCHAR(6000) NULL, --执行sql语句
SEARCH_NAME                                         VARCHAR(512) NULL, --join类型
REMARK                                              VARCHAR(512) NULL, --备注
VERSION_DATE                                        CHAR(8) NOT NULL, --版本日期
CONSTRAINT DM_OPERATION_INFO_Version_PK PRIMARY KEY (VERSION_ID)   );

