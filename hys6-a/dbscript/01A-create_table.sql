--任务作业关系表
DROP TABLE IF EXISTS TAKE_RELATION_ETL ;
CREATE TABLE TAKE_RELATION_ETL(
TRE_ID                                              BIGINT default 0 NOT NULL, --关系主键ID
ETL_SYS_ID                                          BIGINT default 0 NOT NULL, --系统主键ID
SUB_SYS_ID                                          BIGINT default 0 NOT NULL, --子系统主键ID
ETL_JOB_ID                                          BIGINT default 0 NOT NULL, --作业主键ID
JOB_DATASOURCE                                      CHAR(2) NOT NULL, --作业数据来源
TAKE_ID                                             BIGINT default 0 NOT NULL, --需要关联的业务表ID
TAKE_SOURCE_TABLE                                   VARCHAR(512) NULL, --需要关联的业务表
TRE_REMARK                                          VARCHAR(512) NULL, --备注
CONSTRAINT TAKE_RELATION_ETL_PK PRIMARY KEY (TRE_ID)   );

--系统操作信息
DROP TABLE IF EXISTS LOGIN_OPERATION_INFO ;
CREATE TABLE LOGIN_OPERATION_INFO(
LOG_ID                                              BIGINT default 0 NOT NULL, --日志ID
BROWSER_TYPE                                        VARCHAR(512) NULL, --浏览器类型
BROWSER_VERSION                                     VARCHAR(512) NULL, --浏览器版本
SYSTEM_TYPE                                         VARCHAR(512) NULL, --系统类型
REQUEST_MODE                                        VARCHAR(512) NULL, --请求方式
REMOTEADDR                                          VARCHAR(512) NULL, --客户端的IP
PROTOCOL                                            VARCHAR(512) NULL, --超文本传输协议版本
REQUEST_DATE                                        CHAR(8) NOT NULL, --请求日期
REQUEST_TIME                                        CHAR(6) NOT NULL, --请求时间
REQUEST_TYPE                                        VARCHAR(512) NULL, --请求类型
USER_ID                                             BIGINT default 0 NOT NULL, --用户ID
USER_NAME                                           VARCHAR(512) NULL, --用户名称
OPERATION_TYPE                                      TEXT NULL, --操作类型
CONSTRAINT LOGIN_OPERATION_INFO_PK PRIMARY KEY (LOG_ID)   );

--snowflake主键生成表
DROP TABLE IF EXISTS KEYTABLE_SNOWFLAKE ;
CREATE TABLE KEYTABLE_SNOWFLAKE(
PROJECT_ID                                          VARCHAR(80) NOT NULL, --project_id
DATACENTER_ID                                       INTEGER default 0 NULL, --datacenter_id
MACHINE_ID                                          INTEGER default 0 NULL, --machine_id
CONSTRAINT KEYTABLE_SNOWFLAKE_PK PRIMARY KEY (PROJECT_ID)   );

--数据加工spark语法提示
DROP TABLE IF EXISTS EDW_SPARKSQL_GRAM ;
CREATE TABLE EDW_SPARKSQL_GRAM(
ESG_ID                                              BIGINT default 0 NOT NULL, --序号
FUNCTION_NAME                                       VARCHAR(512) NOT NULL, --函数名称
FUNCTION_EXAMPLE                                    VARCHAR(512) NOT NULL, --函数例子
FUNCTION_DESC                                       VARCHAR(512) NOT NULL, --函数描述
IS_AVAILABLE                                        CHAR(1) NOT NULL, --是否可用
IS_UDF                                              CHAR(1) NOT NULL, --是否udf
CLASS_URL                                           VARCHAR(512) NULL, --函数类路径
JAR_URL                                             VARCHAR(512) NULL, --jar路径
HIVEDB_NAME                                         VARCHAR(100) NULL, --hive库名
IS_SPARKSQL                                         CHAR(1) NOT NULL, --是否同时使用sparksql
FUNCTION_CLASSIFY                                   VARCHAR(100) NOT NULL, --函数分类
REMARK                                              VARCHAR(512) NULL, --备注
CONSTRAINT EDW_SPARKSQL_GRAM_PK PRIMARY KEY (ESG_ID)   );

--主键生成表
DROP TABLE IF EXISTS KEYTABLE ;
CREATE TABLE KEYTABLE(
KEY_NAME                                            VARCHAR(80) NOT NULL, --key_name
KEY_VALUE                                           INTEGER default 0 NULL, --value
CONSTRAINT KEYTABLE_PK PRIMARY KEY (KEY_NAME)   );

--系统参数配置
DROP TABLE IF EXISTS SYS_PARA ;
CREATE TABLE SYS_PARA(
PARA_ID                                             BIGINT default 0 NOT NULL, --参数ID
PARA_NAME                                           VARCHAR(512) NULL, --para_name
PARA_VALUE                                          VARCHAR(512) NULL, --para_value
PARA_TYPE                                           VARCHAR(512) NULL, --para_type
REMARK                                              VARCHAR(512) NULL, --备注
CONSTRAINT SYS_PARA_PK PRIMARY KEY (PARA_ID)   );

--部门信息表
DROP TABLE IF EXISTS DEPARTMENT_INFO ;
CREATE TABLE DEPARTMENT_INFO(
DEP_ID                                              BIGINT default 0 NOT NULL, --部门ID
DEP_NAME                                            VARCHAR(512) NOT NULL, --部门名称
CREATE_DATE                                         CHAR(8) NOT NULL, --创建日期
CREATE_TIME                                         CHAR(6) NOT NULL, --创建时间
DEP_REMARK                                          VARCHAR(512) NULL, --备注
CONSTRAINT DEPARTMENT_INFO_PK PRIMARY KEY (DEP_ID)   );

--角色信息表
DROP TABLE IF EXISTS SYS_ROLE ;
CREATE TABLE SYS_ROLE(
ROLE_ID                                             BIGINT default 0 NOT NULL, --角色ID
ROLE_NAME                                           VARCHAR(512) NOT NULL, --角色名称
IS_ADMIN                                            CHAR(2) NOT NULL, --角色类型
ROLE_REMARK                                         VARCHAR(512) NULL, --备注
CONSTRAINT SYS_ROLE_PK PRIMARY KEY (ROLE_ID)   );

--组件信息表
DROP TABLE IF EXISTS COMPONENT_INFO ;
CREATE TABLE COMPONENT_INFO(
COMP_ID                                             VARCHAR(20) NOT NULL, --组件编号
COMP_NAME                                           VARCHAR(512) NOT NULL, --组件名称
COMP_STATE                                          CHAR(1) NOT NULL, --组件状态
COMP_VERSION                                        VARCHAR(100) NOT NULL, --组件版本
ICON_INFO                                           VARCHAR(512) NULL, --图标
COLOR_INFO                                          VARCHAR(512) NULL, --颜色
COMP_TYPE                                           CHAR(1) NOT NULL, --组件类型
COMP_REMARK                                         VARCHAR(512) NULL, --备注
CONSTRAINT COMPONENT_INFO_PK PRIMARY KEY (COMP_ID)   );

--用户登陆广播表
DROP TABLE IF EXISTS LOGIN_INFO ;
CREATE TABLE LOGIN_INFO(
LI_RADIO_ID                                         BIGINT default 0 NOT NULL, --用户登录广播表ID
USER_ID                                             BIGINT default 0 NULL, --用户ID
USER_NAME                                           VARCHAR(512) NOT NULL, --用户名称
LOGIN_MSG                                           VARCHAR(128) NULL, --用户登陆消息信息
LOGIN_IP                                            VARCHAR(50) NOT NULL, --登录IP
LOG_REMARK                                          VARCHAR(512) NULL, --备注
CONSTRAINT LOGIN_INFO_PK PRIMARY KEY (LI_RADIO_ID)   );

--组件参数
DROP TABLE IF EXISTS COMPONENT_PARAM ;
CREATE TABLE COMPONENT_PARAM(
PARAM_ID                                            BIGINT default 0 NOT NULL, --主键参数id
PARAM_NAME                                          VARCHAR(512) NOT NULL, --参数名称
PARAM_VALUE                                         VARCHAR(100) NOT NULL, --参数value
IS_MUST                                             CHAR(1) NOT NULL, --是否必要
PARAM_REMARK                                        VARCHAR(512) NOT NULL, --备注
COMP_ID                                             VARCHAR(20) NULL, --组件编号
CONSTRAINT COMPONENT_PARAM_PK PRIMARY KEY (PARAM_ID)   );

--代码信息表
DROP TABLE IF EXISTS CODE_INFO ;
CREATE TABLE CODE_INFO(
CI_SP_CODE                                          VARCHAR(20) NOT NULL, --代码值
CI_SP_CLASS                                         VARCHAR(20) NOT NULL, --所属类别号
CI_SP_CLASSNAME                                     VARCHAR(80) NOT NULL, --类别名称
CI_SP_NAME                                          VARCHAR(255) NOT NULL, --代码名称
CI_SP_REMARK                                        VARCHAR(512) NULL, --备注
CONSTRAINT CODE_INFO_PK PRIMARY KEY (CI_SP_CODE,CI_SP_CLASS)   );

--请求Agent类型
DROP TABLE IF EXISTS REQ_AGENTTYPE ;
CREATE TABLE REQ_AGENTTYPE(
REQ_ID                                              BIGINT default 0 NOT NULL, --请求ID
REQ_NAME                                            VARCHAR(512) NOT NULL, --中文名称
REQ_NO                                              CHAR(10) NULL, --请求编号
REQ_REMARK                                          VARCHAR(80) NULL, --备注
COMP_ID                                             VARCHAR(20) NOT NULL, --组件编号
CONSTRAINT REQ_AGENTTYPE_PK PRIMARY KEY (REQ_ID)   );

--solr数据关联表
DROP TABLE IF EXISTS SOLR_DATA_RELATION ;
CREATE TABLE SOLR_DATA_RELATION(
FIELD_NAME                                          VARCHAR(128) NOT NULL, --字段名字
CONSTRAINT SOLR_DATA_RELATION_PK PRIMARY KEY (FIELD_NAME)   );

--组件菜单表
DROP TABLE IF EXISTS COMPONENT_MENU ;
CREATE TABLE COMPONENT_MENU(
MENU_ID                                             BIGINT default 0 NOT NULL, --菜单ID
MENU_NAME                                           VARCHAR(200) NOT NULL, --菜单名称
MENU_DESC                                           VARCHAR(512) NOT NULL, --菜单描述
MENU_LEVEL                                          CHAR(1) NULL, --菜单级别
PARENT_ID                                           BIGINT default 0 NOT NULL, --菜单ID
MENU_PATH                                           VARCHAR(200) NOT NULL, --菜单path
COMP_ID                                             VARCHAR(20) NOT NULL, --组件编号
MENU_REMARK                                         VARCHAR(512) NULL, --备注
MENU_TYPE                                           CHAR(2) NULL, --菜单类型
CONSTRAINT COMPONENT_MENU_PK PRIMARY KEY (MENU_ID)   );

--角色菜单关系表
DROP TABLE IF EXISTS ROLE_MENU ;
CREATE TABLE ROLE_MENU(
ROLE_ID                                             BIGINT default 0 NOT NULL, --角色ID
MENU_ID                                             BIGINT default 0 NOT NULL, --菜单ID
REMARK                                              VARCHAR(512) NULL, --备注
CONSTRAINT ROLE_MENU_PK PRIMARY KEY (ROLE_ID,MENU_ID)   );

--系统备份信息表
DROP TABLE IF EXISTS SYS_DUMP ;
CREATE TABLE SYS_DUMP(
DUMP_ID                                             BIGINT default 0 NOT NULL, --备份id
BAK_DATE                                            CHAR(8) NOT NULL, --备份日期
BAK_TIME                                            CHAR(6) NOT NULL, --备份时间
FILE_SIZE                                           VARCHAR(512) NOT NULL, --文件大小
FILE_NAME                                           VARCHAR(512) NOT NULL, --文件名称
HDFS_PATH                                           VARCHAR(512) NOT NULL, --文件存放hdfs路径
LENGTH                                              VARCHAR(10) NOT NULL, --备份时长
REMARK                                              VARCHAR(512) NULL, --备注
CONSTRAINT SYS_DUMP_PK PRIMARY KEY (DUMP_ID)   );

--备份恢复信息表
DROP TABLE IF EXISTS SYS_RECOVER ;
CREATE TABLE SYS_RECOVER(
RE_ID                                               BIGINT default 0 NOT NULL, --恢复id
RE_DATE                                             CHAR(8) NOT NULL, --恢复日期
RE_TIME                                             CHAR(6) NOT NULL, --恢复时间
LENGTH                                              VARCHAR(10) NOT NULL, --恢复时长
REMARK                                              VARCHAR(512) NULL, --备注
DUMP_ID                                             BIGINT default 0 NOT NULL, --备份id
CONSTRAINT SYS_RECOVER_PK PRIMARY KEY (RE_ID)   );

--数据库类型
DROP TABLE IF EXISTS DATABASE_INFO ;
CREATE TABLE DATABASE_INFO(
DATABASE_NAME                                       VARCHAR(512) NOT NULL, --数据库名称
DATABASE_REMARK                                     VARCHAR(512) NULL, --备注
CONSTRAINT DATABASE_INFO_PK PRIMARY KEY (DATABASE_NAME)   );

--数据库类型映射表
DROP TABLE IF EXISTS DATABASE_TYPE_MAPPING ;
CREATE TABLE DATABASE_TYPE_MAPPING(
DTM_ID                                              BIGINT default 0 NOT NULL, --数据库类型映射ID
DATABASE_NAME1                                      VARCHAR(512) NOT NULL, --数据库名称1
DATABASE_TYPE1                                      VARCHAR(512) NOT NULL, --数据库类型1
DATABASE_NAME2                                      VARCHAR(512) NOT NULL, --数据库名称2
DATABASE_TYPE2                                      VARCHAR(512) NOT NULL, --数据库类型2
IS_DEFAULT                                          CHAR(1) default '0' NOT NULL, --是否为默认值
DTM_REMARK                                          VARCHAR(512) NULL, --备注
CONSTRAINT DATABASE_TYPE_MAPPING_PK PRIMARY KEY (DTM_ID)   );

--编码信息表
DROP TABLE IF EXISTS HYREN_CODE_INFO ;
CREATE TABLE HYREN_CODE_INFO(
CODE_CLASSIFY                                       VARCHAR(100) NOT NULL, --编码分类
CODE_VALUE                                          VARCHAR(100) NOT NULL, --编码类型值
CODE_CLASSIFY_NAME                                  VARCHAR(512) NOT NULL, --编码分类名称
CODE_TYPE_NAME                                      VARCHAR(512) NOT NULL, --编码名称
CODE_REMARK                                         VARCHAR(512) NULL, --编码描述
CONSTRAINT HYREN_CODE_INFO_PK PRIMARY KEY (CODE_CLASSIFY,CODE_VALUE)   );

--源系统编码信息
DROP TABLE IF EXISTS ORIG_CODE_INFO ;
CREATE TABLE ORIG_CODE_INFO(
ORIG_ID                                             BIGINT default 0 NOT NULL, --源系统编码主键
ORIG_SYS_CODE                                       VARCHAR(100) NULL, --码值系统编码
CODE_CLASSIFY                                       VARCHAR(100) NOT NULL, --编码分类
CODE_VALUE                                          VARCHAR(100) NOT NULL, --编码类型值
ORIG_VALUE                                          VARCHAR(100) NOT NULL, --源系统编码值
CODE_REMARK                                         VARCHAR(512) NULL, --系统编码描述
CONSTRAINT ORIG_CODE_INFO_PK PRIMARY KEY (ORIG_ID)   );

--源系统信
DROP TABLE IF EXISTS ORIG_SYSO_INFO ;
CREATE TABLE ORIG_SYSO_INFO(
ORIG_SYS_CODE                                       VARCHAR(100) NOT NULL, --码值系统编码
ORIG_SYS_NAME                                       VARCHAR(100) NOT NULL, --码值系统名称
ORIG_SYS_REMARK                                     VARCHAR(512) NULL, --码值系统描述
CONSTRAINT ORIG_SYSO_INFO_PK PRIMARY KEY (ORIG_SYS_CODE)   );

--数据库默认长度映射表
DROP TABLE IF EXISTS DEFAULT_LENGTH_MAPPING ;
CREATE TABLE DEFAULT_LENGTH_MAPPING(
DATABASE_NAME                                       VARCHAR(512) NOT NULL, --数据库名称
COLUMN_TYPE                                         VARCHAR(100) NOT NULL, --数据库字段类型
COLUMN_LENGTH                                       VARCHAR(100) NULL, --数据库字段默认长度
DLM_REMARK                                          VARCHAR(80) NULL, --备注
CONSTRAINT DEFAULT_LENGTH_MAPPING_PK PRIMARY KEY (DATABASE_NAME,COLUMN_TYPE)   );

