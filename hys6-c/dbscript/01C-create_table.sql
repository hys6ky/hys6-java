--对象采集数据处理类型对应表
DROP TABLE IF EXISTS OBJECT_HANDLE_TYPE ;
CREATE TABLE OBJECT_HANDLE_TYPE(
OBJECT_HANDLE_ID                                    BIGINT default 0 NOT NULL, --处理编号
OCS_ID                                              BIGINT default 0 NOT NULL, --对象采集任务编号
HANDLE_TYPE                                         CHAR(1) NOT NULL, --处理类型
HANDLE_VALUE                                        VARCHAR(100) NOT NULL, --处理值
CONSTRAINT OBJECT_HANDLE_TYPE_PK PRIMARY KEY (OBJECT_HANDLE_ID,OCS_ID)   );

--对象采集结构信息
DROP TABLE IF EXISTS OBJECT_COLLECT_STRUCT ;
CREATE TABLE OBJECT_COLLECT_STRUCT(
STRUCT_ID                                           BIGINT default 0 NOT NULL, --结构信息id
OCS_ID                                              BIGINT default 0 NOT NULL, --对象采集任务编号
COLUMN_NAME                                         VARCHAR(512) NOT NULL, --字段英文名称
DATA_DESC                                           VARCHAR(200) NULL, --字段中文描述信息
IS_OPERATE                                          CHAR(1) NOT NULL, --是否操作标识字段
IS_ZIPPER_FIELD                                     CHAR(1) NOT NULL, --是否为拉链字段
COLUMNPOSITION                                      VARCHAR(100) NOT NULL, --字段位置
COLUMN_TYPE                                         VARCHAR(100) NOT NULL, --字段类型
REMARK                                              VARCHAR(512) NULL, --备注
CONSTRAINT OBJECT_COLLECT_STRUCT_PK PRIMARY KEY (STRUCT_ID)   );

--数据存储登记
DROP TABLE IF EXISTS DATA_STORE_REG ;
CREATE TABLE DATA_STORE_REG(
FILE_ID                                             VARCHAR(40) NOT NULL, --表文件ID
COLLECT_TYPE                                        CHAR(1) NOT NULL, --采集类型
ORIGINAL_UPDATE_DATE                                CHAR(8) NOT NULL, --原文件最后修改日期
ORIGINAL_UPDATE_TIME                                CHAR(6) NOT NULL, --原文件最后修改时间
ORIGINAL_NAME                                       VARCHAR(512) NOT NULL, --原始表中文名称
TABLE_NAME                                          VARCHAR(512) NULL, --采集的原始表名
HYREN_NAME                                          VARCHAR(512) NOT NULL, --系统内对应表名
META_INFO                                           VARCHAR(6000) NULL, --META元信息
STORAGE_DATE                                        CHAR(8) NOT NULL, --入库日期
STORAGE_TIME                                        CHAR(6) NOT NULL, --入库时间
FILE_SIZE                                           BIGINT default 0 NOT NULL, --文件大小
AGENT_ID                                            BIGINT default 0 NOT NULL, --Agent_id
SOURCE_ID                                           BIGINT default 0 NOT NULL, --数据源ID
DATABASE_ID                                         BIGINT default 0 NOT NULL, --数据库设置id
TABLE_ID                                            BIGINT default 0 NOT NULL, --表名ID
CONSTRAINT DATA_STORE_REG_PK PRIMARY KEY (FILE_ID)   );

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

--数据库采集周期
DROP TABLE IF EXISTS TABLE_CYCLE ;
CREATE TABLE TABLE_CYCLE(
TC_ID                                               BIGINT default 0 NOT NULL, --周期ID
TABLE_ID                                            BIGINT default 0 NOT NULL, --表名ID
INTERVAL_TIME                                       BIGINT default 0 NOT NULL, --频率间隔时间（秒）
OVER_DATE                                           CHAR(8) NOT NULL, --结束日期
TC_REMARK                                           VARCHAR(512) NULL, --备注
CONSTRAINT TABLE_CYCLE_PK PRIMARY KEY (TC_ID)   );

--部门信息表
DROP TABLE IF EXISTS DEPARTMENT_INFO ;
CREATE TABLE DEPARTMENT_INFO(
DEP_ID                                              BIGINT default 0 NOT NULL, --部门ID
DEP_NAME                                            VARCHAR(512) NOT NULL, --部门名称
CREATE_DATE                                         CHAR(8) NOT NULL, --创建日期
CREATE_TIME                                         CHAR(6) NOT NULL, --创建时间
SUP_DEP_ID                                          BIGINT default 0 NULL, --上级部门ID
DEP_REMARK                                          VARCHAR(512) NULL, --备注
CONSTRAINT DEPARTMENT_INFO_PK PRIMARY KEY (DEP_ID)   );

--采集情况信息表
DROP TABLE IF EXISTS COLLECT_CASE ;
CREATE TABLE COLLECT_CASE(
JOB_RS_ID                                           VARCHAR(40) NOT NULL, --作业执行结果ID
COLLECT_TYPE                                        CHAR(1) NOT NULL, --采集类型
JOB_TYPE                                            VARCHAR(10) NULL, --任务类型
COLLECT_TOTAL                                       BIGINT default 0 NULL, --总共采集(文件)表
COLECT_RECORD                                       BIGINT default 0 NOT NULL, --总共采集记录数
COLLET_DATABASE_SIZE                                VARCHAR(100) NULL, --总共采集数据大小
COLLECT_S_DATE                                      CHAR(8) NOT NULL, --开始采集日期
COLLECT_S_TIME                                      CHAR(6) NOT NULL, --开始采集时间
COLLECT_E_DATE                                      CHAR(8) NULL, --采集结束日期
COLLECT_E_TIME                                      CHAR(6) NULL, --采集结束时间
EXECUTE_LENGTH                                      VARCHAR(10) NULL, --运行总时长
EXECUTE_STATE                                       CHAR(2) NOT NULL, --运行状态
IS_AGAIN                                            CHAR(1) NOT NULL, --是否重跑
AGAIN_NUM                                           BIGINT default 0 NULL, --重跑次数
JOB_GROUP                                           VARCHAR(100) NOT NULL, --agent组ID
TASK_CLASSIFY                                       VARCHAR(512) NULL, --任务分类
ETL_DATE                                            VARCHAR(18) NULL, --跑批日期
AGENT_ID                                            BIGINT default 0 NOT NULL, --Agent_id
COLLECT_SET_ID                                      BIGINT default 0 NOT NULL, --数据库设置id
SOURCE_ID                                           BIGINT default 0 NOT NULL, --数据源ID
CC_REMARK                                           VARCHAR(512) NULL, --备注
CONSTRAINT COLLECT_CASE_PK PRIMARY KEY (JOB_RS_ID)   );

--Ftp采集设置
DROP TABLE IF EXISTS FTP_COLLECT ;
CREATE TABLE FTP_COLLECT(
FTP_ID                                              BIGINT default 0 NOT NULL, --ftp采集id
FTP_NUMBER                                          VARCHAR(200) NOT NULL, --ftp任务编号
FTP_NAME                                            VARCHAR(512) NOT NULL, --ftp采集任务名称
START_DATE                                          CHAR(8) NOT NULL, --开始日期
END_DATE                                            CHAR(8) NOT NULL, --结束日期
FTP_IP                                              VARCHAR(50) NOT NULL, --ftp服务IP
FTP_PORT                                            VARCHAR(10) NOT NULL, --ftp服务器端口
FTP_USERNAME                                        VARCHAR(512) NOT NULL, --ftp用户名
FTP_PASSWORD                                        VARCHAR(100) NOT NULL, --用户密码
FTP_DIR                                             VARCHAR(512) NOT NULL, --ftp服务器目录
LOCAL_PATH                                          VARCHAR(512) NOT NULL, --本地路径
FTP_RULE_PATH                                       CHAR(1) default '1' NOT NULL, --下级目录规则
CHILD_FILE_PATH                                     VARCHAR(512) NULL, --下级文件路径
CHILD_TIME                                          CHAR(1) NULL, --下级文件时间
FILE_SUFFIX                                         VARCHAR(200) NULL, --获取文件后缀
FTP_MODEL                                           CHAR(1) default '1' NOT NULL, --FTP推拉模式是为推模式
RUN_WAY                                             CHAR(1) NOT NULL, --启动方式
REMARK                                              VARCHAR(512) NULL, --备注
IS_SENDOK                                           CHAR(1) NOT NULL, --是否完成
IS_UNZIP                                            CHAR(1) NOT NULL, --是否解压
REDUCE_TYPE                                         CHAR(1) NULL, --解压格式
IS_READ_REALTIME                                    CHAR(1) NOT NULL, --是否实时读取
REALTIME_INTERVAL                                   BIGINT default 0 NOT NULL, --实时读取间隔时间
AGENT_ID                                            BIGINT default 0 NOT NULL, --Agent_id
CONSTRAINT FTP_COLLECT_PK PRIMARY KEY (FTP_ID)   );

--ftp已传输表
DROP TABLE IF EXISTS FTP_TRANSFERED ;
CREATE TABLE FTP_TRANSFERED(
FTP_TRANSFERED_ID                                   VARCHAR(40) NOT NULL, --已传输表id-UUID
FTP_ID                                              BIGINT default 0 NOT NULL, --ftp采集id
TRANSFERED_NAME                                     VARCHAR(512) NOT NULL, --已传输文件名称
FILE_PATH                                           VARCHAR(512) NOT NULL, --文件绝对路径
FTP_FILEMD5                                         VARCHAR(40) NULL, --文件MD5
FTP_DATE                                            CHAR(8) NOT NULL, --ftp日期
FTP_TIME                                            CHAR(6) NOT NULL, --ftp时间
REMARK                                              VARCHAR(512) NULL, --备注
CONSTRAINT FTP_TRANSFERED_PK PRIMARY KEY (FTP_TRANSFERED_ID)   );

--数据库类型
DROP TABLE IF EXISTS DATABASE_INFO ;
CREATE TABLE DATABASE_INFO(
DATABASE_NAME                                       VARCHAR(512) NOT NULL, --数据库名称
DATABASE_REMARK                                     VARCHAR(512) NULL, --备注
CONSTRAINT DATABASE_INFO_PK PRIMARY KEY (DATABASE_NAME)   );

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

--错误信息表
DROP TABLE IF EXISTS ERROR_INFO ;
CREATE TABLE ERROR_INFO(
ERROR_ID                                            BIGINT default 0 NOT NULL, --错误ID
JOB_RS_ID                                           VARCHAR(40) NOT NULL, --作业执行结果ID
ERROR_MSG                                           VARCHAR(15555) NULL, --error_msg
CONSTRAINT ERROR_INFO_PK PRIMARY KEY (ERROR_ID)   );

--信号文件入库信息
DROP TABLE IF EXISTS SIGNAL_FILE ;
CREATE TABLE SIGNAL_FILE(
SIGNAL_ID                                           BIGINT default 0 NOT NULL, --信号id
IS_INTO_HBASE                                       CHAR(1) NOT NULL, --是否入hbase
IS_COMPRESSION                                      CHAR(1) default '0' NOT NULL, --Hbase是使用压缩
IS_INTO_HIVE                                        CHAR(1) NOT NULL, --是否入hive
IS_MPP                                              CHAR(1) NOT NULL, --是否为MPP
TABLE_TYPE                                          CHAR(1) NOT NULL, --是内部表还是外部表
IS_FULLINDEX                                        CHAR(1) NOT NULL, --是否创建全文索引
FILE_FORMAT                                         CHAR(1) NOT NULL, --文件格式
IS_SOLR_HBASE                                       CHAR(1) default '1' NOT NULL, --是否使用solrOnHbase
IS_CBD                                              CHAR(1) default '1' NOT NULL, --是否使用carbondata
DATABASE_ID                                         BIGINT default 0 NOT NULL, --数据库设置id
CONSTRAINT SIGNAL_FILE_PK PRIMARY KEY (SIGNAL_ID)   );

--表存储信息
DROP TABLE IF EXISTS TABLE_STORAGE_INFO ;
CREATE TABLE TABLE_STORAGE_INFO(
STORAGE_ID                                          BIGINT default 0 NOT NULL, --储存编号
FILE_FORMAT                                         CHAR(1) default '1' NOT NULL, --文件格式
STORAGE_TYPE                                        CHAR(1) NOT NULL, --进数方式
IS_ZIPPER                                           CHAR(1) NOT NULL, --是否拉链存储
IS_MD5                                              CHAR(1) default '0' NOT NULL, --是否计算MD5
STORAGE_TIME                                        BIGINT default 0 NOT NULL, --存储期限（以天为单位）
HYREN_NAME                                          VARCHAR(100) NOT NULL, --进库之后拼接的表名
IS_PREFIX                                           CHAR(1) default '1' NOT NULL, --表名是否使用前缀
TABLE_ID                                            BIGINT default 0 NULL, --表名ID
CONSTRAINT TABLE_STORAGE_INFO_PK PRIMARY KEY (STORAGE_ID)   );

--数据源
DROP TABLE IF EXISTS DATA_SOURCE ;
CREATE TABLE DATA_SOURCE(
SOURCE_ID                                           BIGINT default 0 NOT NULL, --数据源ID
DATASOURCE_NUMBER                                   VARCHAR(100) NULL, --数据源编号
DATASOURCE_NAME                                     VARCHAR(512) NOT NULL, --数据源名称
SOURCE_REMARK                                       VARCHAR(512) NULL, --数据源详细描述
CREATE_DATE                                         CHAR(8) NOT NULL, --创建日期
CREATE_TIME                                         CHAR(6) NOT NULL, --创建时间
CREATE_USER_ID                                      BIGINT default 0 NOT NULL, --创建用户
DATASOURCE_REMARK                                   VARCHAR(512) NULL, --备注
CONSTRAINT DATA_SOURCE_PK PRIMARY KEY (SOURCE_ID)   );

--数据源与部门关系
DROP TABLE IF EXISTS SOURCE_RELATION_DEP ;
CREATE TABLE SOURCE_RELATION_DEP(
SOURCE_ID                                           BIGINT default 0 NOT NULL, --数据源ID
DEP_ID                                              BIGINT default 0 NOT NULL, --部门ID
CONSTRAINT SOURCE_RELATION_DEP_PK PRIMARY KEY (SOURCE_ID,DEP_ID)   );

--Agent信息表
DROP TABLE IF EXISTS AGENT_INFO ;
CREATE TABLE AGENT_INFO(
AGENT_ID                                            BIGINT default 0 NOT NULL, --Agent_id
AGENT_NAME                                          VARCHAR(512) NOT NULL, --Agent名称
AGENT_TYPE                                          CHAR(1) NOT NULL, --agent类别
AGENT_IP                                            VARCHAR(50) NOT NULL, --Agent所在服务器IP
AGENT_PORT                                          VARCHAR(10) NULL, --agent服务器端口
AGENT_STATUS                                        CHAR(1) NOT NULL, --agent状态
CREATE_DATE                                         CHAR(8) NOT NULL, --创建日期
CREATE_TIME                                         CHAR(6) NOT NULL, --创建时间
SOURCE_ID                                           BIGINT default 0 NOT NULL, --数据源ID
USER_ID                                             BIGINT default 0 NOT NULL, --用户ID
CONSTRAINT AGENT_INFO_PK PRIMARY KEY (AGENT_ID)   );

--源系统数据库设置
DROP TABLE IF EXISTS DATABASE_SET ;
CREATE TABLE DATABASE_SET(
DATABASE_ID                                         BIGINT default 0 NOT NULL, --数据库设置id
AGENT_ID                                            BIGINT default 0 NULL, --Agent_id
HOST_NAME                                           VARCHAR(512) NULL, --主机名
DATABASE_NUMBER                                     VARCHAR(10) NULL, --数据库设置编号
SYSTEM_TYPE                                         VARCHAR(512) NULL, --操作系统类型
TASK_NAME                                           VARCHAR(512) NULL, --数据库采集任务名称
DB_AGENT                                            CHAR(1) NOT NULL, --是否DB文件数据采集
DATABASE_SEPARATORR                                 VARCHAR(512) NULL, --数据采用分隔符
ROW_SEPARATOR                                       VARCHAR(512) NULL, --数据行分隔符
PLANE_URL                                           VARCHAR(512) NULL, --DB文件数据字典位置
IS_SENDOK                                           CHAR(1) NOT NULL, --是否设置完成并发送成功
CP_OR                                               VARCHAR(512) NULL, --清洗顺序
COLLECT_TYPE                                        CHAR(1) NOT NULL, --数据库采集方式
DSL_ID                                              BIGINT default 0 NULL, --存储层配置ID
CLASSIFY_ID                                         BIGINT default 0 NOT NULL, --分类id
SOURCE_ID                                           BIGINT default 0 NULL, --数据源ID
CONSTRAINT DATABASE_SET_PK PRIMARY KEY (DATABASE_ID)   );

--清洗作业参数属性表
DROP TABLE IF EXISTS CLEAN_PARAMETER ;
CREATE TABLE CLEAN_PARAMETER(
C_ID                                                BIGINT default 0 NOT NULL, --清洗参数编号
CLEAN_TYPE                                          CHAR(1) NOT NULL, --清洗方式
FILLING_TYPE                                        CHAR(1) NULL, --补齐方式
CHARACTER_FILLING                                   VARCHAR(512) NULL, --补齐字符
FILLING_LENGTH                                      BIGINT default 0 NULL, --补齐长度
FIELD                                               VARCHAR(512) NULL, --原字段
REPLACE_FEILD                                       VARCHAR(512) NULL, --替换字段
DATABASE_ID                                         BIGINT default 0 NOT NULL, --数据库设置id
CONSTRAINT CLEAN_PARAMETER_PK PRIMARY KEY (C_ID)   );

--数据库对应表
DROP TABLE IF EXISTS TABLE_INFO ;
CREATE TABLE TABLE_INFO(
TABLE_ID                                            BIGINT default 0 NOT NULL, --表名ID
TABLE_NAME                                          VARCHAR(512) NOT NULL, --表名
TABLE_CH_NAME                                       VARCHAR(512) NOT NULL, --中文名称
REC_NUM_DATE                                        CHAR(8) NOT NULL, --数据获取时间
TABLE_COUNT                                         VARCHAR(16) default 0 NULL, --记录数
DATABASE_ID                                         BIGINT default 0 NOT NULL, --数据库设置id
SOURCE_TABLEID                                      VARCHAR(512) NULL, --源表ID
VALID_S_DATE                                        CHAR(8) NOT NULL, --有效开始日期
VALID_E_DATE                                        CHAR(8) NOT NULL, --有效结束日期
UNLOAD_TYPE                                         CHAR(1) NULL, --落地文件-卸数方式
SQL                                                 VARCHAR(6000) NULL, --自定义sql语句
TI_OR                                               VARCHAR(512) NULL, --清洗顺序
IS_MD5                                              CHAR(1) NOT NULL, --是否使用MD5-只限数据抽数
IS_REGISTER                                         CHAR(1) NOT NULL, --是否仅登记
IS_CUSTOMIZE_SQL                                    CHAR(1) NOT NULL, --是否并行抽取中的自定义sql
IS_PARALLEL                                         CHAR(1) NOT NULL, --是否并行抽取
IS_USER_DEFINED                                     CHAR(1) default '1' NOT NULL, --是否sql抽取
PAGE_SQL                                            VARCHAR(6000) NULL, --分页sql
PAGEPARALLELS                                       INTEGER default 0 NULL, --分页并行数
DATAINCREMENT                                       INTEGER default 0 NULL, --每天数据增量
DATABASE_TYPE                                       VARCHAR(512) NULL, --数据库类型-只针对db文件采集
REMARK                                              VARCHAR(512) NULL, --备注
CONSTRAINT TABLE_INFO_PK PRIMARY KEY (TABLE_ID)   );

--表清洗参数信息
DROP TABLE IF EXISTS TABLE_CLEAN ;
CREATE TABLE TABLE_CLEAN(
TABLE_CLEAN_ID                                      BIGINT default 0 NOT NULL, --清洗参数编号
CLEAN_TYPE                                          CHAR(1) NOT NULL, --清洗方式
FILLING_TYPE                                        CHAR(1) NULL, --补齐方式
CHARACTER_FILLING                                   VARCHAR(512) NULL, --补齐字符
FILLING_LENGTH                                      BIGINT default 0 NULL, --补齐长度
FIELD                                               VARCHAR(512) NULL, --原字段
REPLACE_FEILD                                       VARCHAR(512) NULL, --替换字段
TABLE_ID                                            BIGINT default 0 NOT NULL, --表名ID
CONSTRAINT TABLE_CLEAN_PK PRIMARY KEY (TABLE_CLEAN_ID)   );

--列清洗参数信息
DROP TABLE IF EXISTS COLUMN_CLEAN ;
CREATE TABLE COLUMN_CLEAN(
COL_CLEAN_ID                                        BIGINT default 0 NOT NULL, --清洗参数编号
CONVERT_FORMAT                                      VARCHAR(512) NULL, --转换格式
OLD_FORMAT                                          VARCHAR(512) NULL, --原始格式
CLEAN_TYPE                                          CHAR(1) NOT NULL, --清洗方式
FILLING_TYPE                                        CHAR(1) NULL, --补齐方式
CHARACTER_FILLING                                   VARCHAR(512) NULL, --补齐字符
FILLING_LENGTH                                      BIGINT default 0 NULL, --补齐长度
CODENAME                                            VARCHAR(512) NULL, --码值名称
CODESYS                                             VARCHAR(512) NULL, --码值所属系统
FIELD                                               VARCHAR(512) NULL, --原字段
REPLACE_FEILD                                       VARCHAR(512) NULL, --替换字段
COLUMN_ID                                           BIGINT default 0 NOT NULL, --字段ID
CONSTRAINT COLUMN_CLEAN_PK PRIMARY KEY (COL_CLEAN_ID)   );

--Agent下载信息
DROP TABLE IF EXISTS AGENT_DOWN_INFO ;
CREATE TABLE AGENT_DOWN_INFO(
DOWN_ID                                             BIGINT default 0 NOT NULL, --下载编号(primary)
AGENT_NAME                                          VARCHAR(512) NOT NULL, --Agent名称
AGENT_IP                                            VARCHAR(50) NOT NULL, --Agent IP
AGENT_PORT                                          VARCHAR(10) NOT NULL, --Agent端口
USER_NAME                                           VARCHAR(10) NULL, --用户名
PASSWD                                              VARCHAR(10) NULL, --密码
SAVE_DIR                                            VARCHAR(512) NOT NULL, --存放目录
LOG_DIR                                             VARCHAR(512) NOT NULL, --日志目录
DEPLOY                                              CHAR(1) NOT NULL, --是否部署
AI_DESC                                             VARCHAR(200) NULL, --描述
AGENT_CONTEXT                                       VARCHAR(200) NOT NULL, --agent的context
AGENT_PATTERN                                       VARCHAR(200) NOT NULL, --agent的访问路径
AGENT_TYPE                                          CHAR(1) NOT NULL, --agent类别
AGENT_ID                                            BIGINT default 0 NULL, --Agent_id
USER_ID                                             BIGINT default 0 NOT NULL, --用户ID
agent_date                                          CHAR(8) NULL, --agent部署日期
agent_time                                          CHAR(6) NULL, --agent部署时间
REMARK                                              VARCHAR(512) NULL, --备注
CONSTRAINT AGENT_DOWN_INFO_PK PRIMARY KEY (DOWN_ID)   );

--表对应的字段
DROP TABLE IF EXISTS TABLE_COLUMN ;
CREATE TABLE TABLE_COLUMN(
COLUMN_ID                                           BIGINT default 0 NOT NULL, --字段ID
IS_GET                                              CHAR(1) default '0' NULL, --是否采集
IS_PRIMARY_KEY                                      CHAR(1) NOT NULL, --是否为主键
COLUMN_NAME                                         VARCHAR(512) NOT NULL, --列名
COLUMN_TYPE                                         VARCHAR(512) NULL, --列字段类型
COLUMN_CH_NAME                                      VARCHAR(512) NULL, --列中文名称
TABLE_ID                                            BIGINT default 0 NOT NULL, --表名ID
VALID_S_DATE                                        CHAR(8) NOT NULL, --有效开始日期
VALID_E_DATE                                        CHAR(8) NOT NULL, --有效结束日期
IS_ALIVE                                            CHAR(1) default '1' NOT NULL, --是否保留原字段
IS_NEW                                              CHAR(1) default '1' NOT NULL, --是否为变化生成
IS_ZIPPER_FIELD                                     CHAR(1) default '0' NOT NULL, --是否为拉链字段
TC_OR                                               VARCHAR(512) NULL, --清洗顺序
TC_REMARK                                           VARCHAR(512) NULL, --备注
CONSTRAINT TABLE_COLUMN_PK PRIMARY KEY (COLUMN_ID)   );

--采集任务分类表
DROP TABLE IF EXISTS COLLECT_JOB_CLASSIFY ;
CREATE TABLE COLLECT_JOB_CLASSIFY(
CLASSIFY_ID                                         BIGINT default 0 NOT NULL, --分类id
CLASSIFY_NUM                                        VARCHAR(512) NOT NULL, --分类编号
CLASSIFY_NAME                                       VARCHAR(512) NOT NULL, --分类名称
REMARK                                              VARCHAR(512) NULL, --备注
USER_ID                                             BIGINT default 0 NOT NULL, --用户ID
AGENT_ID                                            BIGINT default 0 NULL, --Agent_id
SOURCE_ID                                           BIGINT default 0 NULL, --数据源ID
CONSTRAINT COLLECT_JOB_CLASSIFY_PK PRIMARY KEY (CLASSIFY_ID)   );

--文件源设置
DROP TABLE IF EXISTS FILE_SOURCE ;
CREATE TABLE FILE_SOURCE(
FILE_SOURCE_ID                                      BIGINT default 0 NOT NULL, --文件源ID
FILE_SOURCE_PATH                                    VARCHAR(512) NOT NULL, --文件源路径
IS_PDF                                              CHAR(1) NOT NULL, --PDF文件
IS_OFFICE                                           CHAR(1) NOT NULL, --office文件
IS_TEXT                                             CHAR(1) NOT NULL, --文本文件
IS_VIDEO                                            CHAR(1) NOT NULL, --视频文件
IS_AUDIO                                            CHAR(1) NOT NULL, --音频文件
IS_IMAGE                                            CHAR(1) NOT NULL, --图片文件
IS_COMPRESS                                         CHAR(1) NOT NULL, --压缩文件
CUSTOM_SUFFIX                                       VARCHAR(80) NULL, --自定义后缀
IS_OTHER                                            CHAR(1) NOT NULL, --其他
FILE_REMARK                                         VARCHAR(512) NULL, --备注
FCS_ID                                              BIGINT default 0 NOT NULL, --文件系统采集ID
AGENT_ID                                            BIGINT default 0 NOT NULL, --Agent_id
CONSTRAINT FILE_SOURCE_PK PRIMARY KEY (FILE_SOURCE_ID)   );

--列拆分信息表
DROP TABLE IF EXISTS COLUMN_SPLIT ;
CREATE TABLE COLUMN_SPLIT(
COL_SPLIT_ID                                        BIGINT default 0 NOT NULL, --字段编号
COL_NAME                                            VARCHAR(512) NOT NULL, --字段名称
COL_OFFSET                                          VARCHAR(512) NULL, --字段偏移量
SPLIT_SEP                                           VARCHAR(512) NULL, --拆分分隔符
SEQ                                                 BIGINT default 0 NULL, --拆分对应序号
SPLIT_TYPE                                          CHAR(1) NOT NULL, --拆分方式
COL_ZHNAME                                          VARCHAR(512) NULL, --中文名称
REMARK                                              VARCHAR(512) NULL, --备注
COL_TYPE                                            VARCHAR(512) NOT NULL, --字段类型
VALID_S_DATE                                        CHAR(8) NOT NULL, --有效开始日期
VALID_E_DATE                                        CHAR(8) NOT NULL, --有效结束日期
COL_CLEAN_ID                                        BIGINT default 0 NOT NULL, --清洗参数编号
COLUMN_ID                                           BIGINT default 0 NOT NULL, --字段ID
CONSTRAINT COLUMN_SPLIT_PK PRIMARY KEY (COL_SPLIT_ID)   );

--列合并信息表
DROP TABLE IF EXISTS COLUMN_MERGE ;
CREATE TABLE COLUMN_MERGE(
COL_MERGE_ID                                        BIGINT default 0 NOT NULL, --字段编号
COL_NAME                                            VARCHAR(512) NOT NULL, --合并后字段名称
OLD_NAME                                            VARCHAR(512) NOT NULL, --要合并的字段
COL_ZHNAME                                          VARCHAR(512) NULL, --中文名称
REMARK                                              VARCHAR(512) NULL, --备注
COL_TYPE                                            VARCHAR(512) NOT NULL, --字段类型
VALID_S_DATE                                        CHAR(8) NOT NULL, --有效开始日期
VALID_E_DATE                                        CHAR(8) NOT NULL, --有效结束日期
TABLE_ID                                            BIGINT default 0 NOT NULL, --表名ID
CONSTRAINT COLUMN_MERGE_PK PRIMARY KEY (COL_MERGE_ID)   );

--对象采集对应信息
DROP TABLE IF EXISTS OBJECT_COLLECT_TASK ;
CREATE TABLE OBJECT_COLLECT_TASK(
OCS_ID                                              BIGINT default 0 NOT NULL, --对象采集任务编号
AGENT_ID                                            BIGINT default 0 NOT NULL, --Agent_id
EN_NAME                                             VARCHAR(512) NOT NULL, --英文名称
ZH_NAME                                             VARCHAR(512) NOT NULL, --中文名称
COLLECT_DATA_TYPE                                   CHAR(1) NOT NULL, --数据类型
FIRSTLINE                                           VARCHAR(2048) NULL, --第一行数据
REMARK                                              VARCHAR(512) NULL, --备注
DATABASE_CODE                                       CHAR(1) NOT NULL, --采集编码
UPDATETYPE                                          CHAR(1) NOT NULL, --更新方式
ODC_ID                                              BIGINT default 0 NULL, --对象采集id
CONSTRAINT OBJECT_COLLECT_TASK_PK PRIMARY KEY (OCS_ID)   );

--文件系统设置
DROP TABLE IF EXISTS FILE_COLLECT_SET ;
CREATE TABLE FILE_COLLECT_SET(
FCS_ID                                              BIGINT default 0 NOT NULL, --文件系统采集ID
AGENT_ID                                            BIGINT default 0 NULL, --Agent_id
FCS_NAME                                            VARCHAR(512) NOT NULL, --文件系统采集任务名称
HOST_NAME                                           VARCHAR(512) NULL, --主机名称
SYSTEM_TYPE                                         VARCHAR(512) NULL, --操作系统类型
IS_SENDOK                                           CHAR(1) NOT NULL, --是否设置完成并发送成功
IS_SOLR                                             CHAR(1) NOT NULL, --是否入solr
REMARK                                              VARCHAR(512) NULL, --备注
CONSTRAINT FILE_COLLECT_SET_PK PRIMARY KEY (FCS_ID)   );

--对象采集设置
DROP TABLE IF EXISTS OBJECT_COLLECT ;
CREATE TABLE OBJECT_COLLECT(
ODC_ID                                              BIGINT default 0 NOT NULL, --对象采集id
OBJECT_COLLECT_TYPE                                 CHAR(1) NOT NULL, --对象采集方式
OBJ_NUMBER                                          VARCHAR(200) NOT NULL, --对象采集设置编号
OBJ_COLLECT_NAME                                    VARCHAR(512) NOT NULL, --对象采集任务名称
SYSTEM_NAME                                         VARCHAR(512) NOT NULL, --操作系统类型
HOST_NAME                                           VARCHAR(512) NOT NULL, --主机名称
LOCAL_TIME                                          CHAR(20) NOT NULL, --本地系统时间
SERVER_DATE                                         CHAR(20) NOT NULL, --服务器日期
S_DATE                                              CHAR(8) NOT NULL, --开始日期
E_DATE                                              CHAR(8) NOT NULL, --结束日期
DATABASE_CODE                                       CHAR(1) NOT NULL, --采集编码
FILE_PATH                                           VARCHAR(512) NOT NULL, --采集文件路径
IS_DICTIONARY                                       CHAR(1) NOT NULL, --是否存在数据字典
IS_SENDOK                                           CHAR(1) NOT NULL, --是否设置完成并发送成功
DATA_DATE                                           CHAR(8) NOT NULL, --数据日期
FILE_SUFFIX                                         VARCHAR(100) NOT NULL, --文件后缀名
REMARK                                              VARCHAR(512) NULL, --备注
AGENT_ID                                            BIGINT default 0 NOT NULL, --Agent_id
CONSTRAINT OBJECT_COLLECT_PK PRIMARY KEY (ODC_ID)   );

--数据抽取定义
DROP TABLE IF EXISTS DATA_EXTRACTION_DEF ;
CREATE TABLE DATA_EXTRACTION_DEF(
DED_ID                                              BIGINT default 0 NOT NULL, --数据抽取定义主键
TABLE_ID                                            BIGINT default 0 NOT NULL, --表名ID
DATA_EXTRACT_TYPE                                   CHAR(1) NOT NULL, --数据文件源头
IS_HEADER                                           CHAR(1) NOT NULL, --是否需要表头
DATABASE_CODE                                       CHAR(1) NOT NULL, --数据抽取落地编码
ROW_SEPARATOR                                       VARCHAR(512) NULL, --行分隔符
DATABASE_SEPARATORR                                 VARCHAR(512) NULL, --列分割符
DBFILE_FORMAT                                       CHAR(1) default '1' NOT NULL, --数据落地格式
PLANE_URL                                           VARCHAR(512) NULL, --数据落地目录
FILE_SUFFIX                                         VARCHAR(80) NULL, --落地文件后缀名
IS_ARCHIVED                                         CHAR(1) default '0' NOT NULL, --是否转存
DED_REMARK                                          VARCHAR(512) NULL, --备注
CONSTRAINT DATA_EXTRACTION_DEF_PK PRIMARY KEY (DED_ID)   );

--数据存储层配置表
DROP TABLE IF EXISTS DATA_STORE_LAYER ;
CREATE TABLE DATA_STORE_LAYER(
DSL_ID                                              BIGINT default 0 NOT NULL, --存储层配置ID
DSL_NAME                                            VARCHAR(512) NOT NULL, --配置属性名称
STORE_TYPE                                          CHAR(1) NOT NULL, --存储类型
IS_HADOOPCLIENT                                     CHAR(1) NOT NULL, --是否支持外部表
DATABASE_NAME                                       VARCHAR(512) NULL, --数据库名称
DSL_SOURCE                                          CHAR(1) default '1' NOT NULL, --存储层是否为源库
DSL_GOAL                                            CHAR(1) default '1' NOT NULL, --存储层是否我目标库
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

--数据表原字段与目标表字段映射
DROP TABLE IF EXISTS tbcol_srctgt_map ;
CREATE TABLE tbcol_srctgt_map(
COLUMN_ID                                           BIGINT default 0 NOT NULL, --字段ID
DSL_ID                                              BIGINT default 0 NOT NULL, --存储层配置ID
COLUMN_TAR_TYPE                                     VARCHAR(512) NOT NULL, --目标字段类型
TSM_REMARK                                          VARCHAR(512) NULL, --备注
CONSTRAINT tbcol_srctgt_map_PK PRIMARY KEY (COLUMN_ID,DSL_ID)   );

