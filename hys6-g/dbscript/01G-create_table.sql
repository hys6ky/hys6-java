--接口信息表
DROP TABLE IF EXISTS INTERFACE_INFO ;
CREATE TABLE INTERFACE_INFO(
INTERFACE_ID                                        BIGINT default 0 NOT NULL, --接口ID
URL                                                 VARCHAR(512) NOT NULL, --请求地址
INTERFACE_NAME                                      VARCHAR(512) NOT NULL, --接口名称
INTERFACE_TYPE                                      CHAR(1) NOT NULL, --接口类型
INTERFACE_STATE                                     CHAR(1) NOT NULL, --接口状态
INTERFACE_CODE                                      VARCHAR(100) NOT NULL, --接口代码
INTERFACE_NOTE                                      VARCHAR(512) NULL, --备注
USER_ID                                             BIGINT default 0 NOT NULL, --用户ID
CONSTRAINT INTERFACE_INFO_PK PRIMARY KEY (INTERFACE_ID)   );

--接口使用信息表
DROP TABLE IF EXISTS INTERFACE_USE ;
CREATE TABLE INTERFACE_USE(
INTERFACE_USE_ID                                    BIGINT default 0 NOT NULL, --接口使用ID
URL                                                 VARCHAR(512) NOT NULL, --请求地址
INTERFACE_NAME                                      VARCHAR(512) NOT NULL, --接口名称
INTERFACE_CODE                                      VARCHAR(100) NOT NULL, --接口代码
THEIR_TYPE                                          CHAR(1) NOT NULL, --接口所属类型
USER_NAME                                           VARCHAR(512) NOT NULL, --用户名称
CLASSIFY_NAME                                       VARCHAR(512) NULL, --分类名称
CREATE_ID                                           BIGINT default 0 NOT NULL, --创建者ID
USE_STATE                                           CHAR(1) NOT NULL, --使用状态
START_USE_DATE                                      CHAR(8) NOT NULL, --开始使用日期
USE_VALID_DATE                                      CHAR(8) NOT NULL, --使用有效日期
INTERFACE_NOTE                                      VARCHAR(512) NULL, --备注
INTERFACE_ID                                        BIGINT default 0 NOT NULL, --接口ID
USER_ID                                             BIGINT default 0 NOT NULL, --使用接口用户
CONSTRAINT INTERFACE_USE_PK PRIMARY KEY (INTERFACE_USE_ID)   );

--表使用信息表
DROP TABLE IF EXISTS TABLE_USE_INFO ;
CREATE TABLE TABLE_USE_INFO(
USE_ID                                              BIGINT default 0 NOT NULL, --表使用ID
SYSREG_NAME                                         VARCHAR(512) NOT NULL, --系统登记表名
ORIGINAL_NAME                                       VARCHAR(512) NOT NULL, --原始文件名称
TABLE_BLSYSTEM                                      CHAR(3) NOT NULL, --数据表所属系统
TABLE_NOTE                                          VARCHAR(512) NULL, --表说明
USER_ID                                             BIGINT default 0 NOT NULL, --用户ID
CONSTRAINT TABLE_USE_INFO_PK PRIMARY KEY (USE_ID)   );

--系统登记表参数信息
DROP TABLE IF EXISTS SYSREG_PARAMETER_INFO ;
CREATE TABLE SYSREG_PARAMETER_INFO(
PARAMETER_ID                                        BIGINT default 0 NOT NULL, --参数ID
TABLE_EN_COLUMN                                     VARCHAR(512) NOT NULL, --表列英文名称
TABLE_CH_COLUMN                                     VARCHAR(256) NOT NULL, --表列中文名称
IS_FLAG                                             CHAR(1) NOT NULL, --是否可用
REMARK                                              VARCHAR(512) NULL, --备注
USE_ID                                              BIGINT default 0 NOT NULL, --表使用ID
USER_ID                                             BIGINT default 0 NOT NULL, --用户ID
CONSTRAINT SYSREG_PARAMETER_INFO_PK PRIMARY KEY (PARAMETER_ID)   );

--接口文件生成信息表
DROP TABLE IF EXISTS INTERFACE_FILE_INFO ;
CREATE TABLE INTERFACE_FILE_INFO(
FILE_ID                                             VARCHAR(40) NOT NULL, --文件ID
USER_ID                                             BIGINT default 0 NOT NULL, --用户ID
FILE_PATH                                           VARCHAR(512) NOT NULL, --文件路径
DATA_CLASS                                          VARCHAR(10) NOT NULL, --输出数据类型
DATA_OUTPUT                                         VARCHAR(20) NOT NULL, --数据数据形式
REMARK                                              VARCHAR(512) NULL, --备注
CONSTRAINT INTERFACE_FILE_INFO_PK PRIMARY KEY (FILE_ID)   );

--接口使用信息日志表
DROP TABLE IF EXISTS INTERFACE_USE_LOG ;
CREATE TABLE INTERFACE_USE_LOG(
LOG_ID                                              BIGINT default 0 NOT NULL, --日志ID
INTERFACE_NAME                                      VARCHAR(512) NOT NULL, --接口名称
REQUEST_STATE                                       VARCHAR(200) NOT NULL, --请求状态
RESPONSE_TIME                                       DECIMAL(10) NOT NULL, --响应时间
BROWSER_TYPE                                        VARCHAR(512) NULL, --浏览器类型
BROWSER_VERSION                                     VARCHAR(512) NULL, --浏览器版本
SYSTEM_TYPE                                         VARCHAR(512) NULL, --系统类型
REQUEST_MODE                                        VARCHAR(512) NULL, --请求方式
REMOTEADDR                                          VARCHAR(512) NULL, --客户端的IP
PROTOCOL                                            VARCHAR(512) NULL, --超文本传输协议版本
REQUEST_INFO                                        VARCHAR(6000) NULL, --请求信息
REQUEST_STIME                                       VARCHAR(512) NULL, --请求起始时间
REQUEST_ETIME                                       VARCHAR(512) NULL, --请求结束时间
REQUEST_TYPE                                        VARCHAR(512) NULL, --请求类型
INTERFACE_USE_ID                                    BIGINT default 0 NOT NULL, --接口使用ID
USER_ID                                             BIGINT default 0 NOT NULL, --用户ID
USER_NAME                                           VARCHAR(512) NULL, --用户名称
CONSTRAINT INTERFACE_USE_LOG_PK PRIMARY KEY (LOG_ID)   );

--用户信息表
DROP TABLE IF EXISTS SYS_USER ;
CREATE TABLE SYS_USER(
USER_ID                                             BIGINT default 0 NOT NULL, --用户ID
CREATE_ID                                           BIGINT default 0 NOT NULL, --建立用户ID
DEP_ID                                              BIGINT default 0 NOT NULL, --部门ID
ROLE_ID                                             BIGINT default 0 NOT NULL, --角色ID
USER_NAME                                           VARCHAR(512) NOT NULL, --用户名称
USER_PASSWORD                                       VARCHAR(100) NOT NULL, --用户密码
USER_EMAIL                                          VARCHAR(100) NULL, --邮箱
USER_MOBILE                                         VARCHAR(20) NULL, --移动电话
LOGIN_IP                                            VARCHAR(50) NULL, --登录IP
LOGIN_DATE                                          CHAR(8) NULL, --最后登录时间
USER_STATE                                          CHAR(1) NOT NULL, --用户状态
CREATE_DATE                                         CHAR(8) NOT NULL, --创建日期
CREATE_TIME                                         CHAR(6) NULL, --创建时间
UPDATE_DATE                                         CHAR(8) NULL, --更新日期
UPDATE_TIME                                         CHAR(6) NULL, --更新时间
USER_REMARK                                         VARCHAR(512) NULL, --备注
TOKEN                                               VARCHAR(40) default 0 NOT NULL, --token
VALID_TIME                                          VARCHAR(40) default 0 NOT NULL, --token有效时间
IS_LOGIN                                            CHAR(1) default '0' NOT NULL, --是否已登录
LIMITMULTILOGIN                                     CHAR(1) default '1' NOT NULL, --是否允许多人登录
CONSTRAINT SYS_USER_PK PRIMARY KEY (USER_ID)   );

--部门信息表
DROP TABLE IF EXISTS DEPARTMENT_INFO ;
CREATE TABLE DEPARTMENT_INFO(
DEP_ID                                              BIGINT default 0 NOT NULL, --部门ID
DEP_NAME                                            VARCHAR(512) NOT NULL, --部门名称
CREATE_DATE                                         CHAR(8) NOT NULL, --创建日期
CREATE_TIME                                         CHAR(6) NOT NULL, --创建时间
DEP_REMARK                                          VARCHAR(512) NULL, --备注
CONSTRAINT DEPARTMENT_INFO_PK PRIMARY KEY (DEP_ID)   );

