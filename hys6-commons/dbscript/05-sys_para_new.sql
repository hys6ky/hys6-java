truncate table sys_para;
-- 系统配置
INSERT INTO sys_para VALUES ('1', 'sys_Name', '海云数服', 'system.properties', '系统名称最多输入8个字');
INSERT INTO sys_para VALUES ('2', 'logo_img', 'hyrenlogo.png', 'system.properties', '系统logo图片名称');
INSERT INTO sys_para VALUES ('3', 'logo_depict', '博彦泓智科技（上海）有限公司', 'system.properties', 'logo描述信息');
INSERT INTO sys_para VALUES ('4', 'hyren_host', 'hdp007.beyondsoft.com', 'common.properties', 'HYREN服务器IP');
INSERT INTO sys_para VALUES ('5', 'hyren_port', '2000', 'system.properties', 'HYREN服务器接受进程端口');
INSERT INTO sys_para VALUES ('6', 'hyren_osname', 'linux', 'system.properties', 'HYREN服务器操作系统');
INSERT INTO sys_para VALUES ('7', 'system_language', 'zh_CN', 'system.properties', '服务器系统语言(默认zh_CN,例如英文；en_US)');
INSERT INTO sys_para VALUES ('8', 'sys_dateformat', 'yyyy-MM-dd', 'system.properties', '系统日期参数格式化定义');
INSERT INTO sys_para VALUES ('9', 'hyren_pwd', 'q1w2e3',  'system.properties', '系统默认海云用户的密码');
INSERT INTO sys_para VALUES ('10', 'is_save_operation_log', '0', 'system.properties', '是否保存操作日志 (1:是,0:否)');
INSERT INTO sys_para VALUES ('11', 'hyren_user', 'hyshf',  'system.properties', '系统默认海云用户名');
INSERT INTO sys_para VALUES ('12', 'time_out', '', 'system.properties', '用户停留页面的自动断开时间(单位/秒)或使用计算的供数 24 * 60 * 60的方式,不填写时默认是24小时');
INSERT INTO sys_para VALUES ('13', 'watermark', '海云数服', 'system.properties', '开始水印时显示的文字');
INSERT INTO sys_para VALUES ('14', 'is_watermark', '0', 'system.properties', '是否开启水印功能 1-表示开启,0表示不开启');
INSERT INTO sys_para VALUES ('15', 'privateKey', 'Rko68l71rLiQBRa5', 'system.properties', '数据加密时的密钥Key');
INSERT INTO sys_para VALUES ('16', 'pubKey', '525tPh0CM6t5pa5p', 'system.properties', '数据加密时的密钥Key');
INSERT INTO sys_para VALUES ('17', 'platform', 'normal', 'server.properties', 'normal:不做集群认证; cdh5:CDH平台认证; 不配置默认normal');
INSERT INTO sys_para VALUES ('18', 'showMonitor', 'false', 'system.properties', '是否在首页开启资源信息监控面板/Agent是否写入资源信息文件');
INSERT INTO sys_para VALUES ('19', 'reverseEngineeringUrl', 'http://172.168.0.90/', 'system.properties', '逆向工程地址');
-- 公共配置
INSERT INTO sys_para VALUES ('101', 'solrclassname', 'hyren.serv6.hadoop.commons.solr.impl.SolrOperatorImpl_7', 'common.properties', 'solr具体实现类全名hyren.serv6.hadoop.commons.solr.impl.SolrOperatorImpl_7');
INSERT INTO sys_para VALUES ('102', 'zkHost', 'hdp001.beyondsoft.com:2181,hdp002.beyondsoft.com:2181,hdp003.beyondsoft.com:2181/solr', 'common.properties', 'solr的zookeeper配置');
INSERT INTO sys_para VALUES ('103', 'collection', 'HrdsFullTextIndexing', 'common.properties', 'solr的collection');
INSERT INTO sys_para VALUES ('104', 'solr_bulk_submissions_num', '50000', 'common.properties', 'solr创建索引批量提交数');
INSERT INTO sys_para VALUES ('105', 'sftp_port', '22', 'common.properties', '部署时服务器ssh的端口');
INSERT INTO sys_para VALUES ('106', 'pathprefix', '/hrds', 'common.properties', 'hdfs上的目录规范,此目录必须为hdfs的根目录,即必须以"/"开头');
INSERT INTO sys_para VALUES ('107', 'file_collection_is_write_hadoop', '1', 'common.properties', '分结构化,文件采集是否写HDFS (1:是,0:否)');
INSERT INTO sys_para VALUES ('108', 'ocr_rpc_address', 'http://172.168.0.23:33333', 'server.properties', 'OCR图片文字识别的RPC服务器地址');
INSERT INTO sys_para VALUES ('109', 'use_ocr_rpc', '1', 'common.properties', '是否使用的是OCR RPC服务,默认否: 1 (1:是,0:否)');
INSERT INTO sys_para VALUES ('110', 'ocr_thread_pool', '4', 'common.properties', 'OCR 跑批线程池大小,默认 4 ');
INSERT INTO sys_para VALUES ('111', 'ocr_recognition_language', 'chi_sim', 'common.properties', 'ocr识别语言,eng(英文)或者chi_sim,默认chi_sim(chi_sim)');
-- 采集配置
INSERT INTO sys_para VALUES ('201', 'summary_volumn', '3', 'hrds_b.properties', '提取的摘要行数,默认 3 行');
INSERT INTO sys_para VALUES ('202', 'file_blocksize', '1024', 'hrds_b.properties', '写单个文件大小(单位:M),建议128M的整数倍,默认1G(2G则写成2048)');
INSERT INTO sys_para VALUES ('203', 'writemultiplefiles', '1', 'hrds_b.properties', '卸数时是否写多个文件(默认1 否)');
INSERT INTO sys_para VALUES ('204', 'determineFileChangesType', 'MD5', 'hrds_b.properties', '文件采集或者ftp采集比较文件是否变化的方式:MD5或fileAttr');
INSERT INTO sys_para VALUES ('205', 'singleAvroSize', '536870912', 'hrds_b.properties', '文件采集单个Avro的大小,默认512MB');
INSERT INTO sys_para VALUES ('206', 'thresholdFileSize', '26214400', 'hrds_b.properties', '文件采集大文件阈值,默认25MB');
INSERT INTO sys_para VALUES ('207', 'isAddOperateInfo', 'true', 'hrds_b.properties','采集是否添加操作时间、操作日期、操作人：true或false');
INSERT INTO sys_para VALUES ('208', 'isWriteDictionary', 'false', 'hrds_b.properties','每次数据库抽取结束是否写数据字典：true或false');
INSERT INTO sys_para VALUES ('209', 'agentpath', '/home/hyshf/HRSDATA/agent_download_package/collect/hrds_Agent.jar', 'hrds_b.properties', '下载agent的文件');
INSERT INTO sys_para VALUES ('210', 'agentDeployPath', '/home/hyshf/HRSDATA/agent_deploy_dir/hrsagent', 'hrds_b.properties', '海云用户默认的安装路径');
INSERT INTO sys_para VALUES ('211', 'agentConfigPath', '/home/hyshf/HRSDATA/agent_config', 'hrds_b.properties', 'agent接受页面配置文件和数据字典存放的顶层目录');
INSERT INTO sys_para VALUES ('212', 'dbBatch_row', '5000', 'hrds_b.properties', '数据采集batch默认提交的行数，这个可以在agent下手动调整，每个agent可以不一样');
INSERT INTO sys_para VALUES ('213', 'distributePath', '/data1/project/hyrenv6/hrsapp/dist_6_1/java/b/hyren-serv6-b-6.1.jar', 'hrds_b.properties', '数据分发程序jar包');

-- 接口配置
INSERT INTO sys_para VALUES ('301', 'restAuthority', '', 'hrds_g.properties', '对于SQL的字段是否使用字段验证');
INSERT INTO sys_para VALUES ('302', 'restFilePath', '/hyren/HRSDATA/bigdata/restFilePath', 'hrds_g.properties', '接口数据文件的存放路径');
INSERT INTO sys_para VALUES ('303', 'isRecordInterfaceLog', '1', 'hrds_g.properties','接口使用日志是否记录标志,1：是,0：否');
-- 集市&加工配置
INSERT INTO sys_para VALUES ('401', 'load.executor.count', '2', 'hrds_h.properties', '集市并发线程数量,当isconcurrent配置为0时,此配置生效,区间在1~4');
INSERT INTO sys_para VALUES ('402', 'mpp.batch.size', '5000', 'hrds_h.properties', 'spark通过jdbc写入到数据库,单批次提交的数量');
INSERT INTO sys_para VALUES ('403', 'isconcurrent', '0', 'hrds_h.properties', '集市是否采用多线程作业,0：是,1：否');
INSERT INTO sys_para VALUES ('404', 'sysnumber', '2000000', 'hrds_h.properties', '数据表记录阈值,超过时使用spark程序生成文件');
INSERT INTO sys_para VALUES ('405', 'spark_home', '/data/project/hyrenv5/hrsapp/dist/java/spark/', 'hrds_h.properties', '默认spark_home');
INSERT INTO sys_para VALUES ('406', 'sparkSubmit', 'local', 'hrds_h.properties', '加工作业的执行方式: yarn/local/local[*]中的一种. yarn: 集群模式. local: 本地单线程. local[*]: 本地多线程');
INSERT INTO sys_para VALUES ('407', 'spark_jar_name', 'hyren-serv6-h-6.0.jar', 'hrds_h.properties', '加工Spark作业的执行jar包名称');
INSERT INTO sys_para VALUES ('408', 'process.deployment.dir', '/data/project/hyren/hrsapp/dist_6/java/h', 'hrds_h.properties', '加工程序部署目录,默认是加工项目部署目录');

-- 数据管控配置
INSERT INTO sys_para VALUES ('501', 'predict_address', 'http://192.168.1.101:38081/predict', 'hrds_k.properties','数据对标-表结构对标预测地址');
INSERT INTO sys_para VALUES ('502', 'algorithms_result_root_path', 'file:///D:/algorithms_result_root_path/', 'hrds_k.properties','数据对标,表函数依赖和主键分析输出结果的根目录');
INSERT INTO sys_para VALUES ('503', 'algorithms_python_serve', 'http://127.0.0.1:33333/', 'hrds_k.properties','数据对标,调用python服务的url');
INSERT INTO sys_para VALUES ('504', 'algorithms_spark_classpath', '.:/opt/hrsapp/dist/java/K/hrds_K.jar:/opt/hrsapp/dist/java/lib/*:/opt/hrsapp/dist/java/K/resources/:/opt/hrsapp/dist/java/spark/jars/*', 'hrds_k.properties','数据对标算主键函数依赖执行spark程序所依赖的classpath');
INSERT INTO sys_para VALUES ('505', 'neo4jUri', 'bolt://172.168.0.102:7687', 'hrds_k.properties','neo4j图数据库连接');
INSERT INTO sys_para VALUES ('506', 'neo4j_user', 'neo4j', 'hrds_k.properties','neo4j图数据库用户名');
INSERT INTO sys_para VALUES ('507', 'neo4j_password', 'Neo4j', 'hrds_k.properties','neo4j图数据库密码');
INSERT INTO sys_para VALUES ('508', 'spark.driver.extraJavaOptions', '-Xss512m -Xmx10240m', 'hrds_k.properties','数据对标的spark程序占用内存大小');
INSERT INTO sys_para VALUES ('509', 'anomalyDataDetection_python_serve', 'http://172.168.0.60:33335/outlier_detect_main', 'hrds_k.properties', '异常数据检测python服务url');

-- 作业配置
INSERT INTO sys_para VALUES ('601', 'etlDeployPath', '/home/hyshf/HRSDATA/agent_deploy_dir/etlagent','hrds_c.properties', '作业调度默认部署路径');
INSERT INTO sys_para VALUES ('602', 'controlPath', '/home/hyshf/HRSDATA/agent_download_package/etl/hrds_Control.jar','hrds_c.properties','CONTROL程序jar包地址');
INSERT INTO sys_para VALUES ('603', 'triggerPath', '/home/hyshf/HRSDATA/agent_download_package/etl/hrds_Trigger.jar', 'hrds_c.properties','TRIGGER程序jar包地址');

--流项目配置
INSERT INTO sys_para VALUES ('703', 'kafka_zk_address', '127.0.0.1:2181', 'hrds_j.properties','kafka的zookeeper服务地址');
INSERT INTO sys_para VALUES ('704', 'kafkaAgentPath','/home/hyshf/HRSDATA/agent_download_package/kafka/kafka_Agent.jar', 'hrds_j.properties','kafka下载agent的文件');
INSERT INTO sys_para VALUES ('705', 'streamingProJar','/home/hyshf/HRSDATA/agent_download_package/kafka/hrds-streaming.jar', 'hrds_j.properties','hrds_streaming的jar包');
INSERT INTO sys_para VALUES ('706', 'StreamProExecut','/home/hyshf/HRSDATA/agent_download_package/kafka/StreamProExecut', 'hrds_j.properties','执行hrds_Streaming的jar包脚本文件');


