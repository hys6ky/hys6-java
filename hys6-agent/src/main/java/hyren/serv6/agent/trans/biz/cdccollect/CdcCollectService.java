package hyren.serv6.agent.trans.biz.cdccollect;

import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.Validator;
import hyren.serv6.agent.run.FlinkKafkaConsumerJobCommand;
import hyren.serv6.agent.run.FlinkKafkaProducerJobCommand;
import hyren.serv6.agent.run.flink.FlinkErrorParams;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

@Slf4j
@Service
public class CdcCollectService {

    private static String PRODUCER_MAIN_CLASS_NAME = FlinkKafkaProducerJobCommand.class.getSimpleName();

    private static String CONSUMER_MAIN_CLASS_NAME = FlinkKafkaConsumerJobCommand.class.getSimpleName();

    private static String CONSUMER_SH_PATH = System.getProperty("user.dir") + File.separator + Constant.COLLECT_CDC_JOB_CONSUMER_COMMAND;

    private static String PRODUCER_SH_PATH = System.getProperty("user.dir") + File.separator + Constant.COLLECT_CDC_JOB_PRODUCER_COMMAND;

    private static int DEFAULTRUNCODE = 0;

    public String execute(Long taskId, String tableNames) {
        String logIdentification = DateUtil.getTimestamp();
        Validator.notNull(taskId, "任务 id 不能为空");
        Validator.notEmpty(tableNames, "采集表不能为空");
        String[] tables = tableNames.split(",");
        File consumerSh = new File(CONSUMER_SH_PATH);
        File producerSh = new File(PRODUCER_SH_PATH);
        if (!consumerSh.isFile()) {
            throw new BusinessException("未找到脚本文件：" + CONSUMER_SH_PATH);
        }
        if (!producerSh.isFile()) {
            throw new BusinessException("未找到脚本文件：" + PRODUCER_SH_PATH);
        }
        Map<String, Long> tableMap = new HashMap<String, Long>();
        for (String table : tables) {
            log.info("********cdc_agent_consumer_init: {} {}*********", taskId, table);
            String[] cCommand = { "bash", CONSUMER_SH_PATH, taskId.toString(), table, logIdentification };
            ProcessBuilder cProcessBuilder = new ProcessBuilder(cCommand);
            log.info("********cdc_agent_consumer_run_command: {}*********", Arrays.toString(cCommand));
            int cRunCode = DEFAULTRUNCODE;
            try {
                Process cProcess = cProcessBuilder.start();
                cRunCode = cProcess.waitFor();
                log.info("********cdc_agent_consumer_start_runCode: {}*********", cRunCode);
            } catch (IOException e) {
                log.info("执行命令失败", e);
            } catch (InterruptedException e) {
                log.error("获取 runCode 失败", e);
            } catch (Exception e) {
                log.error("执行 consumer 脚本未知异常", e);
            }
            if (cRunCode == FlinkErrorParams.FLINK_CDC_CONSUMER_STARTED.getCode()) {
                Long pId = this.getConsumerPid(taskId, table);
                if (pId == null) {
                    this.stopConsumerByTable(tableMap);
                    throw new BusinessException("同步任务未启动");
                } else {
                    tableMap.put(table, pId);
                }
            } else if (cRunCode == FlinkErrorParams.FLINK_CDC_CONSUMER_NO_JDBC.getCode()) {
                log.info("********cdc_agent_consumer_start_end: no_jdbc {} {}*********", taskId, table);
            } else {
                log.info("********cdc_agent_consumer_start_failed: {} {} {}*********", taskId, table, cRunCode);
                this.stopConsumerByTable(tableMap);
                if (cRunCode == FlinkErrorParams.FLINK_CDC_CONSUMER_MAIN_PARAMS_ERROR.getCode()) {
                    throw new BusinessException("consumer main 方法获取参数异常");
                } else if (cRunCode == FlinkErrorParams.FLINK_CDC_CONSUMER_ERROR.getCode()) {
                    throw new BusinessException("consumer 执行异常");
                } else if (cRunCode == FlinkErrorParams.FLINK_CDC_JDBC_ERROR.getCode()) {
                    throw new BusinessException("consumer jdbc 链接异常");
                } else if (cRunCode == FlinkErrorParams.FLINK_CDC_CONSUMER_RUN_TIMEOUT.getCode()) {
                    throw new BusinessException("consumer 启动超时");
                } else if (cRunCode == FlinkErrorParams.FLINK_CDC_CONSUMER_GET_PARAMS_FAIL.getCode()) {
                    throw new BusinessException("consumer 获取远程数据失败");
                } else {
                    throw new BusinessException("启动同步任务失败");
                }
            }
        }
        log.info("********cdc_agent_producer_init: {} {}*********", taskId, tableNames);
        String[] pCommand = { "bash", PRODUCER_SH_PATH, taskId.toString(), tableNames, logIdentification };
        ProcessBuilder pProcessBuilder = new ProcessBuilder(pCommand);
        log.info("********cdc_agent_producer_start_sh: {}*********", Arrays.toString(pCommand));
        int pRunCode = DEFAULTRUNCODE;
        try {
            Process pProcess = pProcessBuilder.start();
            pRunCode = pProcess.waitFor();
            log.info("********cdc_agent_procuder_start_runCode: {}*********", pRunCode);
        } catch (IOException e) {
            log.error("执行命令失败", e);
        } catch (InterruptedException e) {
            log.error("获取 runCode 失败", e);
        } catch (Exception e) {
            log.error("执行 producer 脚本未知异常", e);
        }
        if (pRunCode == FlinkErrorParams.FLINK_CDC_PRODUCER_STARTED.getCode()) {
            return "执行成功";
        } else {
            if (pRunCode == DEFAULTRUNCODE) {
                throw new BusinessException("请检查采集脚本");
            } else if (pRunCode == FlinkErrorParams.FLINK_CDC_PRODUCER_ERROR.getCode()) {
                throw new BusinessException("producer 启动失败");
            } else if (pRunCode == FlinkErrorParams.FLINK_CDC_PRODUCER_RUN_TIMEOUT.getCode()) {
                throw new BusinessException("producer 启动超时");
            } else if (pRunCode == FlinkErrorParams.FLINK_CDC_PRODUCER_GET_PARAMS_FAIL.getCode()) {
                throw new BusinessException("producer 获取远程数据失败");
            } else if (pRunCode == FlinkErrorParams.FLINK_CDC_PRODUCER_CHECKPOINT_URL_IS_ERROR.getCode()) {
                throw new BusinessException("producer 检查点地址错误");
            } else if (pRunCode == FlinkErrorParams.FLINK_CDC_PRODUCER_MAIN_PARAMS_ERROR.getCode()) {
                throw new BusinessException("producer main 方法获取参数异常");
            } else {
                throw new BusinessException("启动采集任务失败:(" + taskId + "," + tableNames);
            }
        }
    }

    private void stopConsumerByTable(Map<String, Long> tableMap) {
        for (Entry<String, Long> entry : tableMap.entrySet()) {
            try {
                String command = "kill -9 " + entry.getValue();
                log.info("stop consumer :" + command);
                Process exec = Runtime.getRuntime().exec(command);
            } catch (IOException e) {
                log.info("sto consumer error:", e);
            }
        }
    }

    public Map<String, Boolean> getStatus(Long taskId, String tableName) {
        String consumer = "CONSUMER";
        String producer = "PRODUCER";
        Map<String, Boolean> map = new HashMap<String, Boolean>();
        map.put(consumer, false);
        map.put(producer, false);
        try {
            String line;
            String[] pCommand = new String[] { "sh", "-c", "ps -ef | grep " + CONSUMER_MAIN_CLASS_NAME + " | grep " + taskId + " | grep " + tableName };
            ProcessBuilder consumerProcessBuilder = new ProcessBuilder(pCommand);
            Process consumerProcess = consumerProcessBuilder.start();
            BufferedReader consumerReader = new BufferedReader(new InputStreamReader(consumerProcess.getInputStream()));
            while ((line = consumerReader.readLine()) != null) {
                if (line.contains(taskId.toString()) && line.contains(tableName)) {
                    map.put(consumer, true);
                    break;
                }
            }
            String[] cCommand = new String[] { "sh", "-c", "ps -ef | grep " + PRODUCER_MAIN_CLASS_NAME + " | grep " + taskId + " | grep " + tableName };
            ProcessBuilder producerProcessBuilder = new ProcessBuilder(cCommand);
            Process producerProcess = producerProcessBuilder.start();
            BufferedReader producerReader = new BufferedReader(new InputStreamReader(producerProcess.getInputStream()));
            while ((line = producerReader.readLine()) != null) {
                if (line.contains(taskId.toString()) && line.contains(tableName)) {
                    map.put(producer, true);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException(e.getMessage());
        }
        return map;
    }

    public Long getConsumerPid(Long taskId, String tableName) {
        try {
            String line;
            String[] command = { "sh", "-c", "ps -ef | grep " + CONSUMER_MAIN_CLASS_NAME + " | grep " + taskId + " | grep " + tableName };
            log.info(Arrays.toString(command));
            ProcessBuilder producerProcessBuilder = new ProcessBuilder(command);
            Process producerProcess = producerProcessBuilder.start();
            BufferedReader producerReader = new BufferedReader(new InputStreamReader(producerProcess.getInputStream()));
            while ((line = producerReader.readLine()) != null) {
                if (line.contains(taskId.toString()) && line.contains(tableName)) {
                    log.info(line);
                    String pIdStr = line.trim().split("\\s+")[1];
                    return Long.parseLong(pIdStr);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException(e.getMessage());
        }
        return null;
    }

    public Long getProducerPid(Long taskId, String tableName) {
        try {
            String line;
            String[] command = { "sh", "-c", "ps -ef | grep " + PRODUCER_MAIN_CLASS_NAME + " | grep " + taskId + " | grep " + tableName };
            log.info(Arrays.toString(command));
            ProcessBuilder producerProcessBuilder = new ProcessBuilder(command);
            Process producerProcess = producerProcessBuilder.start();
            BufferedReader producerReader = new BufferedReader(new InputStreamReader(producerProcess.getInputStream()));
            while ((line = producerReader.readLine()) != null) {
                if (line.contains(taskId.toString()) && line.contains(tableName)) {
                    log.info(line);
                    String pIdStr = line.trim().split("\\s+")[1];
                    return Long.parseLong(pIdStr);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException(e.getMessage());
        }
        return null;
    }

    public Map<Long, Boolean> status(long[] pids) {
        Map<Long, Boolean> map = new HashMap<Long, Boolean>();
        for (Long pid : pids) {
            map.put(pid, this.status(pid));
        }
        return map;
    }

    public boolean status(long pId) {
        try {
            String command = "ps -p " + pId;
            Process process = Runtime.getRuntime().exec(command);
            log.info("******status command:{}******" + command);
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains(String.valueOf(pId))) {
                    return true;
                }
            }
            process.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public Boolean abort(long[] pIds) {
        for (Long pid : pIds) {
            if (this.status(pid)) {
                try {
                    String command = "kill -9 " + pid;
                    Process exec = Runtime.getRuntime().exec(command);
                    log.info("******abort command:{}******" + command);
                } catch (IOException e) {
                    log.error("执行中止命令失败：", e);
                    throw new BusinessException(e.getMessage());
                }
            } else {
                log.info("********abort params does not exist:{}********", pid);
            }
        }
        return true;
    }

    public Boolean startCollect(Long taskId, String tableNames) {
        String logIdentification = DateUtil.getTimestamp();
        log.info("********cdc_agent_producer_init: {} {}*********", taskId, tableNames);
        String[] pCommand = { "bash", PRODUCER_SH_PATH, taskId.toString(), tableNames, logIdentification };
        ProcessBuilder pProcessBuilder = new ProcessBuilder(pCommand);
        log.info("********cdc_agent_producer_start_sh: {}*********", Arrays.toString(pCommand));
        int pRunCode = DEFAULTRUNCODE;
        try {
            Process pProcess = pProcessBuilder.start();
            pRunCode = pProcess.waitFor();
            log.info("********cdc_agent_procuder_start_runCode: {}*********", pRunCode);
        } catch (IOException e) {
            log.error("执行命令失败", e);
        } catch (InterruptedException e) {
            log.error("获取 runCode 失败", e);
        } catch (Exception e) {
            log.error("执行 producer 脚本未知异常", e);
        }
        if (pRunCode == FlinkErrorParams.FLINK_CDC_PRODUCER_STARTED.getCode()) {
            return true;
        } else {
            if (pRunCode == DEFAULTRUNCODE) {
                throw new BusinessException("请检查采集脚本");
            } else if (pRunCode == FlinkErrorParams.FLINK_CDC_PRODUCER_ERROR.getCode()) {
                throw new BusinessException("producer 启动失败");
            } else if (pRunCode == FlinkErrorParams.FLINK_CDC_PRODUCER_RUN_TIMEOUT.getCode()) {
                throw new BusinessException("producer 启动超时");
            } else if (pRunCode == FlinkErrorParams.FLINK_CDC_PRODUCER_GET_PARAMS_FAIL.getCode()) {
                throw new BusinessException("producer 获取远程数据失败");
            } else if (pRunCode == FlinkErrorParams.FLINK_CDC_PRODUCER_CHECKPOINT_URL_IS_ERROR.getCode()) {
                throw new BusinessException("producer 检查点地址错误");
            } else if (pRunCode == FlinkErrorParams.FLINK_CDC_PRODUCER_MAIN_PARAMS_ERROR.getCode()) {
                throw new BusinessException("producer main 方法获取参数异常");
            } else {
                throw new BusinessException("启动采集任务失败:(" + taskId + "," + tableNames);
            }
        }
    }

    public Boolean startSync(Long taskId, String[] tableNames) {
        String logIdentification = DateUtil.getTimestamp();
        Map<String, Long> tableMap = new HashMap<String, Long>();
        for (String table : tableNames) {
            log.info("********cdc_agent_consumer_init: {} {}*********", taskId, table);
            String[] cCommand = { "bash", CONSUMER_SH_PATH, taskId.toString(), table, logIdentification };
            ProcessBuilder cProcessBuilder = new ProcessBuilder(cCommand);
            log.info("********cdc_agent_consumer_run_command: {}*********", Arrays.toString(cCommand));
            int cRunCode = DEFAULTRUNCODE;
            try {
                Process cProcess = cProcessBuilder.start();
                cRunCode = cProcess.waitFor();
                log.info("********cdc_agent_consumer_start_runCode: {}*********", cRunCode);
            } catch (IOException e) {
                log.info("执行命令失败", e);
            } catch (InterruptedException e) {
                log.error("获取 runCode 失败", e);
            } catch (Exception e) {
                log.error("执行 consumer 脚本未知异常", e);
            }
            if (cRunCode == FlinkErrorParams.FLINK_CDC_CONSUMER_STARTED.getCode()) {
                Long pId = this.getConsumerPid(taskId, table);
                if (pId == null) {
                    this.stopConsumerByTable(tableMap);
                    throw new BusinessException("同步任务未启动");
                } else {
                    tableMap.put(table, pId);
                }
            } else if (cRunCode == FlinkErrorParams.FLINK_CDC_CONSUMER_NO_JDBC.getCode()) {
                log.info("********cdc_agent_consumer_start_end: no_jdbc{} {}*********", taskId, table);
            } else {
                log.info("********cdc_agent_consumer_start_failed: {} {} {}*********", taskId, table, cRunCode);
                this.stopConsumerByTable(tableMap);
                if (cRunCode == FlinkErrorParams.FLINK_CDC_CONSUMER_MAIN_PARAMS_ERROR.getCode()) {
                    throw new BusinessException("consumer main 方法获取参数异常");
                } else if (cRunCode == FlinkErrorParams.FLINK_CDC_CONSUMER_ERROR.getCode()) {
                    throw new BusinessException("consumer 执行异常");
                } else if (cRunCode == FlinkErrorParams.FLINK_CDC_JDBC_ERROR.getCode()) {
                    throw new BusinessException("consumer jdbc 链接异常");
                } else if (cRunCode == FlinkErrorParams.FLINK_CDC_CONSUMER_RUN_TIMEOUT.getCode()) {
                    throw new BusinessException("consumer 启动超时");
                } else if (cRunCode == FlinkErrorParams.FLINK_CDC_CONSUMER_GET_PARAMS_FAIL.getCode()) {
                    throw new BusinessException("consumer 获取远程数据失败");
                } else {
                    throw new BusinessException("启动同步任务失败");
                }
            }
        }
        return true;
    }
}
