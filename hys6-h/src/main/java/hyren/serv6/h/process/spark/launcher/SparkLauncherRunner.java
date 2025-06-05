package hyren.serv6.h.process.spark.launcher;

import fd.ng.core.utils.FileUtil;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.constant.CommonVariables;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import hyren.serv6.h.process.args.HandleArgs;
import hyren.serv6.h.process.utils.ProcessJobRunStatusEnum;
import hyren.serv6.h.process.utils.ProcessTableConfBeanUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SparkLauncherRunner {

    private static final String SPARK_MAIN_CLASS = "hyren.serv6.h.process.spark.run.SparkRunJobMain";

    private static final SparkLauncherParam sparkLauncherParam;

    private static final String PRINCIPAL;

    private static final String KEYTAB;

    private static final String QUEUE;

    private static final int CONNECTED_TIMEOUT;

    static {
        try {
            Class<?> aClass = Class.forName(SPARK_MAIN_CLASS);
            sparkLauncherParam = new SparkLauncherParam();
            sparkLauncherParam.setSparkHome(CommonVariables.SPARK_HOME);
            sparkLauncherParam.setMainClass(SPARK_MAIN_CLASS);
            String sparkMaster = PropertyParaValue.getString("spark.master", "local");
            sparkLauncherParam.setMaster(sparkMaster);
            if ("local".equalsIgnoreCase(sparkMaster)) {
                sparkLauncherParam.setDeployMode("client");
            } else if ("yarn".equalsIgnoreCase(sparkMaster)) {
                sparkLauncherParam.setDeployMode("cluster");
            } else {
                throw new AppSystemException("暂不支持是的提交模式! " + sparkMaster);
            }
            sparkLauncherParam.setSparkEnableKerberos(PropertyParaValue.getBoolean("spark.kerberos.enable", Boolean.FALSE));
            PRINCIPAL = PropertyParaValue.getString("spark.kerberos.principal", "hyren@bank.com");
            KEYTAB = PropertyParaValue.getString("spark.kerberos.keytab", "./conf/hyren.keytab");
            sparkLauncherParam.setJarPath("hyren-serv6-h-process.jar");
            QUEUE = PropertyParaValue.getString("spark.yarn.queue", "default");
            Map<String, String> otherParams = new HashMap<>();
            if ("yarn".equalsIgnoreCase(sparkLauncherParam.getMaster()) && !"default".equalsIgnoreCase(QUEUE)) {
                otherParams.put("spark.yarn.queue", QUEUE);
            }
            sparkLauncherParam.setOtherParams(otherParams);
            CONNECTED_TIMEOUT = PropertyParaValue.getInt("spark.launcher.connected.timeout", 30);
        } catch (Exception e) {
            throw new AppSystemException("主类 [ " + SPARK_MAIN_CLASS + " ] 不存在" + e);
        }
    }

    public static ProcessJobRunStatusEnum runJob(HandleArgs handleArgs) {
        final ProcessJobRunStatusEnum[] jobRunStatusEnums = new ProcessJobRunStatusEnum[1];
        String serializeFileName = handleArgs.getModuleTableId_JobTableId();
        String serializeFilePath = ProcessTableConfBeanUtil.PROCESS_SERIALIZATION_FILE_DIR + serializeFileName;
        String tableName = handleArgs.getTableName();
        String appName = "Spark_Process_" + tableName;
        File appRunLogDir = new File(System.getProperty("user.dir") + "/sparkAppRunLogs");
        try {
            FileUtil.forceMkdir(appRunLogDir);
        } catch (IOException e) {
            throw new BusinessException(String.format("初始化app运行日志目录 %s 失败! 异常: %s", appRunLogDir.getAbsolutePath(), e));
        }
        SparkLauncher launcher = new SparkLauncher().setSparkHome(sparkLauncherParam.getSparkHome()).setMaster(sparkLauncherParam.getMaster()).setDeployMode(sparkLauncherParam.getDeployMode()).setAppResource(sparkLauncherParam.getJarPath()).setMainClass(sparkLauncherParam.getMainClass()).setConf("spark.driver.memory", sparkLauncherParam.getDriverMemory()).setConf("spark.driver.allowMultipleContexts", sparkLauncherParam.getAllowMultipleContexts()).setConf("spark.executor.memory", sparkLauncherParam.getExecutorMemory()).setConf("spark.executor.cores", sparkLauncherParam.getExecutorCores()).setPropertiesFile("resources/spark/spark.conf").addFile("resources/*").addFile("resources/spark/*").addFile(serializeFilePath).addJar("./process-run-jars/hyren-*").addJar("../jdbc/*").setAppName(appName).addAppArgs(serializeFileName, handleArgs.toString()).redirectOutput(new File(appRunLogDir.getAbsolutePath() + File.separator + tableName + ".out")).redirectError(new File(appRunLogDir.getAbsolutePath() + File.separator + tableName + "_err.out")).setVerbose(true);
        Map<String, String> otherParams = sparkLauncherParam.getOtherParams();
        if (otherParams != null && !otherParams.isEmpty()) {
            log.info("开始设置spark运行参数:{}", JsonUtil.toJson(otherParams));
            for (Map.Entry<String, String> otherParam : otherParams.entrySet()) {
                log.debug("---- 参数名: {} 参数值: {}", otherParam.getKey(), otherParam.getValue());
                launcher.setConf(otherParam.getKey(), otherParam.getValue());
            }
        }
        if (sparkLauncherParam.getSparkEnableKerberos()) {
            log.info(String.format("开始设置Spark认证参数: " + Constant.__PRINCIPAL + " %s " + Constant.__KEYTAB + " %s", PRINCIPAL, KEYTAB));
            launcher.addAppArgs(Constant.__PRINCIPAL, PRINCIPAL);
            launcher.addAppArgs(Constant.__KEYTAB, KEYTAB);
        }
        log.info("参数设置完成, 开始提交spark任务: " + appName);
        try {
            final String[] appId = { "" };
            final int[] connected_time = { 0 };
            CountDownLatch countDownLatch = new CountDownLatch(1);
            SparkAppHandle handle = launcher.startApplication(new SparkAppHandle.Listener() {

                @Override
                public void stateChanged(SparkAppHandle sparkAppHandle) {
                    SparkAppHandle.State state = sparkAppHandle.getState();
                    appId[0] = sparkAppHandle.getAppId();
                    if (sparkAppHandle.getState().isFinal())
                        countDownLatch.countDown();
                    switch(state) {
                        case UNKNOWN:
                            log.info("Spark任务: {} 应用程序未报告!", appName);
                            break;
                        case CONNECTED:
                            jobRunStatusEnums[0] = ProcessJobRunStatusEnum.CONNECTED;
                            log.info("Spark任务: {}, 应用程序已连接!", appName);
                            break;
                        case SUBMITTED:
                            jobRunStatusEnums[0] = ProcessJobRunStatusEnum.SUBMITTED;
                            log.info("Spark任务: {}, appId: {}, 应用程序已提交!", appName, appId[0]);
                            break;
                        case RUNNING:
                            jobRunStatusEnums[0] = ProcessJobRunStatusEnum.RUNNING;
                            log.info("Spark任务: {}, appId: {}, 应用程序正在运行!", appName, appId[0]);
                            break;
                        case FINISHED:
                            log.info("Spark任务: {}, appId: {}, 应用程序已完成!", appName, appId[0]);
                            jobRunStatusEnums[0] = ProcessJobRunStatusEnum.FINISHED;
                            return;
                        case FAILED:
                            jobRunStatusEnums[0] = ProcessJobRunStatusEnum.FAILED;
                            log.info("Spark任务: {}, appId: {}, 应用程序以失败状态结束!", appName, appId[0]);
                            return;
                        case KILLED:
                            jobRunStatusEnums[0] = ProcessJobRunStatusEnum.KILLED;
                            log.info("Spark任务: {}, appId: {}, 应用程序已终止!", appName, appId[0]);
                            return;
                        case LOST:
                            jobRunStatusEnums[0] = ProcessJobRunStatusEnum.LOST;
                            log.info("Spark任务: {}, appId: {}, Spark Submit JVM 未知状态退出!", appName, appId[0]);
                            return;
                        default:
                            throw new AppSystemException("未知的 SparkAppHandle.State 状态信息! " + state);
                    }
                }

                @Override
                public void infoChanged(SparkAppHandle sparkAppHandle) {
                    if (sparkAppHandle.getAppId() != null) {
                        appId[0] = sparkAppHandle.getAppId();
                        log.info("Spark任务: {}, appId: {}, 状态信息变更: {}", appName, sparkAppHandle.getAppId(), sparkAppHandle.getState());
                    }
                }
            });
            while (!handle.getState().isFinal()) {
                if (handle.getState() == ProcessJobRunStatusEnum.CONNECTED.getState()) {
                    if (connected_time[0] >= CONNECTED_TIMEOUT) {
                        log.warn("connected_time[0]: " + connected_time[0]);
                        try {
                            log.error("Spark任务: {}, 阶段: {} 处理超时, 超时时间: {} 秒 直接 stop() !", appName, handle.getState(), connected_time[0]);
                            handle.stop();
                        } catch (Exception e) {
                            log.error("Spark任务: {}, 阶段: {} 执行 stop() 发生异常 直接 kill() !", appName, handle.getState());
                            handle.kill();
                        }
                    }
                }
                if (null == appId[0] || appId[0].isEmpty()) {
                    log.info("Spark任务: {}, 当前状态: {}", appName, handle.getState());
                } else {
                    log.info("Spark任务: {}, appId: {}, 当前状态: {}", appName, appId[0], handle.getState());
                }
                TimeUnit.SECONDS.sleep(5L);
                connected_time[0] += 5;
            }
            if (handle.getState().isFinal()) {
                log.info("Spark任务: {}, appId: {}, 当前状态: {}, 任务到达最终执行状态!", appName, appId[0], handle.getState());
                try {
                    handle.stop();
                } catch (Exception e) {
                    handle.kill();
                }
            }
            countDownLatch.await();
            log.debug("加工Spark作业 _jobRunStatusEnum.getCode(): {}", jobRunStatusEnums[0].getCode());
            log.info("Spark任务: {}, appId: {}, 执行结束,结束状态: {}", appName, appId[0], handle.getState());
            return jobRunStatusEnums[0];
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            System.exit(-1);
            throw new AppSystemException("加工提交 SparkLauncher 作业执行发生异常! e: " + e);
        }
    }
}
