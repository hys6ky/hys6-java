package hyren.serv6.commons.hadoop.algorithms.main;

import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.*;

@Slf4j
public class SparkJobRunner {

    private static final String ALGORITHMS_SPARK_CLASSPATH = PropertyParaValue.getString("algorithms_spark_classpath", "");

    private static final long SPARK_JOB_TIMEOUT_SECONDS = PropertyParaValue.getLong("spark.job.timeout.seconds", 24L * 60 * 60);

    private static final String SPARK_DRIVER_EXTRAJAVAOPTIONS = PropertyParaValue.getString("spark.driver.extraJavaOptions", "-Xss20m -Xmx61440m");

    public static void runJob(String spark_main_class, String table_name) {
        try {
            Class.forName(spark_main_class);
        } catch (ClassNotFoundException e) {
            log.error("主类不存在");
        }
        long start = System.currentTimeMillis();
        String command = String.format("java %s -cp %s %s %s", SPARK_DRIVER_EXTRAJAVAOPTIONS, ALGORITHMS_SPARK_CLASSPATH, spark_main_class, table_name);
        log.info(String.format("开始执行spark作业调度:[%s]", command));
        CommandLine commandLine = CommandLine.parse(command);
        DefaultExecutor executor = new DefaultExecutor();
        ExecuteWatchdog watchdog = new ExecuteWatchdog(SPARK_JOB_TIMEOUT_SECONDS * 1000);
        executor.setWatchdog(watchdog);
        executor.setStreamHandler(new PumpStreamHandler(new LogOutputStream() {

            @Override
            protected void processLine(String line, int logLevel) {
                log.info(line);
            }
        }));
        Thread shutdownThread = new Thread(watchdog::destroyProcess);
        Runtime.getRuntime().addShutdownHook(shutdownThread);
        try {
            executor.execute(commandLine);
        } catch (Exception e) {
            throw new AppSystemException("调度spark作业失败");
        } finally {
            log.info("Spark作业执行时间：" + (System.currentTimeMillis() - start) / 1000 + "s");
            Runtime.getRuntime().removeShutdownHook(shutdownThread);
        }
    }
}
