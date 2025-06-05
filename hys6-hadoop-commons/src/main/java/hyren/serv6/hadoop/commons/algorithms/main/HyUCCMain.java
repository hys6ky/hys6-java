package hyren.serv6.hadoop.commons.algorithms.main;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.commons.hadoop.algorithms.conf.AlgorithmsConf;
import hyren.serv6.hadoop.commons.algorithms.impl.DistributedHyUCC;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class HyUCCMain {

    public static void main(String[] args) throws IOException {
        String table_name = args[0];
        String data = FileUtils.readFileToString(new File(Constant.ALGORITHMS_CONF_SERIALIZE_PATH + table_name), StandardCharsets.UTF_8);
        AlgorithmsConf algorithmsConf = JsonUtil.toObject(JsonUtil.toJson(data), new TypeReference<AlgorithmsConf>() {
        });
        executeUCC(algorithmsConf);
    }

    public static void executeUCC(AlgorithmsConf algorithmsConf) throws IOException {
        SparkConf sparkConf = new SparkConf().setAppName("DistributedHybridUCC").setMaster("local[*]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryoserializer.buffer.max", "1024m");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SparkSession spark = SparkSession.builder().getOrCreate();
        Dataset<Row> df;
        if (algorithmsConf.getUseParquet()) {
            df = spark.read().parquet(algorithmsConf.getInputFilePath());
        } else if (algorithmsConf.getUseCsv()) {
            df = spark.read().option("header", algorithmsConf.getInputFileHasHeader()).option("delimiter", algorithmsConf.getInputFileSeparator()).csv(algorithmsConf.getInputFilePath());
        } else {
            Properties properties = new Properties();
            properties.setProperty("driver", algorithmsConf.getDriver());
            properties.setProperty("user", algorithmsConf.getUser());
            properties.setProperty("password", algorithmsConf.getPassword());
            if (algorithmsConf.getPredicates() != null) {
                log.info("读取字段" + Arrays.toString(algorithmsConf.getSelectColumnArray()));
                if (algorithmsConf.getSelectColumnArray().length > 100) {
                    algorithmsConf.setJdbcLimit(200000);
                } else if (algorithmsConf.getSelectColumnArray().length > 30) {
                    algorithmsConf.setJdbcLimit(400000);
                } else if (algorithmsConf.getSelectColumnArray().length > 20) {
                    algorithmsConf.setJdbcLimit(600000);
                } else if (algorithmsConf.getSelectColumnArray().length > 15) {
                    algorithmsConf.setJdbcLimit(800000);
                }
                df = spark.read().jdbc(algorithmsConf.getJdbcUrl(), algorithmsConf.getTable_name(), algorithmsConf.getPredicates(), properties).selectExpr(algorithmsConf.getSelectColumnArray()).limit(algorithmsConf.getJdbcLimit());
            } else {
                log.info("读取字段" + Arrays.toString(algorithmsConf.getSelectColumnArray()));
                if (algorithmsConf.getSelectColumnArray().length > 100) {
                    algorithmsConf.setJdbcLimit(200000);
                } else if (algorithmsConf.getSelectColumnArray().length > 30) {
                    algorithmsConf.setJdbcLimit(400000);
                } else if (algorithmsConf.getSelectColumnArray().length > 20) {
                    algorithmsConf.setJdbcLimit(600000);
                } else if (algorithmsConf.getSelectColumnArray().length > 15) {
                    algorithmsConf.setJdbcLimit(800000);
                }
                df = spark.read().jdbc(algorithmsConf.getJdbcUrl(), algorithmsConf.getTable_name(), properties).selectExpr(algorithmsConf.getSelectColumnArray()).limit(algorithmsConf.getJdbcLimit());
            }
        }
        DistributedHyUCC.sc = sc;
        DistributedHyUCC.df = df;
        DistributedHyUCC.columnNames = DistributedHyUCC.df.columns();
        DistributedHyUCC.numberAttributes = DistributedHyUCC.columnNames.length;
        DistributedHyUCC.datasetFile = algorithmsConf.getInputFilePath();
        DistributedHyUCC.outputFile = algorithmsConf.getOutputFilePath() + Constant.HYUCC_RESULT_PATH_NAME;
        DistributedHyUCC.numPartitions = algorithmsConf.getNumPartition();
        DistributedHyUCC.batchSize = algorithmsConf.getBatchSize();
        DistributedHyUCC.validationBatchSize = algorithmsConf.getValidationBatchSize();
        if (algorithmsConf.getMaxDepth() > 0)
            DistributedHyUCC.maxLhsSize = algorithmsConf.getMaxDepth();
        log.info("\n  ======= Starting HyUCC =======");
        log.info("datasetFile: " + algorithmsConf.getInputFilePath());
        log.info(DistributedHyUCC.sc.sc().applicationId());
        log.info("numPartitions: " + algorithmsConf.getNumPartition());
        log.info("batchSize: " + algorithmsConf.getBatchSize());
        log.info("validationBatchSize: " + algorithmsConf.getValidationBatchSize());
        log.info("numberAttributes: " + DistributedHyUCC.numberAttributes);
        DistributedHyUCC.execute();
        DistributedHyUCC.sc.stop();
    }
}
