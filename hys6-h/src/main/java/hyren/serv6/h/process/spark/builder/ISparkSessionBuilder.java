package hyren.serv6.h.process.spark.builder;

import org.apache.spark.sql.SparkSession;

public interface ISparkSessionBuilder {

    SparkSession build();
}
