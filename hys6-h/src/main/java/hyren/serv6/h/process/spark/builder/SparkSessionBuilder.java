package hyren.serv6.h.process.spark.builder;

import hyren.serv6.base.codes.Store_type;
import hyren.serv6.h.process.bean.ProcessJobTableConfBean;
import hyren.serv6.h.process.spark.func.FunctionsReader;
import hyren.serv6.h.process.spark.func.bean.FunctionBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import java.util.Iterator;

@Slf4j
public class SparkSessionBuilder implements ISparkSessionBuilder {

    private final ProcessJobTableConfBean processJobTableConfBean;

    public SparkSessionBuilder(ProcessJobTableConfBean processJobTableConfBean) {
        this.processJobTableConfBean = processJobTableConfBean;
    }

    @Override
    public SparkSession build() {
        log.info("Initializing SparkSession With Configuration.");
        Builder builder = SparkSession.builder().config(registerKryoClasses());
        Store_type store_type = Store_type.ofEnumByCode(processJobTableConfBean.getDataStoreLayer().getStore_type());
        if (store_type == Store_type.HIVE) {
            builder.config("hive.exec.dynamici.partition", true).config("hive.exec.dynamic.partition.mode", "nonstrict").enableHiveSupport();
        }
        SparkSession sparkSession = builder.getOrCreate();
        registerUDF(sparkSession.udf());
        log.info("SparkSession Initialization Completed.");
        return sparkSession;
    }

    private SparkConf registerKryoClasses() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.registerKryoClasses(new Class<?>[] {});
        return sparkConf;
    }

    private void registerUDF(UDFRegistration udfRegister) {
        Iterator<FunctionBean> iterator = new FunctionsReader().iterator();
        while (iterator.hasNext()) {
            FunctionBean next = iterator.next();
            udfRegister.registerJava(next.getName(), next.getClassName(), transformType(next.getDateType()));
            log.info("Register Custom UDF Functions: " + next.getName());
        }
    }

    private static DataType transformType(String type) {
        switch(type) {
            case "string":
                return DataTypes.StringType;
            case "binary":
                return DataTypes.BinaryType;
            case "boolean":
                return DataTypes.BooleanType;
            case "byte":
                return DataTypes.ByteType;
            case "date":
                return DataTypes.DateType;
            case "double":
                return DataTypes.DoubleType;
            case "integer":
                return DataTypes.IntegerType;
            case "long":
                return DataTypes.LongType;
            case "short":
                return DataTypes.ShortType;
            case "float":
                return DataTypes.FloatType;
            case "null":
                return DataTypes.NullType;
            default:
                log.info("Type not supported: " + type + " ,use default type: DataTypes.StringType");
                return DataTypes.StringType;
        }
    }
}
