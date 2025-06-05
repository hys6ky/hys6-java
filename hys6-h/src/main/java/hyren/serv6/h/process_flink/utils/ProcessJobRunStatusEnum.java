package hyren.serv6.h.process_flink.utils;

import hyren.serv6.base.exception.AppSystemException;
import lombok.Getter;
import org.apache.spark.launcher.SparkAppHandle;

@Getter
public enum ProcessJobRunStatusEnum {

    UNKNOWN(101, "UNKNOWN", "应用程序未报告", SparkAppHandle.State.UNKNOWN),
    CONNECTED(102, "CONNECTED", "应用程序已连接", SparkAppHandle.State.CONNECTED),
    SUBMITTED(103, "SUBMITTED", "应用程序已提交", SparkAppHandle.State.SUBMITTED),
    RUNNING(104, "RUNNING", "应用程序正在运行", SparkAppHandle.State.RUNNING),
    FINISHED(105, "FINISHED", "应用程序已完成,状态为成功", SparkAppHandle.State.FINISHED),
    FAILED(106, "FAILED", "应用程序以失败状态结束", SparkAppHandle.State.FAILED),
    KILLED(107, "KILLED", "应用程序已终止", SparkAppHandle.State.KILLED),
    LOST(108, "LOST", "Spark Submit JVM 知状态退出", SparkAppHandle.State.LOST);

    public static final String CodeName = "ProcessSparkJobRunStatusEnum";

    private final int code;

    private final String name;

    private final String value;

    private final SparkAppHandle.State state;

    ProcessJobRunStatusEnum(int code, String name, String value, SparkAppHandle.State state) {
        this.code = code;
        this.name = name;
        this.value = value;
        this.state = state;
    }

    public static String ofValueByCode(int code) {
        for (ProcessJobRunStatusEnum typeCode : ProcessJobRunStatusEnum.values()) {
            if (typeCode.getCode() == code) {
                return typeCode.value;
            }
        }
        throw new AppSystemException("根据[" + code + "]没有找到对应的代码项" + " [ " + CodeName + ": 加工Spark作业运行状态枚举 ]");
    }

    public static ProcessJobRunStatusEnum ofEnumByCode(int code) {
        for (ProcessJobRunStatusEnum typeCode : ProcessJobRunStatusEnum.values()) {
            if (typeCode.getCode() == code) {
                return typeCode;
            }
        }
        throw new AppSystemException("根据[" + code + "]没有找到对应的代码项" + " [ " + CodeName + ": 加工Spark作业运行状态枚举 ]");
    }

    @Override
    public String toString() {
        throw new AppSystemException(" [ " + CodeName + ": 加工Spark作业运行状态枚举 ]" + " 不支持toString() 操作!");
    }
}
