package hyren.serv6.b.realtimecollection.util;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.SdmSpJobinfo;
import hyren.serv6.base.entity.SdmSpParam;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteResultHandler;
import java.io.File;
import java.util.List;

@Slf4j
public class StreamingProRunner {

    private static final String SPARK_MAIN_CLASS = "hrds.stream.structuredStreaming.StreamingApp";

    private static final String SPACE = " ";

    private static final String SPARK_PREFIX = "--";

    private static final String STREAM_PREFIX = "-";

    private String path;

    private List<SdmSpParam> paramList;

    private String filename;

    private long ssj_job_id;

    public StreamingProRunner(String path, List<SdmSpParam> paramList, String filename, long ssj_job_id) {
        this.path = path;
        this.paramList = paramList;
        this.filename = filename;
        this.ssj_job_id = ssj_job_id;
    }

    public void runJob() {
        Result result = Dbo.queryResult("select * from " + SdmSpJobinfo.TableName + " where ssj_job_id = ?", ssj_job_id);
        String streamingProJar = PropertyParaValue.getString("streamingProJar", "file:///opt/yj/fullgoal/streamingpro-spark-2.0-1.1.0.jar");
        String StreamProExecut = PropertyParaValue.getString("StreamProExecut", "spark2-submit");
        StringBuilder command = new StringBuilder();
        command.append(StreamProExecut).append(" --class ").append(SPARK_MAIN_CLASS).append(SPACE);
        command.append(streamingProJar).append(SPACE).append(" -streaming.job.file.path ").append(path).append(File.separator).append(filename).append(SPACE);
        int duration = 10;
        String platform = "";
        command.append("-streaming.name ").append(result.getString(0, "ssj_job_name")).append(SPACE);
        for (SdmSpParam param : paramList) {
            String ssp_param_key = param.getSsp_param_key();
            String ssp_param_value = param.getSsp_param_value();
            if ("streaming_platform".equals(ssp_param_key)) {
                platform = ssp_param_value;
            }
            if ("streaming_duration".equals(ssp_param_key)) {
                duration = Integer.parseInt(ssp_param_value);
            }
            if ("streaming_watermark".equals(ssp_param_key) && !StringUtil.isEmpty(ssp_param_value)) {
                ssp_param_key = ssp_param_key.replaceAll("_", ".");
                ssp_param_value = ssp_param_value.replaceAll(" ", "_");
                command.append(STREAM_PREFIX).append(ssp_param_key).append(SPACE).append(ssp_param_value).append(SPACE);
            } else if (ssp_param_key.startsWith("streaming") && !StringUtil.isEmpty(ssp_param_value)) {
                ssp_param_key = ssp_param_key.replaceAll("_", ".");
                command.append(STREAM_PREFIX).append(ssp_param_key).append(SPACE).append(ssp_param_value).append(SPACE);
            } else if (!StringUtil.isEmpty(ssp_param_value)) {
                ssp_param_key = ssp_param_key.replaceAll("_", "-");
                command.append(SPARK_PREFIX).append(ssp_param_key).append(SPACE).append(ssp_param_value).append(SPACE);
            } else {
                log.warn("不合法或者值为空的参数：" + ssp_param_key + "----" + ssp_param_value);
            }
        }
        if ("spark".equals(platform)) {
            BatchExecutorThread executorThread = new BatchExecutorThread(command.toString(), duration, ssj_job_id);
            log.info("=============开始调用多线程程序=============");
            new Thread(executorThread).start();
        } else if ("ss".equals(platform)) {
            log.info("开始执行spark作业调度：" + command);
            try {
                CommandLine commandLine = CommandLine.parse(command.toString());
                DefaultExecutor executor = new DefaultExecutor();
                ExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();
                executor.execute(commandLine, resultHandler);
                log.info("执行spark作业调度完成");
            } catch (Exception e) {
                throw new BusinessException("调度spark作业失败!!!");
            }
        } else {
            throw new BusinessException("暂时不支持" + platform + "执行引擎");
        }
    }
}
