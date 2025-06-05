package hyren.serv6.trigger.task.executor;

import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.ETLDataSource;
import hyren.serv6.base.codes.Pro_Type;
import hyren.serv6.base.entity.EtlJobCur;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.jobUtil.task.TaskSqlHelper;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.trigger.util.ProcessUtil;
import lombok.extern.slf4j.Slf4j;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
public class TaskExecutor {

    private static final String PARAM_SEPARATOR = "@";

    private static final String LOGNAME_SEPARATOR = "_";

    private static final String LOG_SUFFIX = ".log";

    private static final String ERRORLOG_SUFFIX = "error.log";

    private static final String YARN_JOBSUFFIX = "_hyren";

    private static final String CRAWL_FLAG = "CRAWL";

    private static final String SHELL_COMMANDLINE = "bash";

    private static final String YARN_COMMANDLINE = "sh";

    private static final String JAVA_COMMANDLINE = "java";

    private static final String PERL_COMMANDLINE = "perl";

    private static final String PYTHON_COMMANDLINE = "python";

    public final static int PROGRAM_DONE_FLAG = 0;

    public final static int PROGRAM_ERROR_FLAG = 1;

    public static EtlJobCur executeEtlJob(final EtlJobCur etlJobCur, final String etlSysCode) throws IOException, InterruptedException {
        long etlSysId = etlJobCur.getEtl_sys_id();
        long etlJobId = etlJobCur.getEtl_job_id();
        String etlJob = etlJobCur.getEtl_job();
        String proType = etlJobCur.getPro_type();
        String jobOnly = etlJob + "_" + etlJobCur.getCurr_bath_date();
        String logDirc = TaskExecutor.getLogPath(etlJob, etlJobCur.getLog_dic(), etlJobCur.getCurr_bath_date(), false);
        String logDircErr = TaskExecutor.getLogPath(etlJob, etlJobCur.getLog_dic(), etlJobCur.getCurr_bath_date(), true);
        if (Pro_Type.KETTLETRSN.getCode().equals(proType)) {
            try {
                etlJobCur.setJob_return_val(runTran(etlJobCur));
            } catch (KettleException e) {
                etlJobCur.setJob_return_val(TaskExecutor.PROGRAM_ERROR_FLAG);
                log.warn("{} execute error!" + e, jobOnly);
            }
        } else if (Pro_Type.KETTLEJOB.getCode().equals(proType)) {
            try {
                etlJobCur.setJob_return_val(runJob(etlJobCur));
            } catch (KettleException e) {
                etlJobCur.setJob_return_val(TaskExecutor.PROGRAM_ERROR_FLAG);
                log.warn("{} execute error!" + e, jobOnly);
            }
        } else {
            String[] cmds = TaskExecutor.getCommend(etlJobCur, etlSysCode);
            log.info("{} 作业开始执行，指令为 {}", jobOnly, String.join(Constant.SPACE, cmds));
            Process process = Runtime.getRuntime().exec(cmds);
            log.info("\n****************************************************************\n" + "{} 作业开始执行，\n指令为 {}\n" + "****************************************************************\n", jobOnly, String.join(" ", cmds));
            if (Pro_Type.Yarn.getCode().equals(proType)) {
                String yarnName = cmds[2];
                String appId = ProcessUtil.getYarnAppID(yarnName);
                etlJobCur.setJob_process_id(appId);
                TaskSqlHelper.updateEtlJobProcessId(appId, etlJobId);
                new WatchThread(process.getInputStream(), logDirc).start();
                new WatchThread(process.getErrorStream(), logDircErr).start();
                etlJobCur.setJob_return_val(ProcessUtil.getStatusOnYarn(appId));
            } else {
                String appId = String.valueOf(ProcessUtil.getPid(process));
                etlJobCur.setJob_process_id(appId);
                TaskSqlHelper.updateEtlJobProcessId(appId, etlJobId);
                String childPidByPPid = ProcessUtil.getChildPidByPPid(appId);
                TaskSqlHelper.updateEtlJobChildPId(appId, etlSysId, etlJobId, childPidByPPid);
                TaskSqlHelper.closeDbConnector();
                new WatchThread(process.getInputStream(), logDirc).start();
                new WatchThread(process.getErrorStream(), logDircErr).start();
                etlJobCur.setJob_return_val(process.waitFor());
            }
        }
        log.info("{} 任务执行结束，返回值 {}", jobOnly, etlJobCur.getJob_return_val());
        if (TaskExecutor.PROGRAM_DONE_FLAG == etlJobCur.getJob_return_val()) {
            if (!StringUtil.isEmpty(etlJobCur.getSuccess_job())) {
                runEndJob(etlJobCur.getSuccess_job(), logDirc, logDircErr, etlSysCode);
            }
        } else {
            if (!StringUtil.isEmpty(etlJobCur.getFail_job())) {
                runEndJob(etlJobCur.getFail_job(), logDirc, logDircErr, etlSysCode);
            }
        }
        return etlJobCur;
    }

    private static int runTran(final EtlJobCur etlJobCur) throws KettleException {
        String jobName = etlJobCur.getEtl_job();
        String proName = etlJobCur.getPro_name();
        String[] propArr = getParaArr(etlJobCur.getPro_para());
        String jobOnly = jobName + "_" + etlJobCur.getCurr_bath_date();
        if (propArr.length < 4) {
            log.warn("{} Repository info is not enough!", jobOnly);
            return TaskExecutor.PROGRAM_ERROR_FLAG;
        }
        KettleDatabaseRepository repository = TaskExecutor.initAndGetKettle(propArr[0], propArr[1], propArr[2], propArr[3]);
        String proDic = getDic(etlJobCur.getPro_dic());
        RepositoryDirectoryInterface directoryInterface = repository.findDirectory(proDic);
        TransMeta transformationMeta = ((Repository) repository).loadTransformation(proName, directoryInterface, null, true, null);
        Trans trans = new Trans(transformationMeta);
        if (propArr.length > 4) {
            String[] parms = new String[propArr.length - 4];
            System.arraycopy(propArr, propArr.length - 3, parms, 0, propArr.length - 4);
            trans.execute(parms);
        } else {
            trans.execute(null);
        }
        trans.waitUntilFinished();
        if (trans.getErrors() > 0) {
            log.warn("{} execute error.", jobOnly);
            return TaskExecutor.PROGRAM_ERROR_FLAG;
        } else {
            log.info("{} execute success.", jobOnly);
            return TaskExecutor.PROGRAM_DONE_FLAG;
        }
    }

    private static int runJob(final EtlJobCur etlJobCur) throws KettleException {
        String jobName = etlJobCur.getEtl_job();
        String proName = etlJobCur.getPro_name();
        String[] propArr = getParaArr(etlJobCur.getPro_para());
        String jobOnly = jobName + "_" + etlJobCur.getCurr_bath_date();
        if (propArr.length < 4) {
            log.warn("{} Repository info is not enough!", jobOnly);
            return PROGRAM_ERROR_FLAG;
        }
        KettleDatabaseRepository repository = TaskExecutor.initAndGetKettle(propArr[0], propArr[1], propArr[2], propArr[3]);
        String proDic = getDic(etlJobCur.getPro_dic());
        RepositoryDirectoryInterface directoryInterface = repository.findDirectory(proDic);
        repository.loadRepositoryDirectoryTree(directoryInterface);
        JobMeta jobMeta = repository.loadJob(proName, directoryInterface, null, null);
        Job job = new Job(repository, jobMeta);
        for (int i = 4; i < propArr.length; ) {
            job.setParameterValue(propArr[i], propArr[i + 1]);
            i = i + 2;
        }
        job.start();
        job.waitUntilFinished();
        if (job.getErrors() > 0) {
            log.warn("{} execute error.", jobOnly);
            return TaskExecutor.PROGRAM_ERROR_FLAG;
        } else {
            log.info("{} execute success.", jobOnly);
            return TaskExecutor.PROGRAM_DONE_FLAG;
        }
    }

    private static KettleDatabaseRepository initAndGetKettle(String schema, String url, String userName, String passWord) throws KettleException {
        KettleEnvironment.init();
        KettleDatabaseRepository repository = new KettleDatabaseRepository();
        DatabaseMeta databaseMeta = new DatabaseMeta("kettle", "Oracle", "jdbc", url, schema, "1521", userName, passWord);
        KettleDatabaseRepositoryMeta kettleDatabaseRepositoryMeta = new KettleDatabaseRepositoryMeta("kettle", "kettle", "Transformation description", databaseMeta);
        repository.init(kettleDatabaseRepositoryMeta);
        repository.connect("admin", "admin");
        return repository;
    }

    private static String[] getCommend(final EtlJobCur etlJobCur, String etlSysCd) {
        long etlJobId = etlJobCur.getEtl_job_id();
        String jobName = etlJobCur.getEtl_job();
        LocalDate.parse(etlJobCur.getCurr_bath_date(), DateUtil.DATE_DEFAULT);
        String currBathDate = etlJobCur.getCurr_bath_date();
        String jobOnly = jobName + "_" + currBathDate;
        String proType = etlJobCur.getPro_type();
        String proName = etlJobCur.getPro_name();
        String proDic = TaskExecutor.getDic(etlJobCur.getPro_dic());
        String logDic = TaskExecutor.getDic(etlJobCur.getLog_dic());
        String[] propArr = TaskExecutor.getParaArr(etlJobCur.getPro_para());
        String[] commands;
        List<String> jobFlag = getJobFlag(jobName, etlJobCur.getJob_datasource(), etlSysCd);
        if (Pro_Type.SHELL.getCode().equals(proType)) {
            commands = new String[propArr.length + 2 + jobFlag.size()];
            commands[0] = SHELL_COMMANDLINE;
            commands[1] = proDic + proName;
            System.arraycopy(propArr, 0, commands, 2, propArr.length);
            for (int i = 0; i < jobFlag.size(); i++) {
                commands[propArr.length + 2 + i] = jobFlag.get(i);
            }
        } else if (Pro_Type.Yarn.getCode().equals(proType)) {
            commands = new String[propArr.length + 3 + jobFlag.size()];
            commands[0] = YARN_COMMANDLINE;
            commands[1] = proDic + proName;
            commands[2] = UUID.randomUUID() + YARN_JOBSUFFIX;
            System.arraycopy(propArr, 0, commands, 3, propArr.length);
            for (int i = 0; i < jobFlag.size(); i++) {
                commands[propArr.length + 3 + i] = jobFlag.get(i);
            }
        } else if (Pro_Type.JAVA.getCode().equals(proType)) {
            commands = new String[propArr.length + 2 + jobFlag.size()];
            commands[0] = JAVA_COMMANDLINE;
            commands[1] = proDic + proName;
            System.arraycopy(propArr, 0, commands, 2, propArr.length);
            for (int i = 0; i < jobFlag.size(); i++) {
                commands[propArr.length + 2 + i] = jobFlag.get(i);
            }
        } else if (Pro_Type.BAT.getCode().equals(proType)) {
            commands = new String[propArr.length + 1 + jobFlag.size()];
            commands[0] = proDic + proName;
            System.arraycopy(propArr, 0, commands, 1, propArr.length);
            for (int i = 0; i < jobFlag.size(); i++) {
                commands[propArr.length + 1 + i] = jobFlag.get(i);
            }
        } else if (Pro_Type.PERL.getCode().equals(proType)) {
            commands = new String[propArr.length + 2 + jobFlag.size()];
            commands[0] = PERL_COMMANDLINE;
            commands[1] = proDic + proName;
            System.arraycopy(propArr, 0, commands, 2, propArr.length);
            for (int i = 0; i < jobFlag.size(); i++) {
                commands[propArr.length + 2 + i] = jobFlag.get(i);
            }
        } else if (Pro_Type.PYTHON.getCode().equals(proType)) {
            commands = new String[propArr.length + 2 + jobFlag.size()];
            commands[0] = PYTHON_COMMANDLINE;
            commands[1] = proDic + proName;
            System.arraycopy(propArr, 0, commands, 2, propArr.length);
            for (int i = 0; i < jobFlag.size(); i++) {
                commands[propArr.length + 2 + i] = jobFlag.get(i);
            }
        } else if (CRAWL_FLAG.equals(proType)) {
            commands = new String[propArr.length + 1 + jobFlag.size()];
            commands[0] = proDic + proName;
            System.arraycopy(propArr, 0, commands, 1, propArr.length);
            for (int i = 0; i < jobFlag.size(); i++) {
                commands[propArr.length + 2 + i] = jobFlag.get(i);
            }
        } else {
            throw new AppSystemException(jobOnly + "暂不支持的程序类型：" + proType);
        }
        return commands;
    }

    private static String getDic(final String dicPath) {
        File file = new File(dicPath);
        if (!file.exists()) {
            try {
                if (!file.mkdirs()) {
                    throw new AppSystemException("创建目录失败! path: " + dicPath);
                }
            } catch (Exception e) {
                throw new AppSystemException("无法创建目录：" + dicPath);
            }
        }
        return dicPath;
    }

    private static String[] getParaArr(final String para) {
        if (StringUtil.isEmpty(para))
            return new String[] {};
        return para.split(PARAM_SEPARATOR);
    }

    private static String getLogPath(final String proName, final String logDic, final String currDate, final boolean isErrorLog) {
        String logPath = logDic + File.separator + proName + LOGNAME_SEPARATOR + currDate;
        if (isErrorLog) {
            logPath = logPath + ERRORLOG_SUFFIX;
        } else {
            logPath = logPath + LOG_SUFFIX;
        }
        return logPath;
    }

    public static void runEndJob(String endEtlJOb, String logDirc, String logDircErr, String etlSysCd) {
        try {
            List<String> jobFlag = getJobFlag("HXZY", "other", etlSysCd);
            String[] commands = new String[jobFlag.size() + 2];
            commands[0] = SHELL_COMMANDLINE;
            commands[1] = endEtlJOb;
            for (int i = 0; i < jobFlag.size(); i++) {
                commands[i + 2] = jobFlag.get(i);
            }
            Process process = Runtime.getRuntime().exec(commands);
            new WatchThread(process.getInputStream(), logDirc).start();
            new WatchThread(process.getErrorStream(), logDircErr).start();
        } catch (Exception e) {
            log.error("后续作业执行失败，跳过");
        }
    }

    private static List<String> getJobFlag(String jobName, String jobSrc, String etlSysCd) {
        List<String> jobParams = new ArrayList<>();
        jobParams.add("-P");
        jobParams.add("hyrenJob");
        jobParams.add("-J");
        jobParams.add(jobName);
        String _js = "other";
        try {
            if (ETLDataSource.DBWenJianCaiJi == ETLDataSource.ofEnumByCode(jobSrc))
                _js = "dbDataCollect";
            else if (ETLDataSource.ShuJuKuCaiJi == ETLDataSource.ofEnumByCode(jobSrc))
                _js = "dataCollect";
            else if (ETLDataSource.ShuJuKuChouShu == ETLDataSource.ofEnumByCode(jobSrc))
                _js = "dataExtract";
            else if (ETLDataSource.BanJieGouHuaCaiJi == ETLDataSource.ofEnumByCode(jobSrc))
                _js = "objectCollect";
            else if (ETLDataSource.FeiJieGouHuaCaiJi == ETLDataSource.ofEnumByCode(jobSrc))
                _js = "fileCollect";
            else if (ETLDataSource.ShuJuJiaGong == ETLDataSource.ofEnumByCode(jobSrc))
                _js = "dataMarket";
            else if (ETLDataSource.ShuJuGuanKong == ETLDataSource.ofEnumByCode(jobSrc))
                _js = "dataManagement";
            else if (ETLDataSource.ShuJuFenFa == ETLDataSource.ofEnumByCode(jobSrc))
                _js = "dataDistribution";
        } catch (Exception e) {
            log.error("这个错不处理，类型全部设置为other，忽略");
        }
        jobParams.add("-S");
        jobParams.add(_js);
        jobParams.add("-E");
        jobParams.add(etlSysCd);
        jobParams.add("-D");
        return jobParams;
    }
}
