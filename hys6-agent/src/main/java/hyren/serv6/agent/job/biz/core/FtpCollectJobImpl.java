package hyren.serv6.agent.job.biz.core;

import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.SftpException;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.*;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;
import hyren.serv6.agent.job.biz.bean.MetaInfoBean;
import hyren.serv6.agent.job.biz.constant.RunStatusConstant;
import hyren.serv6.agent.job.biz.core.ftpConsumer.FtpConsumerThread;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.agent.job.biz.utils.ProductFileUtil;
import hyren.serv6.base.codes.FtpRule;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.TimeType;
import hyren.serv6.base.entity.FtpCollect;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.jsch.SSHDetails;
import hyren.serv6.base.utils.jsch.SSHOperate;
import hyren.serv6.commons.compress.DeCompressionUtil;
import hyren.serv6.commons.utils.MapDBHelper;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/10/12 10:29")
public class FtpCollectJobImpl implements JobInterface {

    private final static ConcurrentMap<String, Thread> mapJob = new ConcurrentHashMap<>();

    private volatile boolean is_real_time = true;

    private final FtpCollect ftp_collect;

    public FtpCollectJobImpl(FtpCollect ftp_collect) {
        this.ftp_collect = ftp_collect;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public JobStatusInfo runJob() {
        String ftpId = ftp_collect.getFtp_id().toString();
        Thread thread = mapJob.get(ftpId);
        if (thread != null && !thread.isInterrupted()) {
            log.info("重复发送，中断上一个实时线程");
            thread.interrupt();
            mapJob.remove(ftpId);
        }
        String statusFilePath = Constant.JOBINFOPATH + ftpId + File.separator + Constant.JOBFILENAME;
        JobStatusInfo jobStatus = JobStatusInfoUtil.getStartJobStatusInfo(statusFilePath, ftpId, "ftp_collect");
        String is_read_realtime = ftp_collect.getIs_read_realtime();
        Long realtime_interval = ftp_collect.getRealtime_interval();
        String ftpDir = ftp_collect.getFtp_dir();
        String localPath = ftp_collect.getLocal_path();
        ftpDir = FileNameUtils.normalize(ftpDir, true);
        localPath = FileNameUtils.normalize(localPath, true);
        String ftpRulePath = ftp_collect.getFtp_rule_path();
        String fileSuffix = ftp_collect.getFile_suffix();
        log.info("开始执行ftp采集，根据当前任务id放入线程");
        mapJob.put(ftpId, Thread.currentThread());
        try {
            if (mapJob.get(ftpId + "_ftpConsumerThread") == null || !mapJob.get(ftpId + "_ftpConsumerThread").isAlive()) {
                log.info("启动实时消费进行，将数据batch进ftp已传输表数据库");
                ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(50000);
                FtpConsumerThread.queueMap.put(ftpId, queue);
                Thread consumerThread = new FtpConsumerThread(ftpId);
                consumerThread.start();
                mapJob.put(ftpId + "_ftpConsumerThread", consumerThread);
            }
            while (is_real_time) {
                if (IsFlag.Fou.getCode().equals(is_read_realtime)) {
                    is_real_time = false;
                }
                SSHDetails sshDetails = SSHOperate.getSSHDetails(ftp_collect.getFtp_ip(), ftp_collect.getFtp_username(), StringUtil.unicode2String(ftp_collect.getFtp_password()), Integer.parseInt(ftp_collect.getFtp_port()));
                try (SSHOperate sshOperate = new SSHOperate(sshDetails)) {
                    String ftpFolderName;
                    if (ftpRulePath.equals(FtpRule.LiuShuiHao.getCode())) {
                        if (IsFlag.Shi.getCode().equals(ftp_collect.getFtp_model())) {
                            ftpFolderName = remoteNumberDir(ftpDir, sshOperate);
                        } else {
                            ftpFolderName = localNumberDir(localPath);
                        }
                    } else if (ftpRulePath.equals(FtpRule.GuDingMuLu.getCode())) {
                        ftpFolderName = ftp_collect.getChild_file_path();
                    } else if (ftpRulePath.equals(FtpRule.AnShiJian.getCode())) {
                        ftpFolderName = getDateDir(ftp_collect.getChild_time());
                    } else {
                        throw new BusinessException("FTP rule 不存在：" + ftpRulePath);
                    }
                    try (MapDBHelper mapDBHelper = new MapDBHelper(Constant.MAPDBPATH + ftpId, ftpId + ".db")) {
                        ConcurrentMap<String, String> fileNameHTreeMap = mapDBHelper.htMap(ftpId, 25 * 12);
                        if (IsFlag.Shi.getCode().equals(ftp_collect.getFtp_model())) {
                            String currentFTPDir;
                            if (ftpDir.endsWith("/")) {
                                currentFTPDir = ftpDir + ftpFolderName;
                            } else {
                                currentFTPDir = ftpDir + "/" + ftpFolderName;
                            }
                            transferPut(currentFTPDir, localPath, sshOperate, fileSuffix, mapDBHelper, fileNameHTreeMap);
                        } else {
                            String currentLoadDir;
                            if (localPath.endsWith("/")) {
                                currentLoadDir = localPath + ftpFolderName;
                            } else {
                                currentLoadDir = localPath + "/" + ftpFolderName;
                            }
                            transferGet(ftpDir, currentLoadDir, sshOperate, ftp_collect.getIs_unzip(), ftp_collect.getReduce_type(), fileSuffix, mapDBHelper, fileNameHTreeMap);
                        }
                    } catch (Exception e) {
                        log.error("创建或打开mapDB文件失败，ftp传输失败", e);
                        throw new BusinessException("创建或打开mapDB文件失败，ftp传输失败");
                    }
                }
                if (realtime_interval == null || realtime_interval == 0) {
                    realtime_interval = 1L;
                }
                try {
                    TimeUnit.SECONDS.sleep(realtime_interval);
                } catch (InterruptedException e) {
                    log.error("线程休眠异常，请检查是否是重复发送实时ftp采集任务", e);
                }
            }
            jobStatus.setRunStatus(RunStatusConstant.SUCCEED.getCode());
        } catch (Exception e) {
            log.error("FTP传输失败！！！", e);
            jobStatus.setRunStatus(RunStatusConstant.FAILED.getCode());
        }
        jobStatus.setEndTime(DateUtil.getSysTime());
        jobStatus.setEndDate(DateUtil.getSysDate());
        mapJob.remove(ftpId);
        try {
            Map<String, Boolean> object = new HashMap<>();
            object.put("end", true);
            FtpConsumerThread.queueMap.get(ftpId).put(JsonUtil.toJson(object));
        } catch (InterruptedException e) {
            log.info("告诉同步程序，当前任务结束被重新发送过来的任务打断", e);
        }
        ProductFileUtil.createStatusFile(statusFilePath, JsonUtil.toJson(jobStatus));
        log.info("任务结束，根据当前任务id移除线程");
        return jobStatus;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "filePath", desc = "", range = "")
    @Return(desc = "", range = "")
    private boolean validateDirectory(String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            return true;
        }
        return file.mkdirs();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "childTime", desc = "", range = "")
    @Return(desc = "", range = "")
    private String getDateDir(String childTime) {
        String dateDir = DateUtil.getSysDate() + DateUtil.getSysTime();
        if (childTime.equals(TimeType.Day.getCode())) {
            return dateDir.substring(0, 8);
        } else if (childTime.equals(TimeType.Hour.getCode())) {
            return dateDir.substring(0, 10);
        } else if (childTime.equals(TimeType.Minute.getCode())) {
            return dateDir.substring(0, 12);
        } else if (childTime.equals(TimeType.Second.getCode())) {
            return dateDir;
        } else {
            throw new BusinessException("下级目录时间错误 ：" + childTime);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ftpDir", desc = "", range = "")
    @Param(name = "destDir", desc = "", range = "")
    @Param(name = "sftp", desc = "", range = "")
    @Param(name = "isUnzip", desc = "", range = "")
    @Param(name = "deCompressWay", desc = "", range = "")
    @Param(name = "fileSuffix", desc = "", range = "")
    @Param(name = "mapDBHelper", desc = "", range = "")
    @Param(name = "fileNameHTreeMap", desc = "", range = "")
    private void transferGet(String ftpDir, String destDir, SSHOperate sftp, String isUnzip, String deCompressWay, String fileSuffix, MapDBHelper mapDBHelper, ConcurrentMap<String, String> fileNameHTreeMap) {
        Map<String, Object> object = new HashMap<>();
        boolean flag = validateDirectory(destDir);
        if (!flag) {
            throw new BusinessException("创建文件夹失败");
        }
        try {
            Vector<LsEntry> listDir;
            if (StringUtil.isEmpty(fileSuffix)) {
                listDir = sftp.listDir(ftpDir);
            } else {
                listDir = sftp.listDir(ftpDir, "*." + fileSuffix);
            }
            for (LsEntry lsEntry : listDir) {
                String tmpDestDir;
                String tmpFtpDir;
                if (ftpDir.endsWith("/")) {
                    tmpFtpDir = ftpDir + lsEntry.getFilename();
                } else {
                    tmpFtpDir = ftpDir + "/" + lsEntry.getFilename();
                }
                if (destDir.endsWith("/")) {
                    tmpDestDir = destDir + lsEntry.getFilename();
                } else {
                    tmpDestDir = destDir + "/" + lsEntry.getFilename();
                }
                if (lsEntry.getAttrs().isDir()) {
                    transferGet(tmpFtpDir, tmpDestDir, sftp, isUnzip, deCompressWay, fileSuffix, mapDBHelper, fileNameHTreeMap);
                } else {
                    if (!fileNameHTreeMap.containsKey(tmpFtpDir) || (fileNameHTreeMap.containsKey(tmpFtpDir) && !fileNameHTreeMap.get(tmpFtpDir).equals(lsEntry.getAttrs().getMtimeString()))) {
                        sftp.transferFile(tmpFtpDir, destDir);
                        boolean isSuccessful;
                        if (IsFlag.Shi.getCode().equals(isUnzip)) {
                            isSuccessful = DeCompressionUtil.deCompression(tmpDestDir, deCompressWay);
                            File file = new File(tmpDestDir);
                            if (file.exists()) {
                                if (!file.delete()) {
                                    throw new BusinessException("删除文件失败");
                                }
                            }
                        } else {
                            isSuccessful = true;
                        }
                        if (isSuccessful) {
                            fileNameHTreeMap.put(tmpFtpDir, lsEntry.getAttrs().getMtimeString());
                            mapDBHelper.commit();
                            object.put("fileName", lsEntry.getFilename());
                            object.put("absolutePath", tmpDestDir);
                            object.put("md5", lsEntry.getAttrs().getMtimeString());
                            object.put("ftpDate", DateUtil.getSysDate());
                            object.put("ftpTime", DateUtil.getSysTime());
                            object.put("end", false);
                            FtpConsumerThread.queueMap.get(ftp_collect.getFtp_id().toString()).put(JsonUtil.toJson(object));
                            object.clear();
                        } else {
                            throw new BusinessException("解压文件失败！！！");
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("FTP传输失败！！！", e);
            throw new BusinessException("ftp传输失败！");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ftpDir", desc = "", range = "")
    @Param(name = "localPath", desc = "", range = "")
    @Param(name = "sftp", desc = "", range = "")
    @Param(name = "fileSuffix", desc = "", range = "")
    @Param(name = "mapDBHelper", desc = "", range = "")
    @Param(name = "fileNameHTreeMap", desc = "", range = "")
    private void transferPut(String ftpDir, String localPath, SSHOperate sftp, String fileSuffix, MapDBHelper mapDBHelper, ConcurrentMap<String, String> fileNameHTreeMap) {
        Map<String, Object> object = new HashMap<>();
        try {
            sftp.scpMkdir(ftpDir);
            File[] files;
            if (JobConstant.FILECHANGESTYPEMD5) {
                files = new File(localPath).listFiles((file) -> (!fileNameHTreeMap.containsKey(file.getAbsolutePath()) || file.isDirectory() || (fileNameHTreeMap.containsKey(file.getAbsolutePath()) && !fileNameHTreeMap.get(file.getAbsolutePath()).equals(MD5Util.md5File(file)))));
            } else {
                files = new File(localPath).listFiles((file) -> (!fileNameHTreeMap.containsKey(file.getAbsolutePath()) || file.isDirectory() || (fileNameHTreeMap.containsKey(file.getAbsolutePath()) && !fileNameHTreeMap.get(file.getAbsolutePath()).equals(String.valueOf(file.lastModified())))));
            }
            if (files != null && files.length > 0) {
                for (File file : files) {
                    String fileName = file.getName();
                    String tmpFtpDir;
                    if (ftpDir.endsWith("/")) {
                        tmpFtpDir = ftpDir + fileName;
                    } else {
                        tmpFtpDir = ftpDir + "/" + fileName;
                    }
                    if (file.isDirectory()) {
                        transferPut(tmpFtpDir, file.getAbsolutePath(), sftp, fileSuffix, mapDBHelper, fileNameHTreeMap);
                    } else {
                        if (StringUtil.isBlank(fileSuffix) || fileName.endsWith(fileSuffix)) {
                            String absolutePath = file.getAbsolutePath();
                            log.info("==============absolutePath==========" + absolutePath);
                            log.info("==============tmpFtpDir=============" + tmpFtpDir);
                            sftp.transferPutFile(absolutePath, tmpFtpDir);
                            if (JobConstant.FILECHANGESTYPEMD5) {
                                String md5 = MD5Util.md5File(file);
                                fileNameHTreeMap.put(absolutePath, md5);
                                object.put("md5", md5);
                            } else {
                                String lastModified = String.valueOf(file.lastModified());
                                fileNameHTreeMap.put(absolutePath, lastModified);
                                object.put("md5", lastModified);
                            }
                            mapDBHelper.commit();
                            object.put("fileName", file.getName());
                            object.put("absolutePath", absolutePath);
                            object.put("ftpDate", DateUtil.getSysDate());
                            object.put("ftpTime", DateUtil.getSysTime());
                            object.put("end", false);
                            FtpConsumerThread.queueMap.get(ftp_collect.getFtp_id().toString()).put(JsonUtil.toJson(object));
                            object.clear();
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("FTP传输失败！！！", e);
            throw new BusinessException("ftp传输失败！");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dir", desc = "", range = "")
    @Param(name = "sftp", desc = "", range = "")
    @Return(desc = "", range = "")
    private String remoteNumberDir(String dir, SSHOperate sftp) throws SftpException {
        Vector<LsEntry> listDir = sftp.listDir(dir);
        int max = -1;
        for (LsEntry lsEntry : listDir) {
            String filename = lsEntry.getFilename();
            if (lsEntry.getAttrs().isDir() && NumberUtil.isNumberic(filename)) {
                int parseInt = Integer.parseInt(filename);
                if (parseInt > max) {
                    max = parseInt;
                }
            }
        }
        if (max == -1) {
            return "0";
        }
        return String.valueOf(max + 1);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dir", desc = "", range = "")
    @Return(desc = "", range = "")
    private String localNumberDir(String dir) {
        File pmFile = new File(dir);
        File[] listFiles = pmFile.listFiles((file) -> NumberUtil.isNumberic(file.getName()) && file.isDirectory());
        if (listFiles == null || listFiles.length == 0) {
            return "0";
        }
        int max = 0;
        for (File file : listFiles) {
            String name = file.getName();
            int parseInt = Integer.parseInt(name);
            if (parseInt > max) {
                max = parseInt;
            }
        }
        return String.valueOf(max + 1);
    }

    @Override
    public List<MetaInfoBean> getMetaInfoGroup() {
        return null;
    }

    @Override
    public MetaInfoBean getMetaInfo() {
        return null;
    }

    @Override
    public JobStatusInfo call() {
        return runJob();
    }
}
