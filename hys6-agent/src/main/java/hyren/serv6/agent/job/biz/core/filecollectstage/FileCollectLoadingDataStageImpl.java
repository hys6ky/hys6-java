package hyren.serv6.agent.job.biz.core.filecollectstage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.jcraft.jsch.ChannelSftp;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.agent.job.biz.core.filecollectstage.methods.AvroBeanProcess;
import hyren.serv6.agent.job.biz.core.filecollectstage.methods.AvroOper;
import hyren.serv6.agent.trans.biz.unstructuredfilecollect.FileCollectJobService;
import hyren.serv6.base.codes.DataSourceType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.utils.MapDBHelper;
import hyren.serv6.commons.utils.agent.bean.AvroBean;
import hyren.serv6.commons.utils.agent.bean.FileCollectParamBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.agent.constant.PropertyParaUtil;
import hyren.serv6.base.utils.jsch.FileProgressMonitor;
import hyren.serv6.base.utils.jsch.SSHDetails;
import hyren.serv6.base.utils.jsch.SSHOperate;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/11/5 10:12")
public class FileCollectLoadingDataStageImpl implements Callable<String> {

    private final FileCollectParamBean fileCollectParamBean;

    private static final String BIGFILENAME = "bigFiles";

    private final ConcurrentMap<String, String> fileNameHTreeMap;

    private final MapDBHelper mapDBHelper;

    FileCollectLoadingDataStageImpl(FileCollectParamBean paramBean, ConcurrentMap<String, String> fileNameHTreeMap, MapDBHelper mapDBHelper) {
        this.mapDBHelper = mapDBHelper;
        this.fileNameHTreeMap = fileNameHTreeMap;
        this.fileCollectParamBean = paramBean;
        String fileCollectHdfsPath = FileNameUtils.normalize(JobConstant.PREFIX + File.separator + DataSourceType.DCL.getCode() + File.separator + paramBean.getFcs_id() + File.separator + paramBean.getFile_source_id() + File.separator + BIGFILENAME, true);
        if (JobConstant.FILE_COLLECTION_IS_WRITE_HADOOP) {
            ClassBase.hadoopInstance().mkdirHdfs(fileCollectHdfsPath);
        }
    }

    @Override
    public String call() {
        String message = "";
        log.info("Start FileCollectLoadingDataStageImpl Thread ...");
        ArrayBlockingQueue<String> queue = FileCollectJobService.mapQueue.get(fileCollectParamBean.getFile_source_id());
        try {
            while (true) {
                String queueMeta = queue.take();
                log.info("queue.size: " + queue.size() + " ; queueMeta: " + queueMeta);
                Map<String, Object> queueJb = JsonUtil.toObject(queueMeta, new TypeReference<Map<String, Object>>() {
                });
                String sysDate = queueJb.get("sys_date").toString();
                String avroFileAbsolutionPath = queueJb.get("avroFileAbsolutionPath").toString();
                String fileCollectHdfsPath = queueJb.get("fileCollectHdfsPath").toString();
                String jobRsId = queueJb.get("job_rs_id").toString();
                long watcherId = Long.parseLong(queueJb.get("watcher_id").toString());
                if (JobConstant.FILE_COLLECTION_IS_WRITE_HADOOP) {
                    copyFileToHDFS(avroFileAbsolutionPath, fileCollectHdfsPath);
                } else {
                    copyFileToRemote(avroFileAbsolutionPath, fileCollectHdfsPath);
                }
                if (IsFlag.Shi.getCode().equals(queueJb.get("isBigFile"))) {
                    continue;
                }
                List<AvroBean> avroBeans = ClassBase.hadoopInstance().getAvroBeans(fileCollectHdfsPath, avroFileAbsolutionPath);
                AvroBeanProcess abp = new AvroBeanProcess(fileCollectParamBean, sysDate, jobRsId);
                if (IsFlag.Shi.getCode().equals(fileCollectParamBean.getIs_solr())) {
                    abp.saveInSolr(avroBeans);
                }
                List<String[]> hbaseList = abp.saveMetaData(avroBeans, fileNameHTreeMap);
                saveInMapDB(avroBeans);
                boolean delete = FileUtils.getFile(avroFileAbsolutionPath).delete();
                if (!delete) {
                    log.error("删除本地文件" + avroFileAbsolutionPath + "失败！");
                }
                if (watcherId == AvroOper.LASTELEMENT) {
                    log.info("End FileCollectLoadingDataStageImpl Thread ...");
                    break;
                }
            }
        } catch (Exception e) {
            log.error("Failed to process Avro file in hdfs FileSystem...", e);
            message = e.getMessage();
        }
        return message;
    }

    private void saveInMapDB(List<AvroBean> avroBeans) {
        for (AvroBean bean : avroBeans) {
            Map<String, Object> object = new HashMap<>();
            object.put("uuid", bean.getUuid());
            object.put("file_md5", bean.getFile_md5());
            fileNameHTreeMap.put(bean.getFile_scr_path(), JsonUtil.toJson(object));
        }
        mapDBHelper.commit();
        log.info("提交到mapDB");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "localPath", desc = "", range = "")
    @Param(name = "hdfsPath", desc = "", range = "")
    private static void copyFileToHDFS(String localPath, String hdfsPath) {
        ClassBase.hadoopInstance().copyFileToHDFS(localPath, hdfsPath);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "localPath", desc = "", range = "")
    @Param(name = "hdfsPath", desc = "", range = "")
    private static void copyFileToRemote(String localPath, String remotePath) {
        SSHDetails sshDetails = new SSHDetails();
        sshDetails.setHost(PropertyParaUtil.getString("hyren_host", ""));
        sshDetails.setUser_name(PropertyParaUtil.getString("hyren_user", ""));
        sshDetails.setPwd(PropertyParaUtil.getString("hyren_pwd", ""));
        sshDetails.setPort(Integer.parseInt(PropertyParaUtil.getString("sftp_port", "22")));
        try (SSHOperate sshOperate = new SSHOperate(sshDetails)) {
            sshOperate.scpMkdir(remotePath);
            long fileSize = new File(localPath).length();
            sshOperate.channelSftp.put(localPath, remotePath, new FileProgressMonitor(fileSize), ChannelSftp.OVERWRITE);
            sshOperate.channelSftp.quit();
        } catch (Exception e) {
            log.error("拷贝本地文件到服务器: " + sshDetails.getHost() + " 失败! 执行异常: ", e.getMessage());
            if (e instanceof BusinessException) {
                throw (BusinessException) e;
            } else {
                throw new AppSystemException(e);
            }
        }
    }
}
