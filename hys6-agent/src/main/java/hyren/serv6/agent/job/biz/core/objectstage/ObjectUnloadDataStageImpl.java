package hyren.serv6.agent.job.biz.core.objectstage;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.FileNameUtils;
import hyren.serv6.agent.job.biz.bean.ObjectCollectParamBean;
import hyren.serv6.agent.job.biz.bean.ObjectTableBean;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;
import hyren.serv6.agent.job.biz.bean.StageStatusInfo;
import hyren.serv6.agent.job.biz.constant.RunStatusConstant;
import hyren.serv6.agent.job.biz.core.AbstractJobStage;
import hyren.serv6.agent.job.biz.core.metaparse.impl.ObjectCollectTableHandleParse;
import hyren.serv6.agent.job.biz.utils.FileUtil;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.enumtools.StageConstant;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.io.FilenameFilter;
import java.util.regex.Pattern;

@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/10/24 11:43")
public class ObjectUnloadDataStageImpl extends AbstractJobStage {

    private final ObjectCollectParamBean objectCollectParamBean;

    private final ObjectTableBean objectTableBean;

    public ObjectUnloadDataStageImpl(ObjectCollectParamBean objectCollectParamBean, ObjectTableBean objectTableBean) {
        this.objectCollectParamBean = objectCollectParamBean;
        this.objectTableBean = objectTableBean;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public StageParamInfo handleStage(StageParamInfo stageParamInfo) {
        long startTime = System.currentTimeMillis();
        log.info("------------------表" + objectTableBean.getEn_name() + "半结构化对象采集卸数阶段开始---------------");
        StageStatusInfo statusInfo = new StageStatusInfo();
        JobStatusInfoUtil.startStageStatusInfo(statusInfo, objectTableBean.getOcs_id(), StageConstant.UNLOADDATA.getCode());
        try {
            TableBean tableBean = new ObjectCollectTableHandleParse().generateTableInfo(objectCollectParamBean, objectTableBean);
            String filePathPattern = objectCollectParamBean.getFile_path() + File.separator + objectTableBean.getEtlDate() + File.separator + objectTableBean.getEn_name() + "_" + objectTableBean.getEtlDate() + "." + objectCollectParamBean.getFile_suffix();
            String file_path = FileNameUtils.getFullPath(filePathPattern);
            String regex = FileNameUtils.getName(filePathPattern);
            String[] file_name_list = new File(file_path).list(new FilenameFilter() {

                private final Pattern pattern = Pattern.compile(regex);

                @Override
                public boolean accept(File dir, String name) {
                    return pattern.matcher(name).matches();
                }
            });
            if (file_name_list != null && file_name_list.length > 0) {
                long fileSize = 0;
                String[] file_path_list = new String[file_name_list.length];
                for (int i = 0; i < file_name_list.length; i++) {
                    file_path_list[i] = file_path + file_name_list[i];
                    if (FileUtil.decideFileExist(file_path_list[i])) {
                        long singleFileSize = FileUtil.getFileSize(file_path_list[i]);
                        fileSize += singleFileSize;
                    } else {
                        throw new AppSystemException(file_path_list[i] + "文件不存在");
                    }
                }
                stageParamInfo.setFileArr(file_path_list);
                stageParamInfo.setFileSize(fileSize);
                stageParamInfo.setFileNameArr(file_name_list);
            } else {
                throw new AppSystemException("半结构化对象" + objectTableBean.getEn_name() + "数据字典指定目录" + file_path + "下通过正则" + regex + "匹配不到对应的数据文件");
            }
            stageParamInfo.setTableBean(tableBean);
            log.info(objectTableBean.getEn_name() + "半结构化对象采集，不需要转存，卸数跳过");
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.SUCCEED.getCode(), "执行成功");
            log.info("------------------表" + objectTableBean.getEn_name() + "半结构化对象采集卸数阶段成功------------------执行时间为：" + (System.currentTimeMillis() - startTime) / 1000 + "，秒");
        } catch (Exception e) {
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.FAILED.getCode(), e.getMessage());
            log.error(objectTableBean.getEn_name() + "半结构化对象采集卸数阶段失败：", e);
        }
        JobStatusInfoUtil.endStageParamInfo(stageParamInfo, statusInfo, objectTableBean, AgentType.DuiXiang.getCode());
        return stageParamInfo;
    }

    @Override
    public int getStageCode() {
        return StageConstant.UNLOADDATA.getCode();
    }
}
