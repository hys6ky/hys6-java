package hyren.serv6.c.etlsys;

import com.jcraft.jsch.JSchException;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.c.util.ETLAgentDeployment;
import hyren.serv6.base.codes.*;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.jobUtil.EtlJobUtil;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.constant.CommonVariables;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import hyren.serv6.base.utils.fileutil.FileDownloadUtil;
import hyren.serv6.commons.utils.fileutil.read.ReadLog;
import hyren.serv6.base.utils.jsch.SSHDetails;
import hyren.serv6.base.utils.jsch.SSHOperate;
import hyren.serv6.base.utils.regular.RegexConstant;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.io.File.separator;

@Service
@Slf4j
public class EtlSysService {

    private static final Logger logger = LogManager.getLogger();

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys", desc = "", range = "", isBean = true)
    @Param(name = "etl_sys_dependency", desc = "", range = "", isBean = true, nullable = true)
    @Param(name = "pre_etl_sys_cds", desc = "", range = "", nullable = true)
    public void addEtlSys(EtlSys etl_sys, String status, Long[] pre_etl_sys_ids, Long userId) {
        checkEtlSysField(etl_sys.getEtl_sys_cd(), etl_sys.getEtl_sys_name());
        etl_sys.setEtl_sys_id(PrimayKeyGener.getNextId());
        etl_sys.setUser_id(userId);
        etl_sys.setBath_shift_time(DateUtil.getSysDate());
        etl_sys.setMain_serv_sync(Main_Server_Sync.YES.getCode());
        etl_sys.setSys_run_status(Job_Status.STOP.getCode());
        etl_sys.setCurr_bath_date(DateUtil.getSysDate());
        if (EtlJobUtil.isEtlSysExist(etl_sys.getEtl_sys_id(), Dbo.db())) {
            throw new BusinessException("工程编号已存在，不能新增！");
        }
        etl_sys.add(Dbo.db());
        String[] paraType = { Pro_Type.Thrift.getCode(), Pro_Type.Yarn.getCode(), Constant.NORMAL_DEFAULT_RESOURCE_TYPE };
        for (String para_type : paraType) {
            EtlResource resource = new EtlResource();
            resource.setEtl_sys_id(etl_sys.getEtl_sys_id());
            resource.setResource_type(para_type);
            resource.setMain_serv_sync(Main_Server_Sync.YES.getCode());
            resource.setResource_max(10);
            resource.setResource_used(0);
            if (para_type.equals(Pro_Type.Thrift.getCode())) {
                resource.setResource_name("Thrift作业资源类型");
            } else if (para_type.equals(Pro_Type.Yarn.getCode())) {
                resource.setResource_name("Yarn作业资源类型");
            } else {
                resource.setResource_name("普通作业资源类型");
            }
            resource.add(Dbo.db());
        }
        EtlSysDependency etl_sys_dependency = new EtlSysDependency();
        etl_sys_dependency.setEtl_sys_id(etl_sys.getEtl_sys_id());
        etl_sys_dependency.setStatus(status);
        saveEtlSysDep(etl_sys_dependency, pre_etl_sys_ids);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    public void deleteEtlProject(Long etl_sys_id) {
        if (Dbo.queryNumber("select count(1) from " + EtlSubSysList.TableName + "  WHERE etl_sys_id=?", etl_sys_id).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("该工程下还有任务，不能删除！");
        }
        if (Dbo.queryNumber("select count(1) from " + EtlJobDef.TableName + "  WHERE etl_sys_id=?", etl_sys_id).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("该工程下还有作业，不能删除！");
        }
        Dbo.execute("delete from " + EtlResource.TableName + " where etl_sys_id=?", etl_sys_id);
        Dbo.execute("delete from " + EtlPara.TableName + " where etl_sys_id=?", etl_sys_id);
        Dbo.execute("delete from " + EtlJobDispHis.TableName + " where etl_sys_id=?", etl_sys_id);
        Dbo.execute("delete from " + EtlJobHand.TableName + " where etl_sys_id=?", etl_sys_id);
        Dbo.execute("delete from " + EtlJobHandHis.TableName + " where etl_sys_id=?", etl_sys_id);
        Dbo.execute("delete from " + EtlJobCur.TableName + " where etl_sys_id=?", etl_sys_id);
        Dbo.execute("delete from " + EtlSysDependency.TableName + " where etl_sys_id=?", etl_sys_id);
        long count = Dbo.queryNumber("select count(1) from " + EtlErrorResource.TableName + " where etl_sys_id = ?", etl_sys_id).orElseThrow(() -> new BusinessException("sql查询错误！"));
        if (count != 0) {
            Dbo.execute("delete from " + EtlErrorResource.TableName + " where etl_sys_id=?", etl_sys_id);
        }
        DboExecute.deletesOrThrow("删除工程失败", "delete from " + EtlSys.TableName + " where etl_sys_id=?", etl_sys_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "etl_serv_ip", desc = "", range = "")
    @Param(name = "serv_file_path", desc = "", range = "", nullable = true)
    @Param(name = "user_name", desc = "", range = "")
    @Param(name = "user_pwd", desc = "", range = "")
    @Param(name = "isCustomize", desc = "", range = "")
    public void deployEtlJobScheduleProject(Long etl_sys_id, String etl_sys_cd, String etl_serv_ip, String serv_file_path, String user_name, String user_pwd, String isCustomize, Long userId) {
        EtlSys etlSys = EtlJobUtil.getEtlSysById(etl_sys_id, userId, Dbo.db());
        if (Job_Status.STOP != (Job_Status.ofEnumByCode(etlSys.getSys_run_status()))) {
            throw new BusinessException("系统不是停止状态不能部署");
        }
        if (IsFlag.Fou == IsFlag.ofEnumByCode(isCustomize)) {
            serv_file_path = PropertyParaValue.getString("etlDeployPath", "/home/hyshf/");
        } else {
            Validator.notBlank(serv_file_path, "部署目录选择自定义时不能为空，请检查");
        }
        ETLAgentDeployment.scpETLAgent(etl_sys_cd, etl_serv_ip, CommonVariables.SFTP_PORT, user_name, user_pwd, serv_file_path, etlSys.getServ_file_path());
        EtlSys etl_sys = new EtlSys();
        etl_sys.setEtl_sys_id(etl_sys_id);
        etl_sys.setEtl_serv_ip(etl_serv_ip);
        etl_sys.setEtl_sys_cd(etl_sys_cd);
        etl_sys.setUser_name(user_name);
        etl_sys.setUser_pwd(user_pwd);
        etl_sys.setServ_file_path(serv_file_path);
        etl_sys.setEtl_serv_port(String.valueOf(CommonVariables.SFTP_PORT));
        etl_sys.update(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "curr_bath_date", desc = "", range = "")
    @Param(name = "isControl", desc = "", range = "")
    public String downloadControlOrTriggerLog(Long etl_sys_id, String etl_sys_cd, String curr_bath_date, String isControl, Long userId) {
        try {
            EtlSys etlSys = EtlJobUtil.getEtlSysById(etl_sys_id, userId, Dbo.db());
            EtlJobUtil.isETLDeploy(etlSys);
            if (curr_bath_date.contains("-") && curr_bath_date.length() == 10) {
                curr_bath_date = StringUtil.replace(curr_bath_date, "-", "");
            }
            String serv_file_path = FilenameUtils.normalize(etlSys.getServ_file_path());
            String remoteLogPath = serv_file_path.endsWith(separator) ? serv_file_path + etl_sys_cd + separator : serv_file_path + separator + etl_sys_cd + separator;
            String fileName;
            if (IsFlag.Fou == IsFlag.ofEnumByCode(isControl)) {
                fileName = curr_bath_date + "_ControlLog.tar.gz";
                remoteLogPath = remoteLogPath + "control" + separator + curr_bath_date.substring(0, 4) + separator + curr_bath_date.substring(0, 6) + separator;
            } else {
                fileName = curr_bath_date + "_TriggerLog.tar.gz";
                remoteLogPath = remoteLogPath + "trigger" + separator + curr_bath_date.substring(0, 4) + separator + curr_bath_date.substring(0, 6) + separator;
            }
            String compressCommand = "tar -zvcPf " + remoteLogPath + fileName + " " + remoteLogPath + curr_bath_date + "*.log";
            SSHDetails sshDetails1 = new SSHDetails();
            EtlJobUtil.interactingWithTheAgentServer(compressCommand, etlSys, sshDetails1);
            String localPath = WebinfoProperties.FileUpload_SavedDirName;
            logger.info("==========control/trigger文件下载本地路径=========" + localPath);
            File localLogFile = new File(localPath);
            if (!localLogFile.exists()) {
                if (!localLogFile.mkdirs()) {
                    throw new BusinessException("创建目录失败：" + localLogFile.getAbsolutePath());
                }
            }
            remoteLogPath = remoteLogPath + fileName;
            localPath = localPath + fileName;
            FileDownloadUtil.downloadLogFile(remoteLogPath, localPath, sshDetails1);
            FileDownloadUtil.deleteLogFileBySFTP(remoteLogPath, sshDetails1);
            return fileName;
        } catch (IOException e) {
            throw new BusinessException("下载日志文件失败！");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getEtlSysDepById(Long etl_sys_id) {
        return Dbo.queryList(Dbo.db(), "select es.etl_sys_name,es.comments,esd.* from " + EtlSys.TableName + " es," + EtlSysDependency.TableName + " esd" + " where es.etl_sys_id = esd.etl_sys_id and es.etl_sys_id = ?", etl_sys_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "readNum", desc = "", range = "", valueIfNull = "100")
    @Param(name = "isControl", desc = "", range = "")
    @Return(desc = "", range = "")
    public String readControlOrTriggerLog(Long etl_sys_id, String etl_sys_cd, Integer readNum, String isControl, Long userId) {
        EtlSys etlSys = EtlJobUtil.getEtlSysById(etl_sys_id, userId, Dbo.db());
        EtlJobUtil.isETLDeploy(etlSys);
        String sysDate = DateUtil.getSysDate();
        String logDir = FilenameUtils.normalize(etlSys.getServ_file_path()) + separator + etl_sys_cd + separator;
        if (IsFlag.Fou == IsFlag.ofEnumByCode(isControl)) {
            logDir = logDir + "control" + separator + sysDate.substring(0, 4) + separator + sysDate.substring(0, 6) + separator + sysDate + "_ControlOut.log";
        } else {
            logDir = logDir + "trigger" + separator + sysDate.substring(0, 4) + separator + sysDate.substring(0, 6) + separator + sysDate + "_TriggerOut.log";
        }
        if (readNum > 1000) {
            readNum = 1000;
        }
        SSHDetails sshDetails = new SSHDetails();
        sshDetails.setUser_name(etlSys.getUser_name());
        sshDetails.setPwd(etlSys.getUser_pwd());
        sshDetails.setHost(etlSys.getEtl_serv_ip());
        sshDetails.setPort(Integer.parseInt(etlSys.getEtl_serv_port()));
        return ReadLog.readAgentLog(logDir, sshDetails, readNum);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_dependency", desc = "", range = "", isBean = true, nullable = true)
    @Param(name = "pre_etl_sys_cds", desc = "", range = "", nullable = true)
    public void saveEtlSysDep(EtlSysDependency etl_sys_dependency, Long[] pre_etl_sys_ids) {
        Dbo.execute("delete from " + EtlSysDependency.TableName + " where etl_sys_id = ?", etl_sys_dependency.getEtl_sys_id());
        if (pre_etl_sys_ids != null && pre_etl_sys_ids.length > 0) {
            for (Long pre_etl_sys_cd : pre_etl_sys_ids) {
                Status.ofEnumByCode(etl_sys_dependency.getStatus());
                etl_sys_dependency.setStatus(StringUtil.isBlank(etl_sys_dependency.getStatus()) ? Status.TRUE.getCode() : etl_sys_dependency.getStatus());
                etl_sys_dependency.setPre_etl_sys_id(pre_etl_sys_cd);
                etl_sys_dependency.setMain_serv_sync(Main_Server_Sync.YES.getCode());
                etl_sys_dependency.add(Dbo.db());
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result searchEtlSys(Long userId) {
        return Dbo.queryResult("select etl_sys_id,etl_sys_cd,etl_sys_name,comments,curr_bath_date,sys_run_status from " + EtlSys.TableName + " where user_id=? order by etl_sys_cd", userId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> searchEtlSysById(Long etl_sys_id, Long userId) {
        EtlSys etl_sys = EtlJobUtil.getEtlSysById(etl_sys_id, userId, Dbo.db());
        String etlDeployPath = PropertyParaValue.getString("etlDeployPath", "/home/hyshf/");
        Map<String, Object> etlSysMap = new HashMap<>();
        if (StringUtil.isBlank(etl_sys.getServ_file_path()) || etl_sys.getServ_file_path().equals(etlDeployPath)) {
            etlSysMap.put("isCustomize", IsFlag.Fou.getCode());
        } else {
            etlSysMap.put("isCustomize", IsFlag.Shi.getCode());
        }
        etlSysMap.put("etlSys", etl_sys);
        return etlSysMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result searchTable(String tableName) {
        return Dbo.queryResult("select d.datasource_number,d.datasource_name,b.database_id,c.agent_id" + ",c.agent_name,b.task_name,a.table_name,dsla.storage_property_val as jdbc_url " + "from table_info a " + "join database_set b on a.database_id = b.database_id " + "join agent_info c on b.agent_id = c.agent_id " + "join data_source d on c.source_id = d.source_id " + "join data_store_layer dsl on b.dsl_id = dsl.dsl_id " + "join data_store_layer_attr dsla on dsl.dsl_id = dsla.dsl_id " + "where lower(a.table_name)  like lower(?) AND dsla.storage_property_key = ?", tableName + "%", StorageTypeKey.jdbc_url);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "isResumeRun", desc = "", range = "")
    @Param(name = "isAutoShift", desc = "", range = "")
    @Param(name = "curr_bath_date", desc = "", range = "")
    public void startControl(Long etl_sys_id, String etl_sys_cd, String isResumeRun, String isAutoShift, String curr_bath_date, String sys_end_date, Long userId) {
        EtlSys etlSys = EtlJobUtil.getEtlSysById(etl_sys_id, userId, Dbo.db());
        EtlJobUtil.isETLDeploy(etlSys);
        if (IsFlag.Shi == IsFlag.ofEnumByCode(isResumeRun)) {
            curr_bath_date = etlSys.getCurr_bath_date();
        }
        Validator.notBlank(curr_bath_date, "当前批量日期不能为空");
        if (IsFlag.Fou == IsFlag.ofEnumByCode(isAutoShift)) {
            if (!EtlJobUtil.isEtlJObDefExistBySysCd(etl_sys_id, Dbo.db())) {
                throw new BusinessException("如果日切方式不是自动日切且工程下作业列表为空，则不能启动!");
            }
        }
        if (Job_Status.STOP != (Job_Status.ofEnumByCode(etlSys.getSys_run_status()))) {
            throw new BusinessException("系统不是停止状态不能启动control");
        }
        List<EtlJobDef> etlJobDefList = Dbo.queryList(EtlJobDef.class, "select * from " + EtlJobDef.TableName + " where etl_sys_id=?", etl_sys_id);
        for (EtlJobDef etl_job_def : etlJobDefList) {
            if (Dbo.queryNumber("select count(1) from " + EtlJobResourceRela.TableName + " where etl_sys_id=? and etl_job_id=?", etl_sys_id, etl_job_def.getEtl_job_id()).orElseThrow(() -> new BusinessException("sql查询错误")) != 1) {
                throw new BusinessException(etl_job_def.getEtl_job() + "作业没有分配资源，不能启动!");
            }
        }
        if (curr_bath_date.contains("-")) {
            curr_bath_date = curr_bath_date.replaceAll("-", "");
        }
        String etlJobChildId = EtlJobUtil.getEtlJobChildId(etl_sys_id, Dbo.db());
        ETLAgentDeployment.startEngineBatchControl(curr_bath_date, sys_end_date, etl_sys_cd, isResumeRun, isAutoShift, etlSys.getEtl_serv_ip(), etlSys.getEtl_serv_port(), etlSys.getUser_name(), etlSys.getUser_pwd(), etlSys.getServ_file_path(), userId, etlJobChildId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    public void startTrigger(Long etl_sys_id, String etl_sys_cd, Long userId) {
        EtlSys etlSys = EtlJobUtil.getEtlSysById(etl_sys_id, userId, Dbo.db());
        if (Job_Status.RUNNING != Job_Status.ofEnumByCode(etlSys.getSys_run_status())) {
            throw new BusinessException("CONTROL还未启动，不能启动TRIGGER");
        }
        ETLAgentDeployment.startEngineBatchTrigger(etl_sys_cd, etlSys.getEtl_serv_ip(), etlSys.getEtl_serv_port(), etlSys.getUser_name(), etlSys.getUser_pwd(), etlSys.getServ_file_path(), etlSys.getCurr_bath_date());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    public void stopEtlProject(Long etl_sys_id, String etl_sys_cd) {
        EtlSys etl_sys = Dbo.queryOneObject(EtlSys.class, "select * from " + EtlSys.TableName + " where etl_sys_id = ?", etl_sys_id).orElseThrow(() -> new BusinessException("sql查询错误," + etl_sys_id + "对应作业工程已不存在，请检查!"));
        SSHDetails sshDetails = new SSHDetails(etl_sys.getEtl_serv_ip(), etl_sys.getUser_name(), etl_sys.getUser_pwd(), Integer.parseInt(etl_sys.getEtl_serv_port()));
        try (SSHOperate sshOperate = new SSHOperate(sshDetails, 0)) {
            String etlJobChildId = EtlJobUtil.getEtlJobChildId(etl_sys_id, Dbo.db());
            String controlPath = etl_sys.getServ_file_path() + separator + etl_sys_cd + separator + "control";
            String triggerPath = etl_sys.getServ_file_path() + separator + etl_sys_cd + separator + "trigger";
            String stopControl = "source /etc/profile;source ~/.profile;source ~/.bashrc; cd " + controlPath + "/ ;" + "bash " + Constant.CONTROL_OPERATION_SH + " " + etl_sys.getCurr_bath_date() + " " + etl_sys_cd + " " + IsFlag.Fou.getCode() + " " + IsFlag.Fou.getCode() + Constant.SPACE + etl_sys.getSys_end_date() + " stop " + etlJobChildId;
            log.info("#########执行脚本停止CONTROL#############" + stopControl);
            String commandResult = sshOperate.execCommandBySSH(stopControl);
            log.info("######执行control脚本返回状态说明######" + commandResult);
            List<String> codeDesc = StringUtil.split(commandResult, ":");
            if (!"5".equals(codeDesc.get(codeDesc.size() - 2)) && !"6".equals(codeDesc.get(codeDesc.size() - 2))) {
                throw new BusinessException("停止control失败：" + codeDesc.get(codeDesc.size() - 1));
            }
            log.info("=======停止control成功========");
            String stopTrigger = "source /etc/profile;source ~/.bash_profile;source ~/.bashrc; cd " + triggerPath + "/ ;" + "bash " + Constant.TRIGGER_OPERATION_SH + " " + etl_sys_cd + " stop";
            log.info("######执行脚本停止TRIGGERR########" + stopTrigger);
            sshOperate.execCommandBySSHNoRs(stopTrigger);
        } catch (JSchException e) {
            log.error("连接失败，请确认用户名密码正确", e);
            throw new BusinessException("连接失败，请确认用户名密码正确" + e.getMessage());
        } catch (IOException e) {
            log.error("网络异常，请确认网络正常", e);
            throw new BusinessException("网络异常，请确认网络正常" + e.getMessage());
        } catch (Exception e) {
            throw new AppSystemException(etl_sys_cd + "工程停止失败！！！" + e);
        }
        DboExecute.updatesOrThrow("停止工程，更新系统运行状态失败", "update " + EtlSys.TableName + " set sys_run_status=? where etl_sys_id=?", Job_Status.STOP.getCode(), etl_sys_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "etl_sys_name", desc = "", range = "")
    @Param(name = "comments", desc = "", range = "", nullable = true)
    @Param(name = "etl_sys_dependency", desc = "", range = "", isBean = true, nullable = true)
    @Param(name = "pre_etl_sys_cds", desc = "", range = "", nullable = true)
    @Param(name = "etl_sys_id", desc = "", range = "", nullable = true)
    public void updateEtlSys(Long etl_sys_id, String etl_sys_cd, String etl_sys_name, String comments, String status, Long[] pre_etl_sys_cds, Long userId) {
        DboExecute.updatesOrThrow("更新保存失败!", "update " + EtlSys.TableName + " set etl_sys_cd=?,etl_sys_name=?,comments=? where etl_sys_id=? and user_id=?", etl_sys_cd, etl_sys_name, comments, etl_sys_id, userId);
        EtlSysDependency etl_sys_dependency = new EtlSysDependency();
        etl_sys_dependency.setEtl_sys_id(etl_sys_id);
        etl_sys_dependency.setStatus(status);
        saveEtlSysDep(etl_sys_dependency, pre_etl_sys_cds);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "etl_sys_name", desc = "", range = "")
    private void checkEtlSysField(String etl_sys_cd, String etl_sys_name) {
        Validator.notBlank(etl_sys_name, "作业调度工程名不能为空！");
        boolean matcher = RegexConstant.matcher(RegexConstant.NUMBER_ENGLISH_UNDERSCORE, etl_sys_cd);
        if (!matcher) {
            throw new BusinessException("工程编号只能为数字英文下划线！");
        }
    }
}
