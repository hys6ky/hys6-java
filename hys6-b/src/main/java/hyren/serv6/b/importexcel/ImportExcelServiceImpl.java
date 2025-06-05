package hyren.serv6.b.importexcel;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.*;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.agent.CheckParam;
import hyren.serv6.b.batchcollection.agent.startwayconf.StartWayConfService;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.entity.fdentity.ProEntity;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.jobUtil.beans.EtlJobInfo;
import hyren.serv6.commons.jobUtil.dcletljob.DclEtlJobUtil;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.database.DatabaseConnUtil;
import hyren.serv6.commons.utils.database.bean.DBConnectionProp;
import hyren.serv6.commons.utils.xlstoxml.util.ExcelUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.util.*;

@Service
@Slf4j
public class ImportExcelServiceImpl {

    private final static String EXCEL_FILEPATH = "importExcel.xlsx";

    public Map<Object, Object> importDatabaseByExcel(MultipartFile file, String upload) {
        Workbook workbookFromExcel = null;
        File uploadedFile = null;
        Map<Object, Object> uploadData = new HashMap<>();
        String originalFilename = file.getOriginalFilename();
        try {
            boolean isUpload = Boolean.parseBoolean(upload);
            if (null == originalFilename) {
                throw new BusinessException("上传文件不存在！");
            }
            uploadedFile = new File(WebinfoProperties.FileUpload_SavedDirName + File.separator + originalFilename);
            file.transferTo(uploadedFile);
            if (!uploadedFile.exists()) {
                throw new BusinessException("上传文件不存在！");
            }
            workbookFromExcel = ExcelUtil.getWorkbookFromExcel(uploadedFile);
            DatabaseWrapper db = Dbo.db();
            Sheet sheet = workbookFromExcel.getSheetAt(0);
            DepartmentInfo department_info = saveDeptInfo(sheet, db, isUpload);
            DataSource data_source = saveDataSource(sheet, department_info, db, isUpload);
            AgentInfo agent_info = saveAgentInfo(sheet, data_source, db, isUpload);
            CollectJobClassify classify = saveClassifyInfo(sheet, agent_info, db, isUpload);
            DatabaseSet database_set = saveDatabaseSetInfo(sheet, classify, agent_info, db, isUpload);
            sheet = workbookFromExcel.getSheetAt(1);
            Map<String, Object> tableMap = saveTableInfo(sheet, database_set, data_source, agent_info, classify, db, isUpload);
            uploadData.put("table", tableMap);
            sheet = workbookFromExcel.getSheetAt(2);
            if (isUpload) {
                saveJobDefInfo(sheet, database_set.getDatabase_id(), data_source.getSource_id(), db, agent_info.getUser_id());
            }
            if (!isUpload) {
                List<Object> tableList = (List<Object>) tableMap.get("tableList");
                Map<Object, List<Map<String, Object>>> jobInfo = getJobInfo(tableList, database_set.getDatabase_id(), sheet);
                uploadData.put("etlJob", jobInfo);
                Map<Object, List<Map<String, Object>>> dclTable = getDclTable(tableList, database_set.getDatabase_id(), agent_info.getAgent_id());
                uploadData.put("dclTable", dclTable);
            }
        } catch (IOException e) {
            CheckParam.throwErrorMsg("未找到上传递的文件(%s)信息", file.getOriginalFilename());
        } finally {
            if (workbookFromExcel != null) {
                ExcelUtil.close(workbookFromExcel);
                uploadedFile.delete();
            }
        }
        return uploadData;
    }

    void saveJobDefInfo(Sheet sheet, long database_id, long source_id, DatabaseWrapper db, long user_id) {
        Row row = sheet.getRow(1);
        String startType = ExcelUtil.getValue(row.getCell(0)).toString();
        if (startType.equals(IsFlag.Shi.getValue())) {
        } else if (startType.equals(IsFlag.Fou.getValue())) {
            EtlSys etl_sys = new EtlSys();
            String etl_sys_cd = ExcelUtil.getValue(row.getCell(2)).toString();
            Validator.notBlank(etl_sys_cd, "作业工程编号不能为空");
            String etl_sys_name = ExcelUtil.getValue(row.getCell(3)).toString();
            Validator.notBlank(etl_sys_name, "作业工程描述不能为空");
            etl_sys.setEtl_sys_name(StringUtil.isBlank(etl_sys_name) ? etl_sys_cd : etl_sys_name);
            etl_sys.setUser_id(user_id);
            Optional<EtlSys> queryEtlSys = Dbo.queryOneObject(EtlSys.class, "SELECT * FROM " + EtlSys.TableName + " WHERE etl_sys_cd = ? AND user_id = ?", etl_sys_cd, user_id);
            if (!queryEtlSys.isPresent()) {
                etl_sys.setEtl_sys_id(PrimayKeyGener.getNextId());
                etl_sys.setEtl_sys_cd(etl_sys_cd);
                etl_sys.setSys_run_status(Job_Status.STOP.getCode());
                etl_sys.setCurr_bath_date(DateUtil.getSysDate());
                etl_sys.add(db);
            } else {
                try {
                    etl_sys.setEtl_sys_cd(queryEtlSys.get().getEtl_sys_cd());
                    etl_sys.update(db);
                } catch (Exception e) {
                    if (!(e instanceof ProEntity.EntityDealZeroException)) {
                        throw new BusinessException(e.getMessage());
                    }
                }
            }
            EtlSubSysList etl_sub_sys_list = new EtlSubSysList();
            String sub_sys_cd = ExcelUtil.getValue(row.getCell(4)).toString();
            Validator.notBlank(sub_sys_cd, "作业任务编号不能为空");
            String sub_sys_desc = ExcelUtil.getValue(row.getCell(5)).toString();
            Validator.notBlank(sub_sys_desc, "作业任务名称不能为空");
            etl_sub_sys_list.setSub_sys_desc(sub_sys_desc);
            Optional<EtlSubSysList> querySubSys = Dbo.queryOneObject(EtlSubSysList.class, "SELECT * FROM " + EtlSubSysList.TableName + " WHERE etl_sys_cd = ? AND sub_sys_cd = ?", etl_sys_cd, sub_sys_cd);
            if (!querySubSys.isPresent()) {
                etl_sub_sys_list.setSub_sys_id(PrimayKeyGener.getNextId());
                etl_sub_sys_list.setEtl_sys_id(etl_sys.getEtl_sys_id());
                etl_sub_sys_list.setSub_sys_cd(sub_sys_cd);
                etl_sub_sys_list.add(db);
            } else {
                try {
                    etl_sub_sys_list.setEtl_sys_id(querySubSys.get().getEtl_sys_id());
                    etl_sub_sys_list.setSub_sys_cd(querySubSys.get().getSub_sys_cd());
                    etl_sub_sys_list.update(db);
                } catch (Exception e) {
                    if (!(e instanceof ProEntity.EntityDealZeroException)) {
                        throw new BusinessException(e.getMessage());
                    }
                }
            }
            String pro_dic = ExcelUtil.getValue(row.getCell(6)).toString();
            Validator.notBlank(pro_dic, "作业程序目录不能为空");
            String log_dic = ExcelUtil.getValue(row.getCell(7)).toString();
            Validator.notBlank(log_dic, "作业日志目录不能为空");
            StartWayConfService startWayConfAction = new StartWayConfService();
            List<Map<String, Object>> previewJob = startWayConfAction.getPreviewJob(database_id);
            List<EtlJobDef> jobDefList = new ArrayList<>();
            List<Long> ded_id = new ArrayList<>();
            previewJob.forEach(itemMap -> {
                EtlJobDef etl_job_def = JsonUtil.toObject(JsonUtil.toJson(itemMap), new TypeReference<EtlJobDef>() {
                });
                long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + EtlJobDef.TableName + " WHERE etl_job = ? AND etl_sys_cd = ?", etl_job_def.getEtl_job(), etl_sys_cd).orElseThrow(() -> new BusinessException("SQL错误"));
                if (countNum == 0) {
                    etl_job_def.setEtl_sys_id(etl_sys_cd);
                    etl_job_def.setSub_sys_id(sub_sys_cd);
                    etl_job_def.setPro_type(Pro_Type.SHELL.getCode());
                    etl_job_def.setPro_name(Constant.COLLECT_JOB_COMMAND);
                    etl_job_def.setDisp_type(Dispatch_Type.TPLUS0.getCode());
                    etl_job_def.setDisp_time(DateUtil.parseStr2TimeWith6Char(DateUtil.getSysTime()).toString());
                    jobDefList.add(etl_job_def);
                    ded_id.add(Long.parseLong(itemMap.get("ded_id").toString()));
                }
            });
            EtlJobInfo etlJobInfo = new EtlJobInfo();
            etlJobInfo.setSource_id(source_id);
            etlJobInfo.setEtl_sys_id(etl_sys.getEtl_sys_id());
            etlJobInfo.setSub_sys_id(etl_sub_sys_list.getSub_sys_id());
            etlJobInfo.setPro_dic(pro_dic);
            etlJobInfo.setLog_dic(log_dic);
            etlJobInfo.setEtlJobs(jobDefList);
            etlJobInfo.setDedIds(ded_id);
            etlJobInfo.setJobRelations("");
            DclEtlJobUtil.saveJobDataToDatabase(etlJobInfo, Dbo.db());
        } else {
            CheckParam.throwErrorMsg("启动方式选择错误");
        }
    }

    Map<Object, List<Map<String, Object>>> getJobInfo(List<Object> tableNameList, long database_id, Sheet sheet) {
        String etl_sys_cd = ExcelUtil.getValue(sheet.getRow(1).getCell(2)).toString();
        Validator.notBlank(etl_sys_cd, "作业工程编号不能为空");
        String sub_sys_cd = ExcelUtil.getValue(sheet.getRow(1).getCell(4)).toString();
        Validator.notBlank(sub_sys_cd, "作业任务编号不能为空");
        Map<Object, List<Map<String, Object>>> dependDataMap = new LinkedHashMap<>();
        tableNameList.forEach(tableName -> {
            List<Map<String, Object>> tableIdList = Dbo.queryList("SELECT etl_job FROM " + TakeRelationEtl.TableName + " t1 JOIN " + TableInfo.TableName + " t3 ON t3.database_id = t1.database_id WHERE t3.table_name = ?" + " AND t3.database_id = ? AND t1.etl_sys_cd = ?", tableName, database_id, etl_sys_cd);
            List<Map<String, Object>> treeList = new ArrayList<>();
            Map<String, Object> rootMap = new LinkedHashMap<>();
            rootMap.put("id", tableName);
            rootMap.put("isroot", true);
            rootMap.put("topic", tableName);
            rootMap.put("background-color", "red");
            treeList.add(rootMap);
            for (Map<String, Object> tableIdMap : tableIdList) {
                treeList.addAll(getTreeData(tableName, etl_sys_cd, tableIdMap.get("etl_job")));
            }
            dependDataMap.put(tableName, treeList);
        });
        return dependDataMap;
    }

    List<Map<String, Object>> getTreeData(Object tableName, String etl_sys_cd, Object etl_job) {
        List<Map<String, Object>> preJob = new ArrayList<>();
        List<Object> preJobList = Dbo.queryOneColumnList("SELECT pre_etl_job from " + EtlDependency.TableName + " WHERE etl_job = ? AND etl_sys_cd = ?", etl_job, etl_sys_cd);
        preJobList.forEach(pre_etl_job -> {
            Map<String, Object> preMap = new LinkedHashMap<>();
            preMap.put("id", pre_etl_job);
            preMap.put("topic", pre_etl_job);
            preMap.put("direction", "left");
            preMap.put("parentid", tableName);
            preMap.put("'background-color'", "green");
            preJob.add(preMap);
        });
        List<Object> nextJobList = Dbo.queryOneColumnList("SELECT etl_job from " + EtlDependency.TableName + " WHERE pre_etl_job = ? AND etl_sys_cd = ?", etl_job, etl_sys_cd);
        nextJobList.forEach(itemJob -> {
            Map<String, Object> nextMap = new LinkedHashMap<>();
            nextMap.put("id", itemJob);
            nextMap.put("topic", itemJob);
            nextMap.put("direction", "right");
            nextMap.put("parentid", tableName);
            nextMap.put("background-color", "#0000ff");
            preJob.add(nextMap);
        });
        return preJob;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sheet", desc = "", range = "")
    @Return(desc = "", range = "")
    DepartmentInfo saveDeptInfo(Sheet sheet, DatabaseWrapper db, boolean isUpload) {
        Row row = sheet.getRow(2);
        DepartmentInfo department_info = new DepartmentInfo();
        String dep_name = ExcelUtil.getValue(row.getCell(2)).toString();
        Validator.notBlank(dep_name, "部门名称不能为空");
        department_info.setDep_name(dep_name);
        String dep_remark = String.valueOf(ExcelUtil.getValue(row.getCell(4)));
        department_info.setDep_remark(dep_remark);
        Optional<DepartmentInfo> queryResult = Dbo.queryOneObject(DepartmentInfo.class, "SELECT * FROM " + DepartmentInfo.TableName + " WHERE dep_name = ?", dep_name);
        ;
        if (!queryResult.isPresent()) {
            department_info.setDep_id(PrimayKeyGener.getNextId());
            department_info.setCreate_date(DateUtil.getSysDate());
            department_info.setCreate_time(DateUtil.getSysTime());
            if (isUpload) {
                department_info.add(db);
            }
        } else {
            try {
                department_info.setDep_id(queryResult.get().getDep_id());
                if (isUpload) {
                    department_info.update(db);
                }
            } catch (Exception e) {
                if (!(e instanceof ProEntity.EntityDealZeroException)) {
                    throw new BusinessException(e.getMessage());
                }
            }
        }
        return department_info;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sheet", desc = "", range = "")
    @Param(name = "dept", desc = "", range = "")
    @Return(desc = "", range = "")
    DataSource saveDataSource(Sheet sheet, DepartmentInfo dept, DatabaseWrapper db, boolean isUpload) {
        Row row = sheet.getRow(5);
        DataSource data_source = new DataSource();
        String datasource_number = ExcelUtil.getValue(row.getCell(2)).toString();
        Validator.notBlank(datasource_number, "数据源编号不能为空");
        data_source.setDatasource_number(datasource_number);
        String datasource_name = ExcelUtil.getValue(row.getCell(4)).toString();
        Validator.notBlank(datasource_name, "数据源名称不能为空");
        data_source.setDatasource_name(datasource_name);
        row = sheet.getRow(6);
        String source_remark = ExcelUtil.getValue(row.getCell(2)).toString();
        data_source.setSource_remark(source_remark);
        Optional<DataSource> queryResult = Dbo.queryOneObject(DataSource.class, "SELECT * FROM " + DataSource.TableName + " WHERE datasource_number = ?", datasource_number);
        if (!queryResult.isPresent()) {
            data_source.setSource_id(PrimayKeyGener.getNextId());
            data_source.setCreate_date(DateUtil.getSysDate());
            data_source.setCreate_time(DateUtil.getSysTime());
            data_source.setCreate_user_id(UserUtil.getUserId());
            if (isUpload) {
                data_source.add(db);
            }
        } else {
            try {
                data_source.setSource_id(queryResult.get().getSource_id());
                if (isUpload) {
                    data_source.update(db);
                }
            } catch (Exception e) {
                if (!(e instanceof ProEntity.EntityDealZeroException)) {
                    throw new BusinessException(e.getMessage());
                }
            }
        }
        if (Dbo.queryNumber("SELECT COUNT(1) FROM " + SourceRelationDep.TableName + " WHERE dep_id = ? AND source_id = ?", dept.getDep_id(), data_source.getSource_id()).orElseThrow(() -> new BusinessException("获取部门和数据源关系失败")) == 0) {
            SourceRelationDep source_relation_dep = new SourceRelationDep();
            source_relation_dep.setDep_id(dept.getDep_id());
            source_relation_dep.setSource_id(data_source.getSource_id());
            if (isUpload) {
                source_relation_dep.add(Dbo.db());
            }
        }
        return data_source;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sheet", desc = "", range = "")
    @Param(name = "data_source", desc = "", range = "")
    @Return(desc = "", range = "")
    AgentInfo saveAgentInfo(Sheet sheet, DataSource data_source, DatabaseWrapper db, boolean isUpload) {
        Row row = sheet.getRow(9);
        AgentInfo agent_info = new AgentInfo();
        agent_info.setSource_id(data_source.getSource_id());
        String agent_name = ExcelUtil.getValue(row.getCell(2)).toString();
        Validator.notBlank(agent_name, "Agent名称不能为空");
        agent_info.setAgent_name(agent_name);
        String agent_type = ExcelUtil.getValue(row.getCell(4)).toString();
        Validator.notBlank(agent_type, "Agent类型不能为空");
        if (agent_type.equals(AgentType.ShuJuKu.getValue())) {
            agent_info.setAgent_type(AgentType.ShuJuKu.getCode());
        } else if (agent_type.equals(AgentType.DBWenJian.getValue())) {
            agent_info.setAgent_type(AgentType.DBWenJian.getCode());
        } else if (agent_type.equals(AgentType.WenJianXiTong.getValue())) {
            agent_info.setAgent_type(AgentType.WenJianXiTong.getCode());
        } else if (agent_type.equals(AgentType.DuiXiang.getValue())) {
            agent_info.setAgent_type(AgentType.DuiXiang.getCode());
        } else if (agent_type.equals(AgentType.FTP.getValue())) {
            agent_info.setAgent_type(AgentType.FTP.getCode());
        } else {
            throw new BusinessException("请选择正确的Agent类型");
        }
        row = sheet.getRow(10);
        String agent_ip = ExcelUtil.getValue(row.getCell(2)).toString();
        Validator.notBlank(agent_ip, "AgentIp不能为空");
        agent_info.setAgent_ip(agent_ip);
        String agent_port = ExcelUtil.getValue(row.getCell(4)).toString();
        Validator.notBlank(agent_port, "Agent端口不能为空");
        agent_info.setAgent_port(agent_port);
        row = sheet.getRow(11);
        String user_id = ExcelUtil.getValue(row.getCell(2)).toString();
        Validator.notBlank(user_id, "Agent所属用户不能为空");
        agent_info.setUser_id(Long.valueOf(user_id));
        Optional<AgentInfo> queryResult = Dbo.queryOneObject(AgentInfo.class, "SELECT * FROM " + AgentInfo.TableName + " WHERE agent_name = ? AND source_id =? AND agent_type = ?", agent_name, data_source.getSource_id(), agent_info.getAgent_type());
        if (!queryResult.isPresent()) {
            agent_info.setAgent_id(PrimayKeyGener.getNextId());
            agent_info.setAgent_status(AgentStatus.WeiLianJie.getCode());
            agent_info.setCreate_date(DateUtil.getSysDate());
            agent_info.setCreate_time(DateUtil.getSysTime());
            if (isUpload) {
                agent_info.add(db);
            }
        } else {
            try {
                agent_info.setAgent_id(queryResult.get().getAgent_id());
                if (isUpload) {
                    agent_info.update(db);
                }
            } catch (Exception e) {
                if (!(e instanceof ProEntity.EntityDealZeroException)) {
                    throw new BusinessException(e.getMessage());
                }
            }
        }
        return agent_info;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sheet", desc = "", range = "")
    @Param(name = "agent_info", desc = "", range = "")
    @Return(desc = "", range = "")
    CollectJobClassify saveClassifyInfo(Sheet sheet, AgentInfo agent_info, DatabaseWrapper db, boolean isUpload) {
        Row row = sheet.getRow(14);
        CollectJobClassify classify = new CollectJobClassify();
        classify.setUser_id(agent_info.getUser_id());
        classify.setAgent_id(agent_info.getAgent_id());
        String classify_num = ExcelUtil.getValue(row.getCell(2)).toString();
        Validator.notBlank(classify_num, "分类编号不能为空");
        classify.setClassify_num(classify_num);
        String classify_name = ExcelUtil.getValue(row.getCell(4)).toString();
        Validator.notBlank(classify_name, "分类名称不能为空");
        classify.setClassify_name(classify_name);
        row = sheet.getRow(15);
        String remark = ExcelUtil.getValue(row.getCell(2)).toString();
        classify.setRemark(remark);
        Optional<CollectJobClassify> queryClassify = Dbo.queryOneObject(CollectJobClassify.class, "SELECT * FROM " + CollectJobClassify.TableName + " WHERE classify_num = ? AND agent_id =?", classify_num, agent_info.getAgent_id());
        if (!queryClassify.isPresent()) {
            classify.setClassify_id(PrimayKeyGener.getNextId());
            if (isUpload) {
                classify.add(db);
            }
        } else {
            try {
                classify.setClassify_id(queryClassify.get().getClassify_id());
                if (isUpload) {
                    classify.update(db);
                }
            } catch (Exception e) {
                if (!(e instanceof ProEntity.EntityDealZeroException)) {
                    throw new BusinessException(e.getMessage());
                }
            }
        }
        return classify;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sheet", desc = "", range = "")
    @Param(name = "agent_info", desc = "", range = "")
    @Param(name = "classify", desc = "", range = "")
    @Return(desc = "", range = "")
    DatabaseSet saveDatabaseSetInfo(Sheet sheet, CollectJobClassify classify, AgentInfo agent_info, DatabaseWrapper db, boolean isUpload) {
        DatabaseSet database_set = new DatabaseSet();
        database_set.setClassify_id(classify.getClassify_id());
        database_set.setAgent_id(agent_info.getAgent_id());
        if (AgentType.ShuJuKu == AgentType.ofEnumByCode(agent_info.getAgent_type())) {
            Row row = sheet.getRow(18);
            String task_name = ExcelUtil.getValue(row.getCell(2)).toString();
            Validator.notBlank(task_name, "任务名称不能为空");
            database_set.setTask_name(task_name);
            String database_number = ExcelUtil.getValue(row.getCell(4)).toString();
            database_set.setDatabase_number(database_number);
            row = sheet.getRow(19);
            String databaseType = ExcelUtil.getValue(row.getCell(2)).toString();
            Validator.notBlank(databaseType, "数据库类型不能为空");
            DBConnectionProp dbConnectionProp = DatabaseConnUtil.getConnParamInfo(databaseType, null);
            String database_name = ExcelUtil.getValue(row.getCell(4)).toString();
            Validator.notBlank(database_name, "数据库名称不能为空");
            row = sheet.getRow(20);
            String database_ip = ExcelUtil.getValue(row.getCell(2)).toString();
            Validator.notBlank(database_ip, "数据库IP不能为空");
            String database_port = ExcelUtil.getValue(row.getCell(4)).toString();
            Validator.notBlank(database_port, "数据库端口不能为空");
            row = sheet.getRow(21);
            String database_user = ExcelUtil.getValue(row.getCell(2)).toString();
            Validator.notBlank(database_user, "数据库用户名称不能为空");
            String database_pwd = ExcelUtil.getValue(row.getCell(4)).toString();
            Validator.notBlank(database_pwd, "数据库用户密码不能为空");
            row = sheet.getRow(22);
            String collectType = ExcelUtil.getValue(row.getCell(2)).toString();
            Validator.notBlank(databaseType, "数据库类型不能为空");
            if (collectType.equals(CollectType.ShuJuKuCaiJi.getValue())) {
                database_set.setCollect_type(CollectType.ShuJuKuCaiJi.getCode());
            } else if (collectType.equals(CollectType.ShuJuKuChouShu.getValue())) {
                database_set.setCollect_type(CollectType.ShuJuKuChouShu.getCode());
            } else if (collectType.equals(CollectType.TieYuanDengJi.getValue())) {
                database_set.setCollect_type(CollectType.TieYuanDengJi.getCode());
            } else {
                throw new BusinessException("请选择正确的数据库采集类型");
            }
            row = sheet.getRow(23);
            String jdbc_url = ExcelUtil.getValue(row.getCell(2)).toString();
            Validator.notBlank(database_pwd, "JDBC的连接信息不能为空");
        }
        if (AgentType.DBWenJian == AgentType.ofEnumByCode(agent_info.getAgent_type())) {
            Row row = sheet.getRow(26);
            String task_name = ExcelUtil.getValue(row.getCell(2)).toString();
            Validator.notBlank(task_name, "任务名称不能为空");
            database_set.setTask_name(task_name);
            String database_number = ExcelUtil.getValue(row.getCell(4)).toString();
            Validator.notBlank(database_number, "采集作业编号不能为空");
            database_set.setDatabase_number(database_number);
            row = sheet.getRow(27);
            String plane_url = ExcelUtil.getValue(row.getCell(2)).toString();
            Validator.notBlank(plane_url, "数据字典文件目录不能为空");
            database_set.setPlane_url(plane_url);
            database_set.setCollect_type(agent_info.getAgent_type());
        }
        Optional<DatabaseSet> queryDatabase = Dbo.queryOneObject(DatabaseSet.class, "SELECT * FROM " + DatabaseSet.TableName + " WHERE agent_id = ? AND classify_id = ? AND task_name = ? AND database_number = ?", agent_info.getAgent_id(), classify.getClassify_id(), database_set.getTask_name(), database_set.getDatabase_number());
        if (!queryDatabase.isPresent()) {
            database_set.setDb_agent(IsFlag.Fou.getCode());
            if (AgentType.DBWenJian == AgentType.ofEnumByCode(agent_info.getAgent_type())) {
                database_set.setDb_agent(IsFlag.Shi.getCode());
            }
            database_set.setIs_sendok(IsFlag.Fou.getCode());
            database_set.setDatabase_id(PrimayKeyGener.getNextId());
            database_set.setCp_or(Constant.DATABASE_CLEAN.toString());
            if (isUpload) {
                database_set.add(db);
            }
        } else {
            try {
                database_set.setDatabase_id(queryDatabase.get().getDatabase_id());
                if (isUpload) {
                    database_set.update(db);
                }
            } catch (Exception e) {
                if (!(e instanceof ProEntity.EntityDealZeroException)) {
                    throw new BusinessException(e.getMessage());
                }
            }
        }
        return database_set;
    }

    Map<String, Object> saveTableInfo(Sheet sheet, DatabaseSet database_set, DataSource data_source, AgentInfo agent_info, CollectJobClassify classify, DatabaseWrapper db, boolean isUpload) {
        int lastRowNum = sheet.getLastRowNum();
        Row row;
        TableInfo table_info;
        Map<String, Object> updateDataMaps = new HashMap<>();
        List<String> tableList = new ArrayList<>();
        for (int i = 1; i <= lastRowNum; i++) {
            Map<String, Object> updateDataObj = new LinkedHashMap<>();
            table_info = new TableInfo();
            row = sheet.getRow(i);
            String table_name = ExcelUtil.getValue(row.getCell(0)).toString();
            Validator.notBlank(table_name, "第" + i + "行,表名不能为空");
            table_info.setTable_name(table_name);
            String table_ch_name = ExcelUtil.getValue(row.getCell(1)).toString();
            Validator.notBlank(table_ch_name, "第" + i + "行,表中文名不能为空");
            table_info.setTable_ch_name(table_ch_name);
            table_info.setIs_register(IsFlag.Shi.getCode());
            table_info.setIs_user_defined(IsFlag.Fou.getCode());
            if (AgentType.ShuJuKu == AgentType.ofEnumByCode(agent_info.getAgent_type())) {
                if (CollectType.ShuJuKuChouShu == CollectType.ofEnumByCode(database_set.getCollect_type())) {
                    String unload_type = ExcelUtil.getValue(row.getCell(2)).toString();
                    Validator.notBlank(unload_type, "第" + i + "行,卸数方式不能为空");
                    if (unload_type.equals(UnloadType.ZengLiangXieShu.getValue())) {
                        table_info.setUnload_type(UnloadType.ZengLiangXieShu.getCode());
                        String sql = ExcelUtil.getValue(row.getCell(7)).toString();
                        Validator.notBlank(sql, "第" + i + "行,增量SQL不能为空");
                        table_info.setSql(sql);
                        table_info.setIs_parallel(IsFlag.Fou.getCode());
                        table_info.setIs_customize_sql(IsFlag.Fou.getCode());
                    } else if (unload_type.equals(UnloadType.QuanLiangXieShu.getValue())) {
                        table_info.setUnload_type(UnloadType.QuanLiangXieShu.getCode());
                        setIsParallelParams(row, table_info, i);
                    } else {
                        CheckParam.throwErrorMsg("第" + i + "行,请选择正确的并行抽取");
                    }
                    String md5 = ExcelUtil.getValue(row.getCell(8)).toString();
                    Validator.notBlank(md5, "第" + i + "行,是否计算MD5不能为空");
                    if (md5.equals(IsFlag.Shi.getValue())) {
                        table_info.setIs_md5(IsFlag.Shi.getCode());
                    } else if (md5.equals(IsFlag.Fou.getValue())) {
                        table_info.setIs_md5(IsFlag.Fou.getCode());
                    } else {
                        CheckParam.throwErrorMsg("第" + i + "行,请选择是否计算MD5");
                    }
                    dealTableInfo(database_set, Dbo.db(), isUpload, agent_info, table_info, updateDataObj, table_name);
                } else if (CollectType.ShuJuKuCaiJi == CollectType.ofEnumByCode(database_set.getCollect_type())) {
                    table_info.setIs_md5(IsFlag.Fou.getCode());
                    setIsParallelParams(row, table_info, i);
                    dealTableInfo(database_set, Dbo.db(), isUpload, agent_info, table_info, updateDataObj, table_name);
                    tableStorageInfoAndRelation(data_source, classify, db, isUpload, row, table_info, updateDataObj);
                }
            }
            if (AgentType.DBWenJian == AgentType.ofEnumByCode(agent_info.getAgent_type())) {
                table_info.setIs_md5(IsFlag.Fou.getCode());
                table_info.setIs_customize_sql(IsFlag.Fou.getCode());
                table_info.setIs_parallel(IsFlag.Fou.getCode());
                table_info.setUnload_type(UnloadType.QuanLiangXieShu.getCode());
                dealTableInfo(database_set, Dbo.db(), isUpload, agent_info, table_info, updateDataObj, table_name);
                tableStorageInfoAndRelation(data_source, classify, db, isUpload, row, table_info, updateDataObj);
            }
            tableColumnData(row, i, table_info, database_set, db, updateDataObj, isUpload);
            dataExtractionDef(database_set, db, isUpload, row, agent_info, table_info, i, updateDataObj);
            if (!updateDataObj.isEmpty()) {
                updateDataMaps.put(table_name, updateDataObj);
                tableList.add(table_name);
            }
        }
        updateDataMaps.put("tableList", tableList);
        return updateDataMaps;
    }

    private void dataExtractionDef(DatabaseSet database_set, DatabaseWrapper db, boolean isUpload, Row row, AgentInfo agent_info, TableInfo table_info, int i, Map<String, Object> updateDataObj) {
        DataExtractionDef extraction_def = new DataExtractionDef();
        extraction_def.setIs_archived(IsFlag.Fou.getCode());
        if (AgentType.DBWenJian == AgentType.ofEnumByCode(agent_info.getAgent_type())) {
            extraction_def.setData_extract_type(DataExtractType.YuanShuJuGeShi.getCode());
            String dbFile_format = setExtractDef(row, i, extraction_def);
            if (dbFile_format.equals(FileFormat.CSV.getValue()) || dbFile_format.equals(FileFormat.DingChang.getValue()) || dbFile_format.equals(FileFormat.FeiDingChang.getValue())) {
                String database_separator = ExcelUtil.getValue(row.getCell(14)).toString();
                Validator.notBlank(database_separator, "第" + i + "行数据分隔符不能为空");
                extraction_def.setDatabase_separatorr(StringUtil.string2Unicode(database_separator));
            } else {
                extraction_def.setDatabase_separatorr("");
            }
            String plane_url = ExcelUtil.getValue(row.getCell(22)).toString();
            Validator.notBlank(plane_url, "第" + i + "行,源文件储存路径不能为空");
            extraction_def.setPlane_url(plane_url);
        }
        if (AgentType.ShuJuKu == AgentType.ofEnumByCode(agent_info.getAgent_type())) {
            if (CollectType.ShuJuKuChouShu == CollectType.ofEnumByCode(database_set.getCollect_type())) {
                extraction_def.setData_extract_type(DataExtractType.ShuJuKuChouQuLuoDi.getCode());
                String dbFile_format = setExtractDef(row, i, extraction_def);
                String plane_url = ExcelUtil.getValue(row.getCell(12)).toString();
                Validator.notBlank(plane_url, "第" + i + "行,数据落地目录不能为空");
                extraction_def.setPlane_url(plane_url);
                if (dbFile_format.equals(FileFormat.CSV.getValue()) || dbFile_format.equals(FileFormat.DingChang.getValue()) || dbFile_format.equals(FileFormat.FeiDingChang.getValue())) {
                    String row_separator = ExcelUtil.getValue(row.getCell(13)).toString();
                    Validator.notBlank(row_separator, "第" + i + "行,行分隔符不能为空");
                    extraction_def.setRow_separator(StringUtil.string2Unicode(row_separator));
                    String database_separator = ExcelUtil.getValue(row.getCell(14)).toString();
                    Validator.notBlank(database_separator, "第" + i + "行数据分隔符不能为空");
                    extraction_def.setDatabase_separatorr(StringUtil.string2Unicode(database_separator));
                } else {
                    extraction_def.setRow_separator("");
                    extraction_def.setDatabase_separatorr("");
                }
            }
            if (CollectType.ShuJuKuCaiJi == CollectType.ofEnumByCode(database_set.getCollect_type())) {
                extraction_def.setTable_id(table_info.getTable_id());
                extraction_def.setData_extract_type(DataExtractType.YuanShuJuGeShi.getCode());
                extraction_def.setIs_header(IsFlag.Fou.getCode());
                extraction_def.setDatabase_code(DataBaseCode.UTF_8.getCode());
                extraction_def.setDbfile_format(FileFormat.PARQUET.getCode());
            }
        }
        Optional<DataExtractionDef> queryData = Dbo.queryOneObject(DataExtractionDef.class, "SELECT * FROM " + DataExtractionDef.TableName + " WHERE table_id = ? ", table_info.getTable_id());
        if (!queryData.isPresent()) {
            extraction_def.setDed_id(PrimayKeyGener.getNextId());
            extraction_def.setTable_id(table_info.getTable_id());
            updateDataObj.put("addTableChName新增表中文名", table_info.getTable_ch_name());
            updateDataObj.put("addTableType新增表落地方式", FileFormat.ofValueByCode(extraction_def.getDbfile_format()));
            if (isUpload) {
                extraction_def.add(db);
            }
        } else {
            try {
                extraction_def.setDed_id(queryData.get().getDed_id());
                if (!extraction_def.getDbfile_format().equals(queryData.get().getDbfile_format())) {
                    updateDataObj.put("updateType更新后表落地方式", FileFormat.ofValueByCode(extraction_def.getDbfile_format()));
                    updateDataObj.put("originType原表落地方式", FileFormat.ofValueByCode(queryData.get().getDbfile_format()));
                }
                if (isUpload) {
                    extraction_def.update(db);
                }
            } catch (Exception e) {
                if (!(e instanceof ProEntity.EntityDealZeroException)) {
                    throw new BusinessException(e.getMessage());
                }
            }
        }
    }

    private String setExtractDef(Row row, int i, DataExtractionDef extraction_def) {
        String is_header = ExcelUtil.getValue(row.getCell(9)).toString();
        Validator.notBlank(is_header, "第" + i + "行,是否有表头不能为空");
        if (is_header.equals(IsFlag.Shi.getValue())) {
            extraction_def.setIs_header(IsFlag.Shi.getCode());
        } else if (is_header.equals(IsFlag.Fou.getValue())) {
            extraction_def.setIs_header(IsFlag.Shi.getCode());
        } else {
            CheckParam.throwErrorMsg("第" + i + "行,请选择正确的是否有表头信息");
        }
        String database_code = ExcelUtil.getValue(row.getCell(10)).toString();
        Validator.notBlank(database_code, "第" + i + "行,数据字符集不能为空");
        if (database_code.equals(DataBaseCode.UTF_8.getValue())) {
            extraction_def.setDatabase_code(DataBaseCode.UTF_8.getCode());
        } else if (database_code.equals(DataBaseCode.GBK.getValue())) {
            extraction_def.setDatabase_code(DataBaseCode.GBK.getCode());
        } else if (database_code.equals(DataBaseCode.UTF_16.getValue())) {
            extraction_def.setDatabase_code(DataBaseCode.UTF_16.getCode());
        } else if (database_code.equals(DataBaseCode.GB2312.getValue())) {
            extraction_def.setDatabase_code(DataBaseCode.GB2312.getCode());
        } else if (database_code.equals(DataBaseCode.ISO_8859_1.getValue())) {
            extraction_def.setDatabase_code(DataBaseCode.ISO_8859_1.getCode());
        } else {
            CheckParam.throwErrorMsg("第" + i + "行,请选择正确的数据字符集信息");
        }
        String dbFile_format = ExcelUtil.getValue(row.getCell(11)).toString();
        Validator.notBlank(dbFile_format, "第" + i + "行,文件抽取格式不能为空");
        if (dbFile_format.equals(FileFormat.CSV.getValue())) {
            extraction_def.setDbfile_format(FileFormat.CSV.getCode());
        } else if (dbFile_format.equals(FileFormat.FeiDingChang.getValue())) {
            extraction_def.setDbfile_format(FileFormat.FeiDingChang.getCode());
        } else if (dbFile_format.equals(FileFormat.DingChang.getValue())) {
            extraction_def.setDbfile_format(FileFormat.DingChang.getCode());
        } else if (dbFile_format.equals(FileFormat.SEQUENCEFILE.getValue())) {
            extraction_def.setDbfile_format(FileFormat.SEQUENCEFILE.getCode());
        } else if (dbFile_format.equals(FileFormat.ORC.getValue())) {
            extraction_def.setDbfile_format(FileFormat.ORC.getCode());
        } else if (dbFile_format.equals(FileFormat.PARQUET.getValue())) {
            extraction_def.setDbfile_format(FileFormat.PARQUET.getCode());
        } else {
            CheckParam.throwErrorMsg("第" + i + "行,请选择正确的文件抽取格式信息");
        }
        return dbFile_format;
    }

    private void setIsParallelParams(Row row, TableInfo table_info, int i) {
        String is_parallel = ExcelUtil.getValue(row.getCell(3)).toString();
        Validator.notBlank(is_parallel, "第" + i + "行,是否并行抽取不能为空");
        if (is_parallel.equals(IsFlag.Shi.getValue())) {
            table_info.setIs_parallel(IsFlag.Shi.getCode());
            String is_customize_sql = ExcelUtil.getValue(row.getCell(4)).toString();
            Validator.notBlank(is_customize_sql, "第" + i + "行,自定义SQL不能为空");
            if (is_customize_sql.equals(IsFlag.Shi.getValue())) {
                table_info.setIs_customize_sql(IsFlag.Shi.getCode());
                String page_sql = ExcelUtil.getValue(row.getCell(5)).toString();
                Validator.notBlank(page_sql, "第" + i + "行,分页SQL不能为空");
                table_info.setPage_sql(page_sql);
                table_info.setTable_count("0");
                table_info.setDataincrement(0);
                table_info.setPageparallels(0);
            } else if (is_customize_sql.equals(IsFlag.Fou.getValue())) {
                table_info.setIs_customize_sql(IsFlag.Fou.getCode());
                String customizeData = ExcelUtil.getValue(row.getCell(6)).toString();
                Validator.notBlank(customizeData, "第" + i + "行,自定义数据量不能为空");
                if (!customizeData.contains("|")) {
                    CheckParam.throwErrorMsg("第" + i + "行,自定义数据量分隔符不正确");
                }
                List<String> customizeDataList = StringUtil.split(customizeData, "|");
                if (customizeDataList.size() != 3) {
                    CheckParam.throwErrorMsg("第" + i + "行,自定义数据量参数不正确");
                }
                table_info.setTable_count(customizeDataList.get(0));
                table_info.setDataincrement(Integer.valueOf(customizeDataList.get(1)));
                table_info.setPageparallels(Integer.valueOf(customizeDataList.get(2)));
                table_info.setPage_sql("");
            } else {
                CheckParam.throwErrorMsg("第" + i + "行,请选择正确的自定义SQL方式");
            }
        } else if (is_parallel.equals(IsFlag.Fou.getValue())) {
            table_info.setIs_customize_sql(IsFlag.Fou.getCode());
            table_info.setIs_parallel(IsFlag.Fou.getCode());
            table_info.setTable_count("0");
            table_info.setDataincrement(0);
            table_info.setPageparallels(0);
            table_info.setPage_sql("");
        } else {
            CheckParam.throwErrorMsg("第" + i + "行,请选择正确的卸数方式");
        }
    }

    private void dealTableInfo(DatabaseSet database_set, DatabaseWrapper db, boolean isUpload, AgentInfo agent_info, TableInfo table_info, Map<String, Object> updateDataObj, String table_name) {
        Optional<TableInfo> queryTableInfo = Dbo.queryOneObject(TableInfo.class, "SELECT * FROM " + TableInfo.TableName + " WHERE database_id = ? AND table_name = ?", database_set.getDatabase_id(), table_name);
        if (!queryTableInfo.isPresent()) {
            table_info.setRec_num_date(DateUtil.getSysDate());
            table_info.setTable_id(PrimayKeyGener.getNextId());
            table_info.setTi_or(Constant.DEFAULT_TABLE_CLEAN_ORDER.toString());
            table_info.setValid_s_date(DateUtil.getSysDate());
            table_info.setValid_e_date(Constant._MAX_DATE_8);
            table_info.setDatabase_id(database_set.getDatabase_id());
            if (isUpload) {
                table_info.add(db);
            }
            updateDataObj.put("addTableName新增表名", table_info.getTable_name());
            updateDataObj.put("addTableChName新增表中文名", table_info.getTable_ch_name());
            if (AgentType.ShuJuKu == AgentType.ofEnumByCode(agent_info.getAgent_type()) && CollectType.ShuJuKuChouShu == CollectType.ofEnumByCode(database_set.getCollect_type())) {
                updateDataObj.put("addUnloadType新增卸数方式", UnloadType.ofValueByCode(table_info.getUnload_type()));
            }
        } else {
            try {
                table_info.setTable_id(queryTableInfo.get().getTable_id());
                if (isUpload) {
                    table_info.update(db);
                }
                if (!queryTableInfo.get().getTable_ch_name().equals(table_info.getTable_ch_name())) {
                    updateDataObj.put("originTableName原表中文名", queryTableInfo.get().getTable_ch_name());
                    updateDataObj.put("updateTableChName更新后表中文名", table_info.getTable_ch_name());
                }
                if (AgentType.ShuJuKu == AgentType.ofEnumByCode(agent_info.getAgent_type()) && CollectType.ShuJuKuChouShu == CollectType.ofEnumByCode(database_set.getCollect_type())) {
                    String unload_type = queryTableInfo.get().getUnload_type();
                    if (!unload_type.equals(table_info.getUnload_type())) {
                        updateDataObj.put("originUnloadType原卸数方式", UnloadType.ofValueByCode(unload_type));
                        updateDataObj.put("updateUnloadType更新后卸数方式", UnloadType.ofValueByCode(table_info.getUnload_type()));
                    }
                }
            } catch (Exception e) {
                if (!(e instanceof ProEntity.EntityDealZeroException)) {
                    throw new BusinessException(e.getMessage());
                }
            }
        }
    }

    private void tableStorageInfoAndRelation(DataSource data_source, CollectJobClassify classify, DatabaseWrapper db, boolean isUpload, Row row, TableInfo table_info, Map<String, Object> updateDataObj) {
        TableStorageInfo table_storage_info = new TableStorageInfo();
        table_storage_info.setTable_id(table_info.getTable_id());
        String is_zipper = ExcelUtil.getValue(row.getCell(19)).toString();
        Validator.notBlank(is_zipper, "是否拉链存储不能为空");
        table_storage_info.setIs_zipper(IsFlag.getCodeByValue(is_zipper));
        String storage_type = ExcelUtil.getValue(row.getCell(20)).toString();
        if (is_zipper.equals(IsFlag.Fou.getValue())) {
            if (!storage_type.equals(StorageType.TiHuan.getValue()) && !storage_type.equals(StorageType.ZhuiJia.getValue())) {
                throw new BusinessException("请检查存储方式是否正确");
            }
        } else {
            if (!storage_type.equals(StorageType.QuanLiang.getValue()) && !storage_type.equals(StorageType.ZengLiang.getValue())) {
                throw new BusinessException("请检查存储方式是否正确");
            }
        }
        for (StorageType typeCode : StorageType.values()) {
            if (typeCode.getValue().equals(storage_type)) {
                storage_type = typeCode.getCode();
                break;
            }
        }
        table_storage_info.setStorage_type(storage_type);
        String storage_time = ExcelUtil.getValue(row.getCell(21)).toString();
        Validator.notBlank(storage_time, "数据保留天数不能为空");
        table_storage_info.setStorage_time(Long.parseLong(storage_time));
        table_storage_info.setHyren_name(data_source.getDatasource_number() + Constant.SPLITTER + classify.getClassify_num() + Constant.SPLITTER + table_info.getTable_name());
        Optional<TableStorageInfo> tableStorageInfo = Dbo.queryOneObject(TableStorageInfo.class, "SELECT * FROM " + TableStorageInfo.TableName + " WHERE table_id = ? AND hyren_name = ?", table_info.getTable_id(), table_storage_info.getHyren_name());
        if (!tableStorageInfo.isPresent()) {
            if (isUpload) {
                table_storage_info.setStorage_id(PrimayKeyGener.getNextId());
                table_storage_info.add(Dbo.db());
            }
            updateDataObj.put("addStorageType新增进数方式", storage_type);
            updateDataObj.put("addHyrenName新增进库后拼接表名", table_storage_info.getHyren_name());
        } else {
            if (isUpload) {
                table_storage_info.setStorage_id(tableStorageInfo.get().getStorage_id());
                table_storage_info.update(db);
            }
            if (!tableStorageInfo.get().getHyren_name().equals(table_storage_info.getHyren_name())) {
                updateDataObj.put("originStorageType原进数方式", tableStorageInfo.get().getHyren_name());
                updateDataObj.put("updateStorageType更新后进数方式", table_storage_info.getHyren_name());
            }
            if (!tableStorageInfo.get().getStorage_type().equals(table_info.getUnload_type())) {
                updateDataObj.put("originHyrenName原库后拼接表名", storage_type);
                updateDataObj.put("updateHyrenName更新后库后拼接表名", StorageType.ofValueByCode(table_storage_info.getStorage_type()));
            }
        }
        DtabRelationStore dtab_relation_store = new DtabRelationStore();
        String dsl_name = ExcelUtil.getValue(row.getCell(18)).toString();
        Validator.notBlank(dsl_name, "选择存储目的地不能为空");
        DataStoreLayer data_store_layer = Dbo.queryOneObject(DataStoreLayer.class, "select * from " + DataStoreLayer.TableName + " where  dsl_name=?", dsl_name).orElseThrow(() -> new BusinessException("sql查询错误"));
        dtab_relation_store.setDsl_id(data_store_layer.getDsl_id());
        dtab_relation_store.setTab_id(table_storage_info.getStorage_id());
        dtab_relation_store.setIs_successful(IsFlag.Fou.getCode());
        dtab_relation_store.setData_source(StoreLayerDataSource.DB.getCode());
        Optional<DtabRelationStore> dtabRelationStore = Dbo.queryOneObject(DtabRelationStore.class, "SELECT * FROM " + DtabRelationStore.TableName + " WHERE dsl_id = ? AND tab_id = ?", data_store_layer.getDsl_id(), dtab_relation_store.getTab_id());
        if (!dtabRelationStore.isPresent()) {
            if (isUpload) {
                dtab_relation_store.add(Dbo.db());
            }
            updateDataObj.put("addDslName新增存储层-数据来源", dsl_name);
        } else {
            if (isUpload) {
                dtab_relation_store.update(db);
            }
            DataStoreLayer dataStoreLayer = Dbo.queryOneObject(DataStoreLayer.class, "select * from " + DataStoreLayer.TableName + " where dsl_id = ?", dtabRelationStore.get().getDsl_id()).orElseThrow(() -> new BusinessException("sql查询错误"));
            if (!dtabRelationStore.get().getDsl_id().equals(dtab_relation_store.getDsl_id())) {
                updateDataObj.put("originDslName原进数方式", dataStoreLayer.getDsl_name());
                updateDataObj.put("updateDslName更新后进数方式", dsl_name);
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "row", desc = "", range = "")
    @Param(name = "rowNum", desc = "", range = "")
    @Param(name = "table_info", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Return(desc = "", range = "")
    void tableColumnData(Row row, int rowNum, TableInfo table_info, DatabaseSet database_set, DatabaseWrapper db, Map<String, Object> updateDataMap, boolean isUpload) {
        String columns = ExcelUtil.getValue(row.getCell(15)).toString();
        Validator.notBlank(columns, "第" + rowNum + "行,字段信息未填写");
        List<String> columnList = StringUtil.split(columns, "|");
        String columnTypes = ExcelUtil.getValue(row.getCell(16)).toString();
        Validator.notBlank(columnTypes, "第" + rowNum + "行,列字段类型未填写");
        List<String> columnTypeList = StringUtil.split(columnTypes, "|");
        String primaryData = ExcelUtil.getValue(row.getCell(17)).toString();
        Validator.notBlank(primaryData, "第" + rowNum + "行,主键信息未填写");
        List<String> primaryList = StringUtil.split(primaryData, "|");
        if (columnTypeList.size() != columnList.size()) {
            CheckParam.throwErrorMsg("第" + rowNum + "行,字段数量(%s)和字段类型数量(%s)不一致", columnList.size(), columnTypeList.size());
        }
        if (primaryList.size() != columnTypeList.size()) {
            CheckParam.throwErrorMsg("第" + rowNum + "行,字段类型数量(%s)和主键数量(%s)不一致", columnTypeList.size(), primaryList.size());
        }
        if (CollectType.ShuJuKuChouShu.getValue().equals(database_set.getCollect_type()) && table_info.getUnload_type().equals(UnloadType.ZengLiangXieShu.getCode())) {
            if (!primaryList.contains(IsFlag.Shi.getCode())) {
                CheckParam.throwErrorMsg("第" + rowNum + "行,表(%s)的卸数方式为增量,未设置主键信息", table_info.getTable_name());
            }
        }
        List<Map<String, Object>> array = new ArrayList<>();
        TableColumn table_column = new TableColumn();
        for (int i = 0; i < columnList.size(); i++) {
            Map<String, Object> column = new LinkedHashMap<>();
            table_column.setIs_get(IsFlag.Shi.getCode());
            table_column.setColumn_name(columnList.get(i));
            table_column.setColumn_ch_name(columnList.get(i));
            table_column.setColumn_type(columnTypeList.get(i));
            table_column.setIs_new(IsFlag.Fou.getCode());
            table_column.setIs_primary_key(primaryList.get(i));
            Optional<TableColumn> queryColumn = Dbo.queryOneObject(TableColumn.class, "SELECT * FROM " + TableColumn.TableName + " WHERE table_id = ? AND column_name = ?", table_info.getTable_id(), columnList.get(i));
            if (!queryColumn.isPresent()) {
                table_column.setColumn_id(PrimayKeyGener.getNextId());
                table_column.setTc_or(Constant.DEFAULT_COLUMN_CLEAN_ORDER.toString());
                table_column.setValid_s_date(DateUtil.getSysDate());
                table_column.setValid_e_date(Constant._MAX_DATE_8);
                table_column.setTable_id(table_info.getTable_id());
                if (isUpload) {
                    table_column.add(db);
                }
                column.put("addColumnName字段", columnList.get(i));
                column.put("addColumn新增字段中文名称", table_column.getColumn_ch_name());
                column.put("addColumnType新增字段类型", table_column.getColumn_type());
            } else {
                try {
                    table_column.setColumn_id(queryColumn.get().getColumn_id());
                    if (isUpload) {
                        table_column.update(db);
                    }
                    if (!table_column.getColumn_ch_name().equals(queryColumn.get().getColumn_ch_name())) {
                        column.put("columnName字段", columnList.get(i));
                        column.put("originColumnCh原字段中文名称", queryColumn.get().getColumn_ch_name());
                        column.put("updateColumn更新字段中文名称", table_column.getColumn_ch_name());
                    }
                    if (!table_column.getColumn_type().equals(queryColumn.get().getColumn_type())) {
                        column.put("columnName字段", columnList.get(i));
                        column.put("originColumnType原字段类型", queryColumn.get().getColumn_type());
                        column.put("updateColumnType更新字段类型", table_column.getColumn_type());
                    }
                } catch (Exception e) {
                    if (!(e instanceof ProEntity.EntityDealZeroException)) {
                        throw new BusinessException(e.getMessage());
                    }
                }
            }
            if (!column.isEmpty()) {
                array.add(column);
            }
        }
        if (!array.isEmpty()) {
            updateDataMap.put("columnInfo", array);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "datatable_en_name", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<Object, List<Map<String, Object>>> getDclTable(List<Object> tableNameList, long database_id, long agent_id) {
        Map<Object, List<Map<String, Object>>> dependDataMap = new LinkedHashMap<>();
        tableNameList.forEach(tableName -> {
            List<Map<String, Object>> treeList = new ArrayList<>();
            Map<String, Object> rootMap = new LinkedHashMap<>();
            rootMap.put("id", tableName);
            rootMap.put("isroot", true);
            rootMap.put("topic", tableName);
            rootMap.put("background-color", "red");
            treeList.add(rootMap);
            Map<String, Object> map = Dbo.queryOneObject("SELECT hyren_name FROM " + DataStoreReg.TableName + " WHERE table_name = ? and database_id = ? and agent_id = ?", tableName, database_id, agent_id);
            if (map.get("hyren_name") == null) {
                return;
            }
            List<Object> dependTableList = Dbo.queryOneColumnList("SELECT module_table_en_name FROM " + DmModuleTable.TableName + " t1 JOIN " + DmDatatableSource.TableName + " t2 ON " + "t1.module_table_id = t2.module_table_id WHERE t2.own_source_table_name = ?", map.get("hyren_name"));
            dependTableList.forEach(datatable_en_name -> {
                Map<String, Object> nextMap = new LinkedHashMap<>();
                nextMap.put("id", datatable_en_name);
                nextMap.put("topic", datatable_en_name);
                nextMap.put("direction", "right");
                nextMap.put("parentid", tableName);
                nextMap.put("background-color", "#0000ff");
                treeList.add(nextMap);
            });
            dependDataMap.put(tableName, treeList);
        });
        return dependDataMap;
    }

    public void downloadExcel() {
        String path = System.getProperty("user.dir") + File.separator + EXCEL_FILEPATH;
        if (StringUtil.isBlank(path) || !new File(path).exists()) {
            throw new AppSystemException(String.format("Excel模板不存在:%s", path));
        }
        HttpServletResponse response = ContextDataHolder.getResponse();
        HttpServletRequest request = ContextDataHolder.getRequest();
        BufferedInputStream bis = null;
        BufferedOutputStream bos = null;
        try (OutputStream out = response.getOutputStream()) {
            response.reset();
            if (request.getHeader("User-Agent").toLowerCase().indexOf("firefox") > 0) {
                response.setHeader("content-disposition", "attachment;filename=" + new String(EXCEL_FILEPATH.getBytes(), CodecUtil.GBK_STRING));
            } else {
                response.setHeader("content-disposition", "attachment;filename=" + URLEncoder.encode(EXCEL_FILEPATH, CodecUtil.UTF8_STRING));
            }
            response.setContentType("APPLICATION/OCTET-STREAM");
            bis = new BufferedInputStream(Files.newInputStream(new File(path).toPath()));
            bos = new BufferedOutputStream(out);
            byte[] buff = new byte[2048];
            int bytesRead;
            while (-1 != (bytesRead = bis.read(buff, 0, buff.length))) {
                bos.write(buff, 0, bytesRead);
            }
            bos.flush();
        } catch (IOException e) {
            throw new AppSystemException(e);
        } finally {
            if (bis != null) {
                try {
                    bis.close();
                } catch (IOException e) {
                    throw new AppSystemException(e);
                }
            }
            if (bos != null) {
                try {
                    bos.close();
                } catch (IOException e) {
                    throw new AppSystemException(e);
                }
            }
        }
    }
}
