package hyren.serv6.c.etlmonitor;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.*;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.Dispatch_Frequency;
import hyren.serv6.base.codes.Dispatch_Type;
import hyren.serv6.base.codes.Job_Status;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.base.utils.fileutil.FileDownloadUtil;
import hyren.serv6.base.utils.jsch.SSHDetails;
import hyren.serv6.c.entity.JobBean;
import hyren.serv6.c.entity.ProjectBean;
import hyren.serv6.c.entity.TaskBean;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.commons.jobUtil.EtlJobUtil;
import hyren.serv6.commons.utils.fileutil.read.ReadLog;
import it.uniroma1.dis.wsngroup.gexf4j.core.*;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.Attribute;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeClass;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeList;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.viz.PositionImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.springframework.stereotype.Service;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
@Slf4j
public class MonitorService {

    private static final String FILE_NAME = "bachWorkTime.xlsx";

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Param(name = "curr_bath_date", desc = "", range = "")
    public String downHistoryJobLog(Long etl_sys_id, String etl_job, String curr_bath_date, Long userId) {
        try {
            if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
                throw new BusinessException("当前工程已不存在");
            }
            EtlJobCur etlJob = SqlOperator.queryOneObject(Dbo.db(), EtlJobCur.class, "select * from " + EtlJobCur.TableName + " where etl_job = ?", etl_job).orElseThrow(() -> (new BusinessException("未找到作业")));
            if (curr_bath_date.contains("-") && curr_bath_date.length() == 10) {
                curr_bath_date = StringUtil.replace(curr_bath_date, "-", "");
            }
            List<Object> logDicList = Dbo.queryOneColumnList("select log_dic FROM " + EtlJobDispHis.TableName + " where etl_sys_id=? AND etl_job=? and " + " curr_bath_date=? order by curr_end_time desc", etl_sys_id, etl_job, curr_bath_date);
            if (logDicList.isEmpty()) {
                return null;
            }
            String logDir = logDicList.get(0).toString();
            if (!logDir.endsWith("/")) {
                logDir = logDir + File.separator;
            }
            String compressCommand = "tar -zvcPf " + logDir + curr_bath_date + "_" + etlJob.getEtl_job() + ".tar.gz" + " " + logDir + etlJob.getEtl_job() + "_" + curr_bath_date + "*.log ";
            EtlSys etlSysInfo = EtlJobUtil.getEtlSysById(etl_sys_id, userId, Dbo.db());
            EtlJobUtil.isETLDeploy(etlSysInfo);
            SSHDetails sshDetails = new SSHDetails();
            EtlJobUtil.interactingWithTheAgentServer(compressCommand, etlSysInfo, sshDetails);
            String remoteFileName = curr_bath_date + "_" + etlJob.getEtl_job() + ".tar.gz";
            String localPath = WebinfoProperties.FileUpload_SavedDirName + File.separator;
            log.info("==========历史日志文件下载本地路径=========" + localPath + remoteFileName);
            if (logDir.endsWith(File.separator)) {
                logDir = logDir + File.separator + remoteFileName;
            } else {
                logDir = logDir + remoteFileName;
            }
            FileDownloadUtil.downloadLogFile(logDir, localPath + remoteFileName, sshDetails);
            FileDownloadUtil.deleteLogFileBySFTP(logDir, sshDetails);
            return remoteFileName;
        } catch (IOException e) {
            throw new BusinessException("下载日志文件失败！");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "projectData", desc = "", range = "")
    @Param(name = "taskData", desc = "", range = "")
    @Param(name = "jobData", desc = "", range = "")
    public void generateExcel(String projectData, String taskData, String jobData) {
        try {
            List<ProjectBean> projectBeans = JsonUtil.toObject(projectData, new TypeReference<List<ProjectBean>>() {
            });
            List<TaskBean> taskBeans = JsonUtil.toObject(taskData, new TypeReference<List<TaskBean>>() {
            });
            List<JobBean> jobBeans = JsonUtil.toObject(jobData, new TypeReference<List<JobBean>>() {
            });
            HSSFWorkbook wb = new HSSFWorkbook();
            HSSFSheet sheet1 = wb.createSheet("工程耗时记总表");
            HSSFCellStyle style = wb.createCellStyle();
            style.setBorderBottom(BorderStyle.THICK);
            style.setBorderLeft(BorderStyle.THICK);
            style.setBorderTop(BorderStyle.THICK);
            style.setBorderRight(BorderStyle.THICK);
            style.setAlignment(HorizontalAlignment.CENTER);
            HSSFRow rowHead1 = sheet1.createRow(1);
            HSSFCell cellHead1 = rowHead1.createCell(0);
            cellHead1.setCellStyle(style);
            HSSFRow row1 = sheet1.createRow(0);
            HSSFCell cell1 = row1.createCell(0);
            cell1.setCellValue("序号");
            cell1.setCellStyle(style);
            cell1 = row1.createCell(1);
            cell1.setCellValue("开始时间");
            cell1.setCellStyle(style);
            cell1 = row1.createCell(2);
            cell1.setCellValue("结束时间");
            cell1.setCellStyle(style);
            cell1 = row1.createCell(3);
            cell1.setCellValue("总任务数");
            cell1.setCellStyle(style);
            cell1 = row1.createCell(4);
            cell1.setCellValue("总作业数");
            cell1.setCellStyle(style);
            cell1 = row1.createCell(5);
            cell1.setCellValue("总用时");
            cell1.setCellStyle(style);
            cell1 = row1.createCell(6);
            cell1.setCellValue("平均耗时");
            cell1.setCellStyle(style);
            if (projectBeans.size() > 0) {
                for (int i = 0; i < projectBeans.size(); i++) {
                    row1 = sheet1.createRow(i + 1);
                    row1.createCell(0).setCellValue(i + 1);
                    row1.createCell(1).setCellValue(projectBeans.get(0).getCurr_st_time());
                    row1.createCell(2).setCellValue(projectBeans.get(0).getCurr_end_time());
                    row1.createCell(3).setCellValue(projectBeans.get(0).getTaskNum());
                    row1.createCell(4).setCellValue(projectBeans.get(0).getJobNum());
                    row1.createCell(5).setCellValue(projectBeans.get(0).getProjectConsumeTime());
                    row1.createCell(6).setCellValue(projectBeans.get(0).getProjectConsumeAveTime());
                    for (int j = 0; j <= cell1.getColumnIndex(); j++) {
                        Cell cell = row1.getCell(j);
                        cell.setCellStyle(style);
                    }
                }
            }
            sheet1.setColumnWidth(0, 2000);
            sheet1.setColumnWidth(1, 6000);
            sheet1.setColumnWidth(2, 6000);
            sheet1.setColumnWidth(3, 2500);
            sheet1.setColumnWidth(4, 2500);
            sheet1.setColumnWidth(5, 5000);
            sheet1.setColumnWidth(6, 5000);
            HSSFSheet sheet2 = wb.createSheet("任务耗时记总表");
            HSSFRow rowHead2 = sheet2.createRow(1);
            HSSFCell cellHead2 = rowHead2.createCell(0);
            cellHead2.setCellStyle(style);
            HSSFRow row2 = sheet2.createRow(0);
            HSSFCell cell2 = row2.createCell(0);
            cell2.setCellValue("序号");
            cell2.setCellStyle(style);
            cell2 = row2.createCell(1);
            cell2.setCellValue("任务名称");
            cell2.setCellStyle(style);
            cell2 = row2.createCell(2);
            cell2.setCellValue("开始时间");
            cell2.setCellStyle(style);
            cell2 = row2.createCell(3);
            cell2.setCellValue("结束时间");
            cell2.setCellStyle(style);
            cell2 = row2.createCell(4);
            cell2.setCellValue("总作业数");
            cell2.setCellStyle(style);
            cell2 = row2.createCell(5);
            cell2.setCellValue("总用时");
            cell2.setCellStyle(style);
            cell2 = row2.createCell(6);
            cell2.setCellValue("平均耗时");
            cell2.setCellStyle(style);
            if (taskBeans.size() > 0) {
                for (int i = 0; i < taskBeans.size(); i++) {
                    row2 = sheet2.createRow(i + 1);
                    row2.createCell(0).setCellValue(i + 1);
                    row2.createCell(1).setCellValue(taskBeans.get(i).getSub_sys_desc());
                    row2.createCell(2).setCellValue(taskBeans.get(i).getCurr_st_time());
                    row2.createCell(3).setCellValue(taskBeans.get(i).getCurr_end_time());
                    row2.createCell(4).setCellValue(taskBeans.get(i).getJobNum());
                    row2.createCell(5).setCellValue(taskBeans.get(i).getTaskConsumeTime());
                    row2.createCell(6).setCellValue(taskBeans.get(i).getTaskConsumeAveTime());
                    for (int j = 0; j <= cell2.getColumnIndex(); j++) {
                        Cell cell = row2.getCell(j);
                        cell.setCellStyle(style);
                    }
                }
            }
            sheet2.setColumnWidth(0, 2000);
            sheet2.setColumnWidth(1, 6000);
            sheet2.setColumnWidth(2, 6000);
            sheet2.setColumnWidth(3, 6000);
            sheet2.setColumnWidth(4, 2500);
            sheet2.setColumnWidth(5, 5000);
            sheet2.setColumnWidth(6, 5000);
            HSSFSheet sheet3 = wb.createSheet("作业耗时记总表");
            HSSFRow rowHead3 = sheet3.createRow(1);
            HSSFCell cellHead3 = rowHead3.createCell(0);
            cellHead3.setCellStyle(style);
            HSSFRow row3 = sheet3.createRow(0);
            HSSFCell cell3 = row3.createCell(0);
            cell3.setCellValue("序号");
            cell3.setCellStyle(style);
            cell3 = row3.createCell(1);
            cell3.setCellValue("作业名称");
            cell3.setCellStyle(style);
            cell3 = row3.createCell(2);
            cell3.setCellValue("作业描述");
            cell3.setCellStyle(style);
            cell3 = row3.createCell(3);
            cell3.setCellValue("开始时间");
            cell3.setCellStyle(style);
            cell3 = row3.createCell(4);
            cell3.setCellValue("结束时间");
            cell3.setCellStyle(style);
            cell3 = row3.createCell(5);
            cell3.setCellValue("作业用时");
            cell3.setCellStyle(style);
            if (jobBeans.size() > 0) {
                for (int i = 0; i < jobBeans.size(); i++) {
                    row3 = sheet3.createRow(i + 1);
                    row3.createCell(0).setCellValue(i + 1);
                    row3.createCell(1).setCellValue(jobBeans.get(i).getEtl_job());
                    row3.createCell(2).setCellValue(jobBeans.get(i).getEtl_job_desc());
                    row3.createCell(3).setCellValue(jobBeans.get(i).getCurr_st_time());
                    row3.createCell(4).setCellValue(jobBeans.get(i).getCurr_end_time());
                    row3.createCell(5).setCellValue(jobBeans.get(i).getJobTime());
                    for (int j = 0; j <= cell3.getColumnIndex(); j++) {
                        Cell cell = row3.getCell(j);
                        cell.setCellStyle(style);
                    }
                }
            }
            sheet3.setColumnWidth(0, 2000);
            sheet3.setColumnWidth(1, 8000);
            sheet3.setColumnWidth(2, 8000);
            sheet3.setColumnWidth(3, 6000);
            sheet3.setColumnWidth(4, 6000);
            sheet3.setColumnWidth(5, 2500);
            FileOutputStream fout = new FileOutputStream(WebinfoProperties.FileUpload_SavedDirName + File.separator + FILE_NAME);
            log.info("文件生成路径:" + WebinfoProperties.FileUpload_SavedDirName + File.separator + FILE_NAME);
            wb.write(fout);
            fout.close();
        } catch (FileNotFoundException e) {
            log.info("文件异常!");
        } catch (IOException e) {
            log.info("流转化异常!");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "curr_bath_date", desc = "", range = "")
    @Param(name = "etl_job", desc = "", range = "", nullable = true)
    @Param(name = "etl_job_desc", desc = "", range = "", nullable = true)
    @Param(name = "currJobPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageJobSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getJobConsumeTimeSum(Long etl_sys_id, String curr_bath_date, String etl_job, String etl_job_desc, int currJobPage, int pageJobSize) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select etl_job_id,etl_sys_id,etl_job,etl_job_desc,curr_st_time,curr_end_time from " + EtlJobCur.TableName + " where etl_sys_id = ? and curr_bath_date = ?");
        asmSql.addParam(etl_sys_id);
        asmSql.addParam(curr_bath_date);
        if (!StringUtil.isEmpty(etl_job)) {
            asmSql.addSql(" and etl_job like ?").addParam("%" + etl_job.toLowerCase() + "%");
        }
        if (!StringUtil.isEmpty(etl_job_desc)) {
            asmSql.addSql(" and etl_job_desc like ?").addParam("%" + etl_job_desc.toLowerCase() + "%");
        }
        Page page = new DefaultPageImpl(currJobPage, pageJobSize);
        Map<String, Object> jobMap = new HashMap<>();
        List<Map<String, Object>> mapList = Dbo.queryPagedList(page, asmSql.sql(), asmSql.params());
        if (!mapList.isEmpty()) {
            for (Map<String, Object> map : mapList) {
                if (!Objects.isNull(map.get("curr_end_time"))) {
                    long jobTime = getTime(map.get("curr_end_time").toString(), map.get("curr_st_time").toString());
                    map.put("jobTime", jobTime);
                }
            }
        }
        jobMap.put("data", mapList);
        jobMap.put("totalSize", page.getTotalSize());
        return jobMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "curr_bath_date", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getProjectConsumeTimeSum(Long etl_sys_id, String curr_bath_date) {
        Result result = Dbo.queryResult("SELECT list.sub_sys_id,MAX( his.curr_end_time ) AS curr_end_time," + " MIN( his.curr_st_time ) AS curr_st_time FROM " + EtlSubSysList.TableName + " list JOIN " + EtlJobDispHis.TableName + " his ON list.sub_sys_id = his.sub_sys_id " + " WHERE his.curr_bath_date = ? AND list.etl_sys_id = ? GROUP BY list.sub_sys_id", curr_bath_date, etl_sys_id);
        if (!result.isEmpty()) {
            long taskNum = result.getRowCount();
            long jobNum = Dbo.queryNumber("select count(*) from etl_job_cur WHERE etl_sys_id = ? and curr_bath_date = ?", etl_sys_id, curr_bath_date).orElse(0);
            result.setObject(0, "taskNum", taskNum);
            result.setObject(0, "jobNum", jobNum);
            long time = getTime(result.getString(0, "curr_end_time"), result.getString(0, "curr_st_time"));
            long aveTime;
            if (jobNum == 0) {
                aveTime = time;
            } else {
                aveTime = time / jobNum;
            }
            result.setObject(0, "projectConsumeTime", time);
            result.setObject(0, "projectConsumeAveTime", aveTime);
        }
        return result;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileName", desc = "", range = "")
    public void downloadFile(String fileName) {
        FileDownloadUtil.downloadFile(fileName);
        try {
            FileUtil.forceDelete(new File(WebinfoProperties.FileUpload_SavedDirName + File.separator + fileName));
        } catch (IOException e) {
            log.error(e.getMessage());
            throw new BusinessException("删除文件失败！" + e.getMessage());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "curr_bath_date", desc = "", range = "")
    @Param(name = "sub_sys_cd_or_name", desc = "", range = "", nullable = true)
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getTaskConsumeTimeSum(Long etl_sys_id, String curr_bath_date, String sub_sys_cd_or_name, int currPage, int pageSize) {
        Map<String, Object> taskMap = new HashMap<>();
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT list.sub_sys_cd,list.sub_sys_id, list.sub_sys_desc, MAX( his.curr_end_time ) AS curr_end_time, " + " MIN( his.curr_st_time ) AS curr_st_time " + " FROM " + EtlJobDispHis.TableName + " his JOIN " + EtlSubSysList.TableName + " list ON his.sub_sys_id = list.sub_sys_id " + " WHERE list.etl_sys_id = ? and curr_bath_date = ? ");
        asmSql.addParam(etl_sys_id);
        asmSql.addParam(curr_bath_date);
        if (!StringUtil.isEmpty(sub_sys_cd_or_name)) {
            asmSql.addSql(" and ( list.sub_sys_cd like ? or list.sub_sys_desc like ? )").addParam("%" + sub_sys_cd_or_name.toLowerCase() + "%").addParam("%" + sub_sys_cd_or_name.toLowerCase() + "%");
        }
        asmSql.addSql("GROUP BY list.etl_sys_id, list.sub_sys_id");
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> mapList = Dbo.queryPagedList(page, asmSql.sql(), asmSql.params());
        if (!mapList.isEmpty()) {
            for (Map<String, Object> map : mapList) {
                try {
                    if (!Objects.isNull(map.get("sub_sys_id"))) {
                        long sub_sys_id = Long.parseLong(map.get("sub_sys_id").toString());
                        long jobNum = Dbo.queryNumber("select count(1) from " + EtlJobCur.TableName + " where etl_sys_id = ? and sub_sys_id = ? " + "and curr_bath_date = ?", etl_sys_id, sub_sys_id, curr_bath_date).orElse(0);
                        long time = getTime(map.get("curr_end_time").toString(), map.get("curr_st_time").toString());
                        long aveTime;
                        if (jobNum == 0) {
                            aveTime = time;
                        } else {
                            aveTime = time / jobNum;
                        }
                        map.put("taskConsumeTime", time);
                        map.put("taskConsumeAveTime", aveTime);
                        map.put("jobNum", jobNum);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BusinessException(" map format failed. ");
                }
            }
        }
        taskMap.put("data", mapList);
        taskMap.put("totalSize", page.getTotalSize());
        return taskMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "sub_sys_id", desc = "", range = "")
    @Param(name = "curr_bath_date", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> searchMonitorJobStateBySubCd(Long etl_sys_id, Long sub_sys_id, String curr_bath_date) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, UserUtil.getUserId(), Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        if (!EtlJobUtil.isEtlSubSysExist(etl_sys_id, sub_sys_id, Dbo.db())) {
            throw new BusinessException("当前工程对应任务已不存在！");
        }
        return Dbo.queryList("select etl_job_id,etl_job,curr_st_time,curr_st_time,curr_end_time,job_disp_status," + "sub_sys_id,curr_bath_date,etl_sys_id FROM " + EtlJobCur.TableName + " WHERE sub_sys_id=? AND etl_sys_id=? AND REPLACE(curr_bath_date,'-','')=? " + " ORDER BY curr_bath_date", sub_sys_id, etl_sys_id, curr_bath_date);
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result monitorAllProjectChartsData(Long userId) {
        return Dbo.queryResult("SELECT " + " SUM(case when job_disp_status = ? then 1 else 0 end ) Pending," + " SUM(case when job_disp_status = ? then 1 else 0 end ) Waiting," + " SUM(case when job_disp_status = ? then 1 else 0 end ) Runing," + " SUM(case when job_disp_status = ? then 1 else 0 end ) Done," + " SUM(case when job_disp_status = ? then 1 else 0 end ) Suspension," + " SUM(case when job_disp_status = ? then 1 else 0 end ) Error," + " A.curr_bath_date AS bathDate,A.etl_sys_id,B.etl_sys_name," + " CONCAT(B.etl_sys_cd,'(',B.etl_sys_name,')') sys_name FROM " + EtlJobCur.TableName + " A," + EtlSys.TableName + " B where A.etl_sys_id=B.etl_sys_id AND B.user_id=? " + " and A.curr_bath_date=B.curr_bath_date group by bathDate,A.etl_sys_id,B.etl_sys_name,B.etl_sys_cd" + " ORDER BY A.etl_sys_id", Job_Status.PENDING.getCode(), Job_Status.WAITING.getCode(), Job_Status.RUNNING.getCode(), Job_Status.DONE.getCode(), Job_Status.STOP.getCode(), Job_Status.ERROR.getCode(), userId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public String monitorBatchEtlJobDependencyInfo(Long etl_sys_id, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        try {
            String head_1 = "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\"";
            String head = "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\" " + "xmlns:viz=\"http://www.gexf.net/1.2draft/viz\" " + "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" " + "xsi:schemaLocation=\"http://www.gexf.net/1.2draft http://www.gexf.net/1.2draft/gexf.xsd\"";
            Gexf gexf = new GexfImpl();
            Calendar date = Calendar.getInstance();
            date.set(date.get(Calendar.YEAR), date.get(Calendar.MONTH) + 1, date.get(Calendar.DATE));
            gexf.getMetadata().setLastModified(date.getTime()).setCreator("Gephi.org").setDescription("A Web network");
            Graph graph = gexf.getGraph();
            graph.setDefaultEdgeType(EdgeType.UNDIRECTED).setMode(Mode.STATIC);
            AttributeList attrList = new AttributeListImpl(AttributeClass.NODE);
            graph.getAttributeLists().add(attrList);
            Attribute attUrl = attrList.createAttribute("modularity_class", AttributeType.INTEGER, "Modularity Class");
            Map<Long, Node> nodeMap = new HashMap<>();
            List<String> list_Edge = new ArrayList<>();
            Result scheduledOrFrequency = Dbo.queryResult("select sys.sub_sys_cd,job.* from " + EtlJobDef.TableName + " job" + " LEFT JOIN " + EtlSubSysList.TableName + " sys ON job.sub_sys_id = sys.sub_sys_id" + " WHERE job.etl_sys_id=? AND job.disp_type!=?", etl_sys_id, Dispatch_Type.DEPENDENCE.getCode());
            Random random = new Random();
            int number_key = 0;
            for (int i = 0; i < scheduledOrFrequency.getRowCount(); i++) {
                Long etl_job_id = scheduledOrFrequency.getLong(i, "etl_job_id");
                String etl_job = scheduledOrFrequency.getString(i, "etl_job");
                Node gephi = graph.createNode(number_key + "");
                String dispFreq = scheduledOrFrequency.getString(i, "disp_freq");
                if (Dispatch_Frequency.PinLv == (Dispatch_Frequency.ofEnumByCode(dispFreq))) {
                    number_key = number_key + 1;
                    String label = "作业：" + etl_job_id + "\n\r调度频率：频率\n\r" + "隔多长时间：" + scheduledOrFrequency.getString(i, "exe_frequency") + "\n\r执行次数：" + scheduledOrFrequency.getString(i, "exe_num") + "\n\r开始执行时间：" + scheduledOrFrequency.getString(i, "star_time") + "\n\r结束执行时间：" + scheduledOrFrequency.getString(i, "end_time");
                    gephi.setLabel(label).getAttributeValues().addValue(attUrl, "0");
                    gephi.setSize(10);
                    gephi.setPosition(new PositionImpl(random.nextFloat(), random.nextFloat(), 0.0f));
                    nodeMap.put(etl_job_id, gephi);
                } else {
                    number_key = number_key + 1;
                    String label = "作业：" + etl_job + "\n\r触发方式：定时触发\n\r" + "触发时间：" + scheduledOrFrequency.getString(i, "disp_time") + "\n\r任务：" + scheduledOrFrequency.getString(i, "sub_sys_cd");
                    gephi.setLabel(label).getAttributeValues().addValue(attUrl, "1");
                    gephi.setSize(10);
                    gephi.setPosition(new PositionImpl(random.nextFloat(), random.nextFloat(), 0.0f));
                    nodeMap.put(etl_job_id, gephi);
                }
            }
            Result dependencyJobResult = Dbo.queryResult("select sys.sub_sys_cd,job.* from " + EtlJobDef.TableName + " job" + " LEFT JOIN " + EtlSubSysList.TableName + " sys ON job.sub_sys_id = sys.sub_sys_id" + " WHERE job.disp_type=? AND job.etl_sys_id=?", Dispatch_Type.DEPENDENCE.getCode(), etl_sys_id);
            for (int i = 0; i < dependencyJobResult.getRowCount(); i++) {
                Long etl_job_id = dependencyJobResult.getLong(i, "etl_job_id");
                String etl_job = dependencyJobResult.getString(i, "etl_job");
                Result topResult = EtlJobUtil.topEtlJobDependencyInfo(etl_job_id, etl_sys_id, Dbo.db());
                Result downResult = EtlJobUtil.downEtlJobDependencyInfo(etl_sys_id, etl_job_id, Dbo.db());
                Node gephi = graph.createNode(number_key + "");
                if (topResult.isEmpty() && downResult.isEmpty()) {
                    String label = "作业：" + etl_job + "\n\r触发方式：依赖触发，无依赖关系作业\n\r" + "任务：" + dependencyJobResult.getString(i, "sub_sys_cd");
                    gephi.setLabel(label).getAttributeValues().addValue(attUrl, "1");
                } else if (topResult.isEmpty() && !downResult.isEmpty()) {
                    String label = "作业：" + etl_job + "\n\r触发方式：依赖触发，无上游作业\n\r" + "任务：" + dependencyJobResult.getString(i, "sub_sys_cd");
                    gephi.setLabel(label).getAttributeValues().addValue(attUrl, "2");
                } else if (!topResult.isEmpty() && downResult.isEmpty()) {
                    String label = "作业：" + etl_job + "\n\r触发方式：依赖触发,无下游作业\n\r" + "任务：" + dependencyJobResult.getString(i, "sub_sys_cd");
                    gephi.setLabel(label).getAttributeValues().addValue(attUrl, "3");
                } else {
                    String label = "作业：" + etl_job + "\n\r触发方式：依赖触发，上下游作业都有\n\r" + "任务：" + dependencyJobResult.getString(i, "sub_sys_cd");
                    gephi.setLabel(label).getAttributeValues().addValue(attUrl, "4");
                }
                number_key = number_key + 1;
                gephi.setSize(10);
                gephi.setPosition(new PositionImpl(random.nextFloat(), random.nextFloat(), 0.0f));
                if (!nodeMap.containsKey(etl_job_id)) {
                    nodeMap.put(etl_job_id, gephi);
                }
            }
            Result dispatchFreqResult = Dbo.queryResult("select sys.sub_sys_cd,job.* from " + EtlJobDef.TableName + " job" + " LEFT JOIN " + EtlSubSysList.TableName + " sys ON job.sub_sys_id = sys.sub_sys_id" + " WHERE job.disp_freq != ? AND job.etl_sys_id=?", Dispatch_Frequency.PinLv.getCode(), etl_sys_id);
            for (int i = 0; i < dispatchFreqResult.getRowCount(); i++) {
                Long etl_job_id = dispatchFreqResult.getLong(i, "etl_job_id");
                Result topResult = EtlJobUtil.topEtlJobDependencyInfo(etl_job_id, etl_sys_id, Dbo.db());
                Result downResult = EtlJobUtil.downEtlJobDependencyInfo(etl_sys_id, etl_job_id, Dbo.db());
                buildDependencies(nodeMap, list_Edge, etl_job_id, topResult, downResult);
            }
            StaxGraphWriter graphWriter = new StaxGraphWriter();
            StringWriter stringWriter = new StringWriter();
            graphWriter.writeToStream(gexf, stringWriter, CodecUtil.UTF8_STRING);
            String outputXml = stringWriter.toString();
            outputXml = outputXml.replaceAll(head_1, head).replace("\n", "");
            return outputXml;
        } catch (IOException e) {
            throw new BusinessException("查看全作业依赖时写gexf格式的文件失败！");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> monitorCurrentBatchInfo(Long etl_sys_id, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.clean();
        asmSql.addSql("SELECT SUM(case when job_disp_status = ? then 1 else 0 end ) Pending," + " SUM(case when job_disp_status = ? then 1 else 0 end ) Waiting," + " SUM(case when job_disp_status = ? then 1 else 0 end ) Runing," + " SUM(case when job_disp_status = ? then 1 else 0 end ) Done," + " SUM(case when job_disp_status = ? then 1 else 0 end ) Suspension," + " SUM(case when job_disp_status = ? then 1 else 0 end ) Error," + " A.curr_bath_date,A.etl_sys_id,CONCAT(A.etl_sys_id,'(',B.etl_sys_name,')') sys_name" + " FROM " + EtlJobCur.TableName + " A," + EtlSys.TableName + " B ");
        asmSql.addSql(" where A.etl_sys_id = B.etl_sys_id ");
        asmSql.addParam(Job_Status.PENDING.getCode());
        asmSql.addParam(Job_Status.WAITING.getCode());
        asmSql.addParam(Job_Status.RUNNING.getCode());
        asmSql.addParam(Job_Status.DONE.getCode());
        asmSql.addParam(Job_Status.STOP.getCode());
        asmSql.addParam(Job_Status.ERROR.getCode());
        if (etl_sys_id != null) {
            asmSql.addSql(" and  A.etl_sys_id = ? ");
            asmSql.addParam(etl_sys_id);
        }
        asmSql.addSql(" and A.curr_bath_date=B.curr_bath_date group by A.curr_bath_date,A.etl_sys_id,B.etl_sys_name ORDER BY A.etl_sys_id ");
        return Dbo.queryOneObject(asmSql.sql(), asmSql.params());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> monitorCurrentBatchInfoByTask(Long etl_sys_id, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT T1.sub_sys_id, T1.etl_sys_id, MAX(T2.sub_sys_cd), MAX(T3.etl_sys_cd)," + "(case when T1.sub_sys_id = MAX(T2.sub_sys_id)" + " then CONCAT(T2.sub_sys_desc, '(', MAX(T2.sub_sys_cd), ')')" + "else MAX(T2.sub_sys_cd) end) as sub_sys_desc," + " SUM(CASE WHEN job_disp_status = ? THEN 1 ELSE 0 END ) Pending," + " SUM(CASE WHEN job_disp_status = ? THEN 1 ELSE 0 END ) Waiting," + " SUM(CASE WHEN job_disp_status = ? THEN 1 ELSE 0 END ) Runing," + " SUM(CASE WHEN job_disp_status = ? THEN 1 ELSE 0 END ) Done," + " SUM(CASE WHEN job_disp_status = ? THEN 1 ELSE 0 END ) Suspension," + " SUM(CASE WHEN job_disp_status = ? THEN 1 ELSE 0 END ) Error" + " FROM " + EtlJobCur.TableName + " T1 INNER JOIN " + EtlSys.TableName + " T3 ON T1.etl_sys_id = T3.etl_sys_id AND T1.curr_bath_date = T3.curr_bath_date" + " LEFT JOIN " + EtlSubSysList.TableName + " T2 ON T1.sub_sys_id = T2.sub_sys_id " + " WHERE T1.etl_sys_id = ?");
        asmSql.addSql(" group by T1.sub_sys_id, T1.etl_sys_id, sub_sys_desc");
        asmSql.addParam(Job_Status.PENDING.getCode());
        asmSql.addParam(Job_Status.WAITING.getCode());
        asmSql.addParam(Job_Status.RUNNING.getCode());
        asmSql.addParam(Job_Status.DONE.getCode());
        asmSql.addParam(Job_Status.STOP.getCode());
        asmSql.addParam(Job_Status.ERROR.getCode());
        asmSql.addParam(etl_sys_id);
        return Dbo.queryList(asmSql.sql(), asmSql.params());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public Map<String, Object> monitorCurrJobInfo(Long etl_sys_id, Long etl_job_id, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        if (etl_job_id != null) {
            if (!EtlJobUtil.isEtlJobDefExist(etl_sys_id, etl_job_id, Dbo.db())) {
                throw new BusinessException("当前工程对应作业已不存在！");
            }
        }
        if (etl_job_id != null) {
            Map<String, Object> etlJobCur = Dbo.queryOneObject("select * from " + EtlJobCur.TableName + " where etl_sys_id=? and etl_job_id=? order by curr_bath_date desc", etl_sys_id, etl_job_id);
            Map<String, Object> resourceRelation = Dbo.queryOneObject("select resource_type,resource_req from " + EtlJobResourceRela.TableName + " where etl_sys_id=? and etl_job_id=?", etl_sys_id, etl_job_id);
            etlJobCur.put("resourceRelation", resourceRelation);
            return etlJobCur;
        }
        return null;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "curr_bath_date", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public List<Map<String, Object>> monitorHistoryBatchInfo(Long etl_sys_id, String curr_bath_date, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        if (StringUtil.isBlank(curr_bath_date)) {
            curr_bath_date = DateUtil.getSysDate();
        }
        return Dbo.queryList("SELECT MAX(his.curr_end_time) as curr_end_time," + " MIN(his.curr_st_time) as curr_st_time," + " his.curr_bath_date," + " his.sub_sys_id ," + " list.sub_sys_cd," + " CONCAT(list.sub_sys_cd, '(', list.sub_sys_desc, ')') as desc_sys" + " FROM " + EtlJobDispHis.TableName + " his " + " left join " + EtlSubSysList.TableName + " list on his.sub_sys_id  = list.sub_sys_id and his.etl_sys_id = list.etl_sys_id " + " WHERE his.etl_sys_id = ? AND his.curr_bath_date=? " + " group by his.sub_sys_id, list.sub_sys_desc, his.curr_bath_date, list.sub_sys_cd" + " order by curr_st_time", etl_sys_id, curr_bath_date);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Param(name = "start_date", desc = "", range = "", nullable = true)
    @Param(name = "end_date", desc = "", range = "", nullable = true)
    @Param(name = "isHistoryBatch", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public List<Map<String, Object>> monitorHistoryJobInfo(Long etl_sys_id, String etl_job, String start_date, String end_date, String isHistoryBatch, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        if (!EtlJobUtil.isEtlJobDefExist(etl_sys_id, etl_job, Dbo.db())) {
            throw new BusinessException("当前工程对应作业名称已不存在！");
        }
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT etl_job,curr_bath_date,curr_st_time,curr_end_time,(CASE WHEN job_disp_status=?" + " THEN  '" + Job_Status.DONE.getValue() + "(" + Job_Status.DONE.getCode() + ")' else '' END)" + " AS job_disp_status, (CASE WHEN t1.sub_sys_id=t2.sub_sys_id then " + " CONCAT(t2.sub_sys_desc,'(',t1.sub_sys_id,')') ELSE CONCAT(t1.sub_sys_id) END)" + " AS sub_sys_id FROM(SELECT A.* FROM (SELECT * FROM " + EtlJobDispHis.TableName + " WHERE etl_sys_id=? and etl_job=?");
        asmSql.addParam(Job_Status.DONE.getCode());
        asmSql.addParam(etl_sys_id);
        asmSql.addParam(etl_job);
        isHistoryBatch(start_date, end_date, isHistoryBatch);
        asmSql.addSql(") A INNER JOIN (SELECT curr_bath_date,etl_sys_id,etl_job,sub_sys_id," + " MAX(curr_st_time) AS curr_st_time FROM " + EtlJobDispHis.TableName + " WHERE etl_sys_id=? and etl_job=?");
        asmSql.addParam(etl_sys_id);
        asmSql.addParam(etl_job);
        isHistoryBatch(start_date, end_date, isHistoryBatch);
        asmSql.addSql(" GROUP BY curr_bath_date,etl_sys_id,etl_job,sub_sys_id) B " + " ON A.curr_bath_date=B.curr_bath_date AND A.etl_sys_id=B.etl_sys_id " + " AND A.etl_job=B.etl_job AND A.sub_sys_id=B.sub_sys_id " + " AND A.curr_st_time=B.curr_st_time" + ") t1 LEFT JOIN " + EtlSubSysList.TableName + " t2 ON t1.sub_sys_id=t2.sub_sys_id  and t1.etl_sys_id=t2.etl_sys_id ORDER BY" + " curr_bath_date,curr_st_time,curr_end_time");
        return Dbo.queryList(asmSql.sql(), asmSql.params());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> monitorJobDependencyInfo(Long etl_sys_id, Long etl_job_id, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在:" + etl_sys_id);
        }
        EtlJobDef etlJob = SqlOperator.queryOneObject(Dbo.db(), EtlJobDef.class, "SELECT * FROM " + EtlJobDef.TableName + " WHERE etl_job_id=? AND etl_sys_id=?", etl_job_id, etl_sys_id).orElseThrow(() -> new BusinessException("当前工程下的作业已不存在:" + etl_job_id));
        EtlDependency etl_dependency = new EtlDependency();
        etl_dependency.setEtl_sys_id(etl_sys_id);
        etl_dependency.setEtl_job_id(etl_job_id);
        etl_dependency.setPre_etl_sys_id(etl_sys_id);
        etl_dependency.setPre_etl_job_id(etl_job_id);
        List<Map<String, Object>> topJobInfoList = EtlJobUtil.topEtlJobDependencyInfo(etl_dependency.getEtl_job_id(), etl_dependency.getEtl_sys_id(), Dbo.db()).toList();
        List<Map<String, Object>> downJobInfoList = EtlJobUtil.downEtlJobDependencyInfo(etl_dependency.getPre_etl_sys_id(), etl_dependency.getEtl_job_id(), Dbo.db()).toList();
        if (!topJobInfoList.isEmpty()) {
            downJobInfoList.addAll(topJobInfoList);
        }
        Map<String, Object> dataInfo = new HashMap<>();
        dataInfo.put("id", "0");
        dataInfo.put("name", etlJob.getEtl_job());
        dataInfo.put("aid", "999");
        dataInfo.put("children", downJobInfoList);
        return dataInfo;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> monitorSystemResourceInfo(Long etl_sys_id, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        List<Map<String, Object>> etlResourceList = Dbo.queryList("SELECT resource_type,resource_max," + "resource_used,etl_sys_id,(resource_max-resource_used) as free FROM " + EtlResource.TableName + " where etl_sys_id=?", etl_sys_id);
        List<Map<String, Object>> jobRunList = Dbo.queryList("SELECT T1.resource_type," + " concat(T3.sub_sys_cd,'(',T3.sub_sys_desc,')') sub_sys_cd," + " concat(T2.etl_job,'(',T2.etl_job_desc,')') etl_job,T2.job_disp_status," + " T2.curr_st_time AS curr_st_time,T1.resource_req,T1.etl_sys_id FROM " + EtlJobCur.TableName + " T2 INNER JOIN " + EtlJobResourceRela.TableName + " T1 ON T1.etl_sys_id=T2.etl_sys_id AND T2.etl_job_id=T1.etl_job_id INNER JOIN " + EtlSubSysList.TableName + " T3 ON T2.sub_sys_id=T3.sub_sys_id " + " WHERE T2.job_disp_status=? AND T1.etl_sys_id=?", Job_Status.RUNNING.getCode(), etl_sys_id);
        Map<String, Object> monitorSysResource = new HashMap<>();
        monitorSysResource.put("etlResourceList", etlResourceList);
        monitorSysResource.put("jobRunList", jobRunList);
        return monitorSysResource;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Param(name = "readNum", desc = "", range = "", valueIfNull = "100")
    @Return(desc = "", range = "")
    public String readHistoryJobLogInfo(Long etl_sys_id, String etl_job, Integer readNum, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        if (!EtlJobUtil.isEtlJobDefExist(etl_sys_id, etl_job, Dbo.db())) {
            throw new BusinessException("当前工程对应作业已不存在！");
        }
        EtlSys etl_sys = EtlJobUtil.getEtlSysById(etl_sys_id, userId, Dbo.db());
        String curr_bath_date = etl_sys.getCurr_bath_date();
        if (curr_bath_date.contains("-") && curr_bath_date.length() == 10) {
            curr_bath_date = StringUtil.replace(curr_bath_date, "-", "");
        }
        List<Object> logDicList = Dbo.queryOneColumnList("select log_dic from " + EtlJobDispHis.TableName + " where etl_sys_id=? AND etl_job=? and curr_bath_date=?" + " order by curr_end_time desc", etl_sys_id, etl_job, curr_bath_date);
        EtlJobCur etlJob = SqlOperator.queryOneObject(Dbo.db(), EtlJobCur.class, "select * from " + EtlJobCur.TableName + " where etl_job = ?", etl_job).orElseThrow(() -> (new BusinessException("未找到作业")));
        if (logDicList.isEmpty()) {
            return null;
        }
        String log_dic = logDicList.get(0).toString();
        if (!log_dic.endsWith("/")) {
            log_dic = log_dic + File.separator;
        }
        String logDir = log_dic + etlJob.getEtl_job() + "_" + curr_bath_date + ".log";
        if (readNum > 1000) {
            readNum = 1000;
        }
        SSHDetails sshDetails = new SSHDetails();
        sshDetails.setHost(etl_sys.getEtl_serv_ip());
        sshDetails.setPort(Integer.parseInt(etl_sys.getEtl_serv_port()));
        sshDetails.setUser_name(etl_sys.getUser_name());
        sshDetails.setPwd(etl_sys.getUser_pwd());
        return ReadLog.readAgentLog(logDir, sshDetails, readNum);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "curr_end_time", desc = "", range = "")
    @Param(name = "curr_st_time", desc = "", range = "")
    @Return(desc = "", range = "")
    public long getTime(String curr_end_time, String curr_st_time) {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        long bet_time = 0;
        try {
            Date parse = format.parse(curr_st_time.replace(" ", ""));
            Date date = format.parse(curr_end_time.replace(" ", ""));
            bet_time = (date.getTime() - parse.getTime()) / 1000;
            return bet_time;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return bet_time;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "nodeMap", desc = "", range = "")
    @Param(name = "list_Edge", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Param(name = "topResult", desc = "", range = "")
    @Param(name = "downResult", desc = "", range = "")
    private void buildDependencies(Map<Long, Node> nodeMap, List<String> list_Edge, Long etl_job_id, Result topResult, Result downResult) {
        if (!topResult.isEmpty()) {
            for (int i = 0; i < topResult.getRowCount(); i++) {
                Long pre_etl_job_id = topResult.getLong(i, "pre_etl_job_id");
                topNodeConnectToNode(nodeMap, list_Edge, etl_job_id, pre_etl_job_id);
            }
        }
        if (!downResult.isEmpty()) {
            for (int i = 0; i < downResult.getRowCount(); i++) {
                Long down_etl_job_id = downResult.getLong(i, "etl_job_id");
                topNodeConnectToNode(nodeMap, list_Edge, down_etl_job_id, etl_job_id);
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "nodeMap", desc = "", range = "")
    @Param(name = "list_Edge", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Param(name = "pre_etl_job_id", desc = "", range = "")
    private void topNodeConnectToNode(Map<Long, Node> nodeMap, List<String> list_Edge, Long etl_job_id, Long pre_etl_job_id) {
        if (!etl_job_id.equals(pre_etl_job_id)) {
            Node node = nodeMap.get(etl_job_id);
            Node topNode = nodeMap.get(pre_etl_job_id);
            if (!list_Edge.contains(etl_job_id + "-" + pre_etl_job_id)) {
                topNode.connectTo(node);
                list_Edge.add(etl_job_id + "-" + pre_etl_job_id);
            }
        }
    }

    private void addParamsToSql(Long etl_sys_id, SqlOperator.Assembler asmSql) {
        asmSql.addParam(Job_Status.PENDING.getCode());
        asmSql.addParam(Job_Status.WAITING.getCode());
        asmSql.addParam(Job_Status.RUNNING.getCode());
        asmSql.addParam(Job_Status.DONE.getCode());
        asmSql.addParam(Job_Status.STOP.getCode());
        asmSql.addParam(Job_Status.ERROR.getCode());
        asmSql.addParam(etl_sys_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "start_date", desc = "", range = "", nullable = true)
    @Param(name = "end_date", desc = "", range = "", nullable = true)
    @Param(name = "isHistoryBatch", desc = "", range = "", nullable = true)
    private void isHistoryBatch(String start_date, String end_date, String isHistoryBatch) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        if (StringUtil.isNotBlank(isHistoryBatch)) {
            asmSql.addSql(" AND REPLACE(curr_bath_date,'-','') =?");
            asmSql.addParam(start_date);
        } else {
            if (StringUtil.isNotBlank(start_date)) {
                asmSql.addSql(" and REPLACE(curr_bath_date,'-','') >= ?");
                asmSql.addParam(start_date.replace("-", ""));
            }
            if (StringUtil.isNotBlank(end_date)) {
                asmSql.addSql(" and REPLACE(curr_bath_date,'-','') <= ?");
                asmSql.addParam(end_date.replace("-", ""));
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<EtlJobDef> findJobByEtlSysId(Long etl_sys_id, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        List<EtlJobDef> list = Dbo.queryList(EtlJobDef.class, "select * from " + EtlJobDef.TableName + " where etl_sys_id=?", etl_sys_id);
        return list;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "sub_sys_cd", desc = "", range = "")
    @Param(name = "curr_bath_date", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> searchMonitorHisBatchJobBySubCd(Long etl_sys_id, Long sub_sys_id, String sub_sys_cd, String curr_bath_date, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        if (!EtlJobUtil.isEtlSubSysExist(etl_sys_id, sub_sys_id, Dbo.db())) {
            throw new BusinessException("当前工程对应的任务已不存在！");
        }
        if (curr_bath_date.length() == 10 && curr_bath_date.contains("-")) {
            curr_bath_date = StringUtil.replace(curr_bath_date, "-", "");
        }
        return Dbo.queryList("SELECT t1.etl_sys_id,t1.sub_sys_id,t1.etl_job,t1.etl_job_id," + " MIN(curr_st_time) as curr_st_time,MAX(curr_end_time) as curr_end_time,curr_bath_date," + " job_disp_status,t1.etl_job_desc FROM ( SELECT A.*,ejd.etl_job_id FROM (SELECT * FROM " + EtlJobDispHis.TableName + " WHERE curr_bath_date=?) A INNER JOIN " + "(SELECT curr_bath_date,etl_sys_id,etl_job,sub_sys_id,MAX(curr_st_time) AS curr_st_time" + " FROM " + EtlJobDispHis.TableName + " WHERE curr_bath_date=? GROUP BY curr_bath_date," + " etl_sys_id,etl_job,sub_sys_id) B ON A.curr_bath_date=B.curr_bath_date" + " AND A.etl_sys_id=B.etl_sys_id AND A.etl_job=B.etl_job AND A.sub_sys_id=B.sub_sys_id " + " AND A.curr_st_time=B.curr_st_time join etl_job_def ejd on ejd.etl_job = A.etl_job) t1 WHERE t1.etl_sys_id=? and curr_bath_date=? " + " AND t1.sub_sys_id=? GROUP BY t1.etl_sys_id,t1.sub_sys_id,t1.etl_job,t1.etl_job_id,curr_bath_date," + " job_disp_status,t1.etl_job_desc order by curr_st_time asc", curr_bath_date, curr_bath_date, etl_sys_id, curr_bath_date, sub_sys_id);
    }
}
