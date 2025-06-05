package hyren.serv6.c.jobconfig;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.*;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.meta.MetaOperator;
import fd.ng.db.meta.TableMeta;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.c.jobconfig.dto.BatchDeleteEtlParaDTO;
import hyren.serv6.c.util.ConvertColumnNameToChinese;
import hyren.serv6.base.codes.*;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.entity.fdentity.ProEntity;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.jobUtil.EtlJobUtil;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.base.utils.fileutil.FileDownloadUtil;
import hyren.serv6.base.utils.regular.RegexConstant;
import hyren.serv6.commons.utils.xlstoxml.util.ExcelUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.*;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

@Service
@Slf4j
public class JobConfigService {

    private static final Logger logger = LogManager.getLogger();

    private static final String PREFIX = "!";

    private static final Long DefaultEtlSysId = 1000000000000000000L;

    private static final String xlsxSuffix = ".xlsx";

    private static final String pro_type = "pro_type";

    private static final String disp_freq = "disp_freq";

    private static final String disp_type = "disp_type";

    private static final String job_eff_flag = "job_eff_flag";

    private static final String job_disp_status = "job_disp_status";

    private static final String today_disp = "today_disp";

    private static final String main_serv_sync = "main_serv_sync";

    private static final String status = "status";

    private static final String para_type = "para_type";

    private static final String job_datasource = "job_datasource";

    @Method(desc = "", logicStep = "")
    @Param(name = "etlDependencies", desc = "", range = "")
    public void batchDeleteEtlDependency(String etlDependencies, Long userId) {
        Validator.notBlank(etlDependencies);
        List<EtlDependency> etlDependencyList = fd.ng.core.utils.JsonUtil.toObject(etlDependencies, new TypeReference<List<EtlDependency>>() {
        });
        for (EtlDependency etlDependency : etlDependencyList) {
            if (EtlJobUtil.isEtlSysExistById(etlDependency.getEtl_sys_id(), userId, Dbo.db())) {
                throw new BusinessException("当前工程已不存在:" + etlDependency.getEtl_sys_id());
            }
            etlDependency.delete(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "pre_etl_sys_id", desc = "", range = "")
    @Param(name = "status", desc = "", range = "")
    @Param(name = "sub_sys_cd", desc = "", range = "")
    @Param(name = "pre_sub_sys_cd", desc = "", range = "")
    public void batchSaveEtlDependency(Long etl_sys_id, Long pre_etl_sys_id, Long sub_sys_id, Long pre_sub_sys_id, String status, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        if (EtlJobUtil.isEtlSysExistById(pre_etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        Status.ofEnumByCode(status);
        if (!EtlJobUtil.isEtlSubSysExist(etl_sys_id, sub_sys_id, Dbo.db())) {
            throw new BusinessException("当前工程对应任务已不存在！");
        }
        if (!EtlJobUtil.isEtlSubSysExist(etl_sys_id, pre_sub_sys_id, Dbo.db())) {
            throw new BusinessException("当前工程对应上游任务已不存在！");
        }
        List<EtlJobDef> etlJobList = Dbo.queryList(EtlJobDef.class, "select DISTINCT etl_job_id,etl_job,disp_type,disp_freq from " + EtlJobDef.TableName + " where sub_sys_id=? and etl_sys_id=?", sub_sys_id, etl_sys_id);
        List<EtlJobDef> preEtlJobList = Dbo.queryList(EtlJobDef.class, "select DISTINCT etl_job_id,etl_job,disp_type,disp_freq from " + EtlJobDef.TableName + " where sub_sys_id=? and etl_sys_id=?", pre_sub_sys_id, etl_sys_id);
        EtlDependency etlDependency = new EtlDependency();
        for (EtlJobDef etl_job_def : etlJobList) {
            for (EtlJobDef etlJobDef : preEtlJobList) {
                if (etl_job_def.getEtl_job().equals(etlJobDef.getEtl_job())) {
                    continue;
                }
                if (EtlJobUtil.isEtlDependencyExist(etl_sys_id, pre_etl_sys_id, etl_job_def.getEtl_job_id(), etlJobDef.getEtl_job_id(), Dbo.db())) {
                    continue;
                }
                if (Dispatch_Frequency.PinLv == Dispatch_Frequency.ofEnumByCode(etl_job_def.getDisp_freq())) {
                    log.warn("频率作业不能配置依赖关系:" + etl_job_def.getEtl_job());
                    continue;
                }
                if (Dispatch_Frequency.PinLv == Dispatch_Frequency.ofEnumByCode(etlJobDef.getDisp_freq())) {
                    log.warn("频率作业不能配置依赖关系:" + etlJobDef.getEtl_job());
                    continue;
                }
                if (EtlJobUtil.isEtlDependencyExist(etl_sys_id, pre_etl_sys_id, etlJobDef.getEtl_job_id(), etl_job_def.getEtl_job_id(), Dbo.db())) {
                    continue;
                }
                etlDependency.setEtl_sys_id(etl_sys_id);
                etlDependency.setPre_etl_sys_id(pre_etl_sys_id);
                etlDependency.setStatus(status);
                etlDependency.setEtl_job_id(etl_job_def.getEtl_job_id());
                etlDependency.setPre_etl_job_id(etlJobDef.getEtl_job_id());
                etlDependency.add(Dbo.db());
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "pre_etl_sys_cd", desc = "", range = "")
    @Param(name = "etl_job", desc = "", range = "")
    @Param(name = "pre_etl_job", desc = "", range = "")
    public void deleteEtlDependency(Long etl_sys_id, Long pre_etl_sys_id, Long etl_job_id, Long pre_etl_job_id) {
        Dbo.execute("delete from " + EtlDependency.TableName + " where etl_sys_id=? AND pre_etl_sys_id=? AND etl_job_id=? AND pre_etl_job_id=?", etl_sys_id, pre_etl_sys_id, etl_job_id, pre_etl_job_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "etl_job", desc = "", range = "")
    public void deleteEtlJobDef(Long etl_sys_id, Long etl_job_id) {
        deleteEtlJobDispHisIfExist(etl_sys_id, etl_job_id);
        DboExecute.deletesOrThrow("删除作业信息失败，etl_sys_cd=" + etl_sys_id + ",etl_job_id=" + etl_job_id, "delete from " + EtlJobDef.TableName + " where etl_sys_id=?" + " and etl_job_id=?", etl_sys_id, etl_job_id);
        deleteJobResourceRelationIfExist(etl_sys_id, etl_job_id);
        deleteJobDependencyIfExist(etl_sys_id, etl_job_id);
        Dbo.execute("delete from " + TakeRelationEtl.TableName + " where etl_sys_id=? and etl_job_id=?", etl_sys_id, etl_job_id);
        deleteEtlJobCurIfExist(etl_sys_id, etl_job_id);
        deleteEtlJobHandIfExist(etl_sys_id, etl_job_id);
        deleteEtlJobHandHisIfExixt(etl_sys_id, etl_job_id);
        deleteEtlJobCpidIfExist(etl_sys_id, etl_job_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    private void deleteEtlJobCpidIfExist(Long etl_sys_id, Long etl_job_id) {
        long count = SqlOperator.queryNumber(Dbo.db(), " select count(*) from " + EtlJobCpid.TableName + " where etl_sys_id = ? and etl_job_id = ?", etl_sys_id, etl_job_id).orElseThrow(() -> new BusinessException("查询SQL执行失败"));
        if (count > 0) {
            Dbo.execute("delete from " + EtlJobCpid.TableName + " where etl_sys_id = ? and etl_job_id = ?", etl_sys_id, etl_job_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    private void deleteEtlJobHandHisIfExixt(Long etl_sys_id, Long etl_job_id) {
        long count = SqlOperator.queryNumber(Dbo.db(), "select count(*) from " + EtlJobHandHis.TableName + " where etl_sys_id = ? and etl_job_id = ?", etl_sys_id, etl_job_id).orElseThrow(() -> new BusinessException("查询SQL执行失败"));
        if (count > 0) {
            Dbo.execute("delete from " + EtlJobHandHis.TableName + " where etl_sys_id = ? and etl_job_id = ?", etl_sys_id, etl_job_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    private void deleteEtlJobHandIfExist(Long etl_sys_id, Long etl_job_id) {
        long count = SqlOperator.queryNumber(Dbo.db(), "SELECT count(*) FROM " + EtlJobHand.TableName + " WHERE etl_sys_id = ? AND etl_job_id = ? ", etl_sys_id, etl_job_id).orElseThrow(() -> new BusinessException("执行查询SQL失败"));
        if (count > 0) {
            Dbo.execute("DELETE FROM " + EtlJobHand.TableName + " WHERE etl_sys_id = ? AND etl_job_id = ?", etl_sys_id, etl_job_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    private void deleteEtlJobDispHisIfExist(Long etl_sys_id, Long etl_job_id) {
        long count = SqlOperator.queryNumber(Dbo.db(), "select count(*) from " + EtlJobDispHis.TableName + " where etl_sys_id = ? and etl_job in (select etl_job from " + EtlJobDef.TableName + " where etl_job_id = ?) ", etl_sys_id, etl_job_id).orElseThrow(() -> new BusinessException("执行查询SQL失败"));
        if (count > 0) {
            Dbo.execute("delete from " + EtlJobDispHis.TableName + " where etl_sys_id = ? and etl_job in (select etl_job from " + EtlJobDef.TableName + " where etl_job_id = ?) ", etl_sys_id, etl_job_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    private void deleteEtlJobCurIfExist(Long etl_sys_id, Long etl_job_id) {
        long count = SqlOperator.queryNumber(Dbo.db(), "SELECT count(*) from " + EtlJobCur.TableName + " where etl_sys_id = ? and etl_job_id = ?", etl_sys_id, etl_job_id).orElseThrow(() -> new BusinessException("SQL执行错误"));
        if (count > 0) {
            Dbo.execute("delete from " + EtlJobCur.TableName + " where etl_sys_id = ? AND etl_job_id = ?", etl_sys_id, etl_job_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    public void deleteEtlJobResourceRela(Long etl_sys_id, Long etl_job_id) {
        DboExecute.deletesOrThrow("删除资源分配信息失败，etl_sys_id=" + etl_sys_id + ",etl_job_id=" + etl_job_id, "delete from " + EtlJobResourceRela.TableName + " where etl_sys_id =? AND etl_job_id = ?", etl_sys_id, etl_job_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "para_cd", desc = "", range = "")
    public void deleteEtlPara(Long etl_sys_id, String para_cd) {
        DboExecute.deletesOrThrow("删除作业系统参数失败，etl_sys_cd=" + etl_sys_id + ",para_cd=" + para_cd, "delete from " + EtlPara.TableName + " where etl_sys_id = ? AND para_cd = ?", etl_sys_id, para_cd);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "resource_type", desc = "", range = "")
    public void deleteEtlResource(Long etl_sys_id, String resource_type) {
        if (EtlJobUtil.isEtlJobResourceRelaExistByType(etl_sys_id, resource_type, Dbo.db())) {
            throw new BusinessException(resource_type + "资源下已经分配了作业资源不能删除");
        }
        DboExecute.deletesOrThrow("删除作业系统参数失败，etl_sys_cd=" + etl_sys_id + ",resource_type=" + resource_type, "delete from " + EtlResource.TableName + " where etl_sys_id = ? AND resource_type = ?", etl_sys_id, resource_type);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "sub_sys_id", desc = "", range = "")
    public void deleteEtlSubSys(Long etl_sys_id, Long sub_sys_id) {
        EtlJobUtil.isEtlJobDefExistUnderEtlSubSys(etl_sys_id, sub_sys_id, Dbo.db());
        DboExecute.deletesOrThrow("删除任务失败，etl_sys_id=" + etl_sys_id + ",sub_sys_id=" + sub_sys_id, "delete from " + EtlSubSysList.TableName + " where etl_sys_id=? " + " and sub_sys_id=?", etl_sys_id, sub_sys_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    public String generateExcel(Long etl_sys_id, String tableName, Long userId) {
        FileOutputStream out = null;
        XSSFWorkbook workbook = null;
        try {
            if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
                throw new BusinessException("当前工程已不存在:" + etl_sys_id);
            }
            workbook = new XSSFWorkbook();
            XSSFSheet sheet = workbook.createSheet("sheet1");
            XSSFRow headRow = sheet.createRow(0);
            String savePath = WebinfoProperties.FileUpload_SavedDirName + File.separator + tableName + xlsxSuffix;
            File file = new File(savePath);
            if (!new File(file.getParent()).exists()) {
                FileUtil.forceMkdirParent(file);
            }
            if (!file.exists()) {
                if (!file.createNewFile()) {
                    throw new BusinessException("创建文件失败:" + file.getAbsolutePath());
                }
            }
            out = new FileOutputStream(file);
            List<TableMeta> tableMetas = MetaOperator.getTablesWithColumns(Dbo.db(), tableName);
            Set<String> columnNames = tableMetas.get(0).getColumnNames();
            XSSFDrawing xssfDrawing = sheet.createDrawingPatriarch();
            int cellNum = 0;
            for (String columnName : columnNames) {
                XSSFCell createCell = headRow.createCell(cellNum);
                createCell.setCellValue(columnName + Constant.LXKH + ConvertColumnNameToChinese.getZh_name(columnName) + Constant.RXKH);
                String comments = getCodeValueByColumn(columnName);
                if (StringUtil.isNotBlank(comments)) {
                    XSSFComment comment = xssfDrawing.createCellComment(new XSSFClientAnchor(0, 0, 0, 0, (short) 4, 2, (short) 6, 5));
                    comment.setString(new XSSFRichTextString(comments));
                    createCell.setCellComment(comment);
                }
                cellNum++;
            }
            List<List<String>> columnValList = new ArrayList<>();
            List<Map<String, Object>> tableInfoList = getTableInfo(etl_sys_id, tableName);
            if (!tableInfoList.isEmpty()) {
                for (Map<String, Object> tableInfo : tableInfoList) {
                    List<String> columnInfoList = new ArrayList<>();
                    for (String columnName : columnNames) {
                        if (tableInfo.get(columnName) != null) {
                            columnInfoList.add(tableInfo.get(columnName).toString());
                        } else {
                            columnInfoList.add("");
                        }
                    }
                    columnValList.add(columnInfoList);
                }
            }
            if (!columnValList.isEmpty()) {
                for (int i = 0; i < columnValList.size(); i++) {
                    headRow = sheet.createRow(i + 1);
                    List<String> valueList = columnValList.get(i);
                    for (int j = 0; j < valueList.size(); j++) {
                        headRow.createCell(j).setCellValue(valueList.get(j));
                    }
                }
            }
            workbook.write(out);
            return tableName + xlsxSuffix;
        } catch (FileNotFoundException e) {
            log.error(e.getMessage());
            throw new BusinessException("文件不存在:" + e.getMessage());
        } catch (IOException e) {
            log.error(e.getMessage());
            throw new BusinessException("生成excel文件失败:" + e.getMessage());
        } catch (Exception e) {
            throw new AppSystemException(e);
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException e) {
                log.error("关闭输出流失败", e);
            }
            ExcelUtil.close(workbook);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_dependency", desc = "", range = "", isBean = true)
    public void saveEtlDependency(EtlDependency etl_dependency, Long userId) {
        checkEtlDependencyField(etl_dependency, userId);
        if (EtlJobUtil.isEtlSysExistById(etl_dependency.getEtl_sys_id(), userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        if (EtlJobUtil.isEtlDependencyExist(etl_dependency.getEtl_sys_id(), etl_dependency.getPre_etl_sys_id(), etl_dependency.getEtl_job_id(), etl_dependency.getPre_etl_job_id(), Dbo.db())) {
            throw new BusinessException("当前工程对应作业的依赖已存在！");
        }
        if (EtlJobUtil.isEtlDependencyExist(etl_dependency.getEtl_sys_id(), etl_dependency.getPre_etl_sys_id(), etl_dependency.getPre_etl_job_id(), etl_dependency.getEtl_job_id(), Dbo.db())) {
            throw new BusinessException("当前工程对应作业的依赖已存在！");
        }
        if (etl_dependency.getEtl_job_id().equals(etl_dependency.getPre_etl_job_id())) {
            throw new BusinessException("新增依赖当前作业名称与上游作业名称不能相同！");
        }
        isPinLvDependency(etl_dependency);
        etl_dependency.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_error_resource", desc = "", range = "", isBean = true)
    public void saveEtlErrorResource(EtlErrorResource etl_error_resource) {
        if (!NumberUtil.isNumberic(String.valueOf(etl_error_resource.getStart_number()))) {
            throw new BusinessException("作业失败重试次数必须为纯数字!");
        }
        if (!NumberUtil.isNumberic(String.valueOf(etl_error_resource.getStart_interval()))) {
            throw new BusinessException("作业失败重试间隔必须为纯数字!");
        }
        long ret = Dbo.queryNumber("select count(1) from " + EtlErrorResource.TableName + " where etl_sys_id = ?", etl_error_resource.getEtl_sys_id()).orElse(0);
        if (ret != 0) {
            etl_error_resource.update(Dbo.db());
        } else {
            etl_error_resource.add(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_job_def", desc = "", range = "", isBean = true)
    @Param(name = "etl_dependency", desc = "", range = "", isBean = true)
    @Param(name = "pre_etl_job", desc = "", range = "", nullable = true)
    public void saveEtlJobDef(EtlJobDef etl_job_def, EtlDependency etl_dependency, Long[] pre_etl_job_id) {
        checkEtlJobDefField(etl_job_def);
        if (isEtlJobDefExist(etl_job_def.getEtl_sys_id(), etl_job_def.getEtl_job(), Dbo.db())) {
            throw new BusinessException("作业名称已存在不能新增!");
        }
        isThriftOrYarnProType(etl_job_def.getEtl_sys_id(), etl_job_def.getEtl_job_id(), etl_job_def.getPro_type());
        if (StringUtil.isNotBlank(etl_job_def.getDisp_type())) {
            if (Dispatch_Type.DEPENDENCE == Dispatch_Type.ofEnumByCode(etl_job_def.getDisp_type())) {
                saveEtlDependencyFromEtlJobDef(etl_dependency, pre_etl_job_id);
            }
        }
        isDispatchFrequency(etl_job_def);
        etl_job_def.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Return(desc = "", range = "")
    private static boolean isEtlJobDefExist(Long etl_sys_id, String etl_job, DatabaseWrapper db) {
        return SqlOperator.queryNumber(db, "SELECT count(1) FROM " + EtlJobDef.TableName + " WHERE etl_job=? AND etl_sys_id=?", etl_job, etl_sys_id).orElseThrow(() -> new BusinessException("sql查询错误")) > 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "jobResourceRelation", desc = "", range = "", isBean = true)
    public void saveEtlJobResourceRela(EtlJobResourceRela jobResourceRelation) {
        checkEtlJobResourceRelaField(jobResourceRelation);
        if (EtlJobUtil.isEtlJobResourceRelaExist(jobResourceRelation.getEtl_sys_id(), jobResourceRelation.getEtl_job_id(), Dbo.db())) {
            throw new BusinessException("当前工程对应作业资源分配信息已存在，不能新增！");
        }
        EtlJobUtil.isResourceDemandTooLarge(jobResourceRelation.getEtl_sys_id(), jobResourceRelation.getResource_type(), jobResourceRelation.getResource_req(), Dbo.db());
        jobResourceRelation.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "sub_sys_id", desc = "", range = "")
    @Param(name = "etl_job", desc = "", range = "")
    @Param(name = "job_datasource", desc = "", range = "")
    @Param(name = "etl_temp_id", desc = "", range = "")
    @Param(name = "etl_job_temp_para", desc = "", range = "")
    public void saveEtlJobTemp(Long etl_sys_id, Long sub_sys_id, String etl_job, String job_datasource, long etl_temp_id, String[] etl_job_temp_para) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < etl_job_temp_para.length; i++) {
            String value = etl_job_temp_para[i];
            if (i != etl_job_temp_para.length - 1) {
                sb.append(value).append(Constant.ETLPARASEPARATOR);
            } else {
                sb.append(value);
            }
        }
        EtlJobDef etl_job_def = new EtlJobDef();
        etl_job_def.setEtl_job_id(PrimayKeyGener.getNextId());
        etl_job_def.setEtl_job(etl_job);
        etl_job_def.setEtl_sys_id(etl_sys_id);
        etl_job_def.setSub_sys_id(sub_sys_id);
        etl_job_def.setPro_para(sb.toString());
        etl_job_def.setPro_type(Pro_Type.SHELL.getCode());
        etl_job_def.setEtl_job_desc(etl_job);
        etl_job_def.setDisp_type(Dispatch_Type.DEPENDENCE.getCode());
        etl_job_def.setJob_eff_flag(Job_Effective_Flag.YES.getCode());
        etl_job_def.setToday_disp(Today_Dispatch_Flag.YES.getCode());
        etl_job_def.setDisp_freq(Dispatch_Frequency.DAILY.getCode());
        etl_job_def.setUpd_time(DateUtil.parseStr2DateWith8Char(DateUtil.getSysDate()) + " " + DateUtil.parseStr2TimeWith6Char(DateUtil.getSysTime()));
        etl_job_def.setMain_serv_sync(Main_Server_Sync.YES.getCode());
        etl_job_def.setJob_datasource(job_datasource);
        Map<String, Object> jobTemplate = searchEtlJobTemplateById(etl_temp_id);
        if (!jobTemplate.isEmpty()) {
            etl_job_def.setPro_dic(jobTemplate.get("pro_dic").toString());
            etl_job_def.setPro_name(jobTemplate.get("pro_name").toString());
            etl_job_def.setLog_dic(jobTemplate.get("pro_dic").toString());
        }
        if (isEtlJobDefExist(etl_sys_id, etl_job, Dbo.db())) {
            throw new BusinessException("作业名称已存在不能新增!");
        }
        EtlDependency etlDependency = new EtlDependency();
        etlDependency.setEtl_sys_id(etl_sys_id);
        etlDependency.setPre_etl_sys_id(etl_job_def.getEtl_job_id());
        etlDependency.setEtl_job_id(etl_job);
        saveEtlJobDef(etl_job_def, etlDependency, new Long[] {});
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_para", desc = "", range = "", isBean = true)
    public void saveEtlPara(EtlPara etl_para, Long userId) {
        checkEtlParaField(etl_para);
        if (EtlJobUtil.isEtlSysExistById(etl_para.getEtl_sys_id(), userId, Dbo.db())) {
            throw new BusinessException("当前用户对应工程已不存在！");
        }
        String para_cd = PREFIX + etl_para.getPara_cd();
        if (EtlJobUtil.isEtlParaExist(etl_para.getEtl_sys_id(), para_cd, Dbo.db())) {
            throw new BusinessException("作业系统参数变量名称已存在,不能新增！");
        }
        etl_para.setPara_cd(para_cd);
        etl_para.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_resource", desc = "", range = "", isBean = true)
    public void saveEtlResource(EtlResource etl_resource, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_resource.getEtl_sys_id(), userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        checkEtlResourceField(etl_resource);
        if (EtlJobUtil.isEtlResourceExist(etl_resource.getEtl_sys_id(), etl_resource.getResource_type(), Dbo.db())) {
            throw new BusinessException("当前工程对应的资源已存在,不能新增！");
        }
        etl_resource.setMain_serv_sync(Main_Server_Sync.YES.getCode());
        etl_resource.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sub_sys_list", desc = "", range = "", isBean = true)
    public void saveEtlSubSys(EtlSubSysList etl_sub_sys_list, Long userId) {
        checkEtlSubSysField(etl_sub_sys_list);
        if (EtlJobUtil.isEtlSysExistById(etl_sub_sys_list.getEtl_sys_id(), userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在！");
        }
        if (isEtlSubSysExist(etl_sub_sys_list.getEtl_sys_id(), etl_sub_sys_list.getSub_sys_cd(), Dbo.db())) {
            throw new BusinessException("该工程对应的任务已存在，不能新增！");
        }
        etl_sub_sys_list.setSub_sys_id(PrimayKeyGener.getNextId());
        etl_sub_sys_list.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "sub_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    private static boolean isEtlSubSysExist(Long etl_sys_id, String sub_sys_cd, DatabaseWrapper db) {
        return SqlOperator.queryNumber(db, "SELECT count(1) FROM " + EtlSubSysList.TableName + " WHERE etl_sys_id=? AND sub_sys_cd=?", etl_sys_id, sub_sys_cd).orElseThrow(() -> new BusinessException("sql查询错误")) == 1;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "", nullable = true)
    @Param(name = "etl_job", desc = "", range = "", nullable = true)
    @Param(name = "pre_etl_job", desc = "", range = "", nullable = true)
    @Param(name = "pageType", desc = "", range = "", valueIfNull = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> searchEtlDependencyByPage(Long etl_sys_id, Long etl_job_id, String etl_job, String pre_etl_job, String pageType, int currPage, int pageSize, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select ed.etl_sys_id," + "job1.etl_job_id,job1.etl_job," + "ed.pre_etl_sys_id,sys.etl_sys_cd as \"pre_etl_sys_cd\"," + "ed.pre_etl_job_id,job2.etl_job as \"pre_etl_job\",ed.status,ed.main_serv_sync " + " from " + EtlDependency.TableName + " ed " + " left join " + EtlJobDef.TableName + " job1 on ed.etl_job_id = job1.etl_job_id " + " left join " + EtlSys.TableName + " sys on ed.pre_etl_sys_id = sys.etl_sys_id " + " left join " + EtlJobDef.TableName + " job2 on ed.pre_etl_job_id = job2.etl_job_id " + " where ed.etl_sys_id = ?").addParam(etl_sys_id);
        if (etl_job_id != null) {
            asmSql.addSql(" and ed.etl_job_id = ? ").addParam(etl_job_id);
        }
        if (StringUtil.isNotBlank(etl_job)) {
            if (StringUtil.isNotEmpty(pageType)) {
                List<Map<String, Object>> array = fd.ng.core.utils.JsonUtil.toObject(etl_job, new TypeReference<List<Map<String, Object>>>() {
                });
                asmSql.addLikeParam("lower(job1.etl_job)", "%" + array.get(0).toString().toLowerCase() + "%");
                for (int i = 1; i < array.size(); i++) {
                    asmSql.addLikeParam("lower(job1.etl_job)", "%" + array.get(i).toString().toLowerCase() + "%");
                }
                asmSql.addSql(")");
            } else {
                asmSql.addLikeParam("lower(job1.etl_job)", "%" + etl_job.toLowerCase() + "%");
            }
        }
        if (StringUtil.isNotBlank(pre_etl_job)) {
            asmSql.addLikeParam("lower(job2.etl_job)", "%" + pre_etl_job.toLowerCase() + "%");
        }
        asmSql.addSql(" order by ed.etl_sys_id,ed.etl_job_id");
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> etlDependencyList = Dbo.queryPagedList(page, asmSql.sql(), asmSql.params());
        Map<String, Object> etlDependencyMap = new HashMap<>();
        etlDependencyMap.put("etlDependencyList", etlDependencyList);
        etlDependencyMap.put("totalSize", page.getTotalSize());
        return etlDependencyMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result searchEtlErrorResource(Long etl_sys_id) {
        return Dbo.queryResult("select * from " + EtlErrorResource.TableName + " where etl_sys_id = ? ", etl_sys_id);
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result searchEtlJobTemplate() {
        return Dbo.queryResult("select * from " + EtlJobTemp.TableName);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> searchEtlJobDefById(Long etl_sys_id, Long etl_job_id) {
        if (!EtlJobUtil.isEtlJobDefExist(etl_sys_id, etl_job_id, Dbo.db())) {
            throw new BusinessException("当前工程下作业已不存在！");
        }
        Map<String, Object> etlJobDef = EtlJobUtil.getEtlJobByJob(etl_sys_id, etl_job_id, Dbo.db());
        List<EtlDependency> dependencyList = Dbo.queryList(EtlDependency.class, "select pre_etl_sys_id" + ",pre_etl_job_id,status FROM " + EtlDependency.TableName + " WHERE etl_sys_id=? AND etl_job_id=?", etl_sys_id, etl_job_id);
        etlJobDef.put("dependencyList", dependencyList);
        return etlJobDef;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "pro_type", desc = "", range = "", nullable = true)
    @Param(name = "etl_job", desc = "", range = "", nullable = true)
    @Param(name = "pro_name", desc = "", range = "", nullable = true)
    @Param(name = "sub_sys_cd", desc = "", range = "", nullable = true)
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> searchEtlJobDefByPage(Long etl_sys_id, String pro_type, String etl_job, String pro_name, String sub_sys_cd, int currPage, int pageSize, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        StringBuilder sb = new StringBuilder();
        sb.append("select \n" + "\tt1.etl_job_id,\n" + "\tt1.etl_sys_id,\n" + "\tt1.etl_job,\n" + "\tt1.etl_job_desc,\n" + "\tt1.pro_name,\n" + "\tt1.disp_freq,\n" + "\tt1.disp_type,\n" + "\tt1.job_eff_flag,\n" + "\tt1.upd_time,\n" + "\tt1.today_disp,\n" + "\tt3.sub_sys_cd,\n" + "\tt1.pro_type,\n" + "\tt2.etl_sys_name,\n" + "\tt2.etl_sys_cd,\n" + "\tt1.job_datasource \n" + "FROM\n" + "\tetl_job_def t1\n" + "\tLEFT JOIN etl_sys t2 ON t1.etl_sys_id = t2.etl_sys_id\n" + "\tLEFT JOIN etl_sub_sys_list t3 ON t1.sub_sys_id = t3.sub_sys_id \n" + "WHERE\n" + "\tt1.etl_sys_id = " + etl_sys_id + " AND \n" + "\tt2.user_id = " + userId + " ");
        if (StringUtil.isNotBlank(pro_type)) {
            sb.append("AND pro_type = '" + pro_type + "'");
        }
        if (StringUtil.isNotBlank(etl_job)) {
            sb.append(" AND lower(etl_job) like '%" + etl_job.toLowerCase() + "%'");
        }
        if (StringUtil.isNotBlank(pro_name)) {
            sb.append(" AND lower(pro_name) like '%" + pro_name.toLowerCase() + "%'");
        }
        if (StringUtil.isNotBlank(sub_sys_cd)) {
            sb.append(" AND lower(sub_sys_cd) like '%" + sub_sys_cd.toLowerCase() + "%'");
        }
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> etlJobDefList = Dbo.queryPagedList(page, sb.toString());
        Map<String, Object> etlJobDefMap = new HashMap<>();
        etlJobDefMap.put("etlJobDefList", etlJobDefList);
        etlJobDefMap.put("totalSize", page.getTotalSize());
        return etlJobDefMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "etl_job", desc = "", range = "", nullable = true)
    @Param(name = "pageType", desc = "", range = "", valueIfNull = "")
    @Param(name = "resource_type", desc = "", range = "", nullable = true)
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> searchEtlJobResourceRelaByPage(Long etl_sys_id, String etl_job, String pageType, String resource_type, int currPage, int pageSize, Long userId) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        StringBuilder sb = new StringBuilder();
        sb.append("select * from " + EtlJobResourceRela.TableName + " where etl_sys_id= " + etl_sys_id);
        if (StringUtil.isNotBlank(etl_job)) {
            if (StringUtil.isNotEmpty(pageType)) {
                String[] strList = fd.ng.core.utils.JsonUtil.toObject(etl_job, new TypeReference<String[]>() {
                });
                StringBuilder sb1 = new StringBuilder();
                sb1.append("select etl_job_id from " + EtlJobDef.TableName + " where LOWER (etl_job) like '%" + strList[0] + "%' ");
                for (int i = 1; i < strList.length; i++) {
                    sb1.append("or LOWER (etl_job) like '%" + strList[i] + "%' ");
                }
                List<Long> etl_job_ids = Dbo.queryOneColumnList(sb1.toString());
                sb.append(" and etl_job_id in ( ");
                for (Long aLong : etl_job_ids) {
                    sb.append(aLong + ",");
                }
                sb.replace(sb.lastIndexOf(","), sb.lastIndexOf(",") + 1, ")");
            } else {
                List<Long> etl_job_ids = Dbo.queryOneColumnList("select etl_job_id from " + EtlJobDef.TableName + " where LOWER (etl_job) like '%" + etl_job + "%' ");
                if (!etl_job_ids.isEmpty()) {
                    sb.append(" and etl_job_id in ( ");
                    for (Long aLong : etl_job_ids) {
                        sb.append(aLong + ",");
                    }
                    sb.replace(sb.lastIndexOf(","), sb.lastIndexOf(",") + 1, ")");
                }
            }
        }
        if (StringUtil.isNotBlank(resource_type)) {
            sb.append(" and lower(resource_type) like '%" + resource_type.toLowerCase() + "%'");
        }
        sb.append(" order by etl_sys_id,etl_job_id");
        Page page = new DefaultPageImpl(currPage, pageSize);
        Result result = Dbo.queryResult("select resource_name,resource_type from etl_resource where etl_sys_id = ?", etl_sys_id);
        Result eltJobResult = Dbo.queryResult("select etl_job_id,etl_job from " + EtlJobDef.TableName + " where etl_sys_id=?", etl_sys_id);
        List<Map<String, Object>> resourceRelation = Dbo.queryPagedList(page, sb.toString());
        resourceRelation.forEach(relation -> {
            for (int i = 0; i < result.getRowCount(); i++) {
                if (relation.get("resource_type").equals(result.getString(i, "resource_type"))) {
                    relation.put("resource_name", result.getString(i, "resource_name"));
                }
            }
            for (int i = 0; i < eltJobResult.getRowCount(); i++) {
                if (relation.get("etl_job_id").equals(eltJobResult.getLongObject(i, "etl_job_id"))) {
                    relation.put("etl_job", eltJobResult.getString(i, "etl_job"));
                }
            }
        });
        Map<String, Object> resourceRelationMap = new HashMap<>();
        resourceRelationMap.put("jobResourceRelation", resourceRelation);
        resourceRelationMap.put("totalSize", page.getTotalSize());
        return resourceRelationMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_temp_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> searchEtlJobTempAndParam(long etl_temp_id) {
        return Dbo.queryList("SELECT * FROM " + EtlJobTemp.TableName + " t1," + EtlJobTempPara.TableName + " t2 where t1.etl_temp_id=t2.etl_temp_id " + " AND t1.etl_temp_id=? order by etl_pro_para_sort", etl_temp_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "para_cd", desc = "", range = "", nullable = true)
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> searchEtlParaByPage(Long etl_sys_id, String para_cd, int currPage, int pageSize, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + EtlPara.TableName + " where etl_sys_id IN (?,?)");
        asmSql.addParam(etl_sys_id);
        asmSql.addParam(1000000000000000000L);
        if (StringUtil.isNotBlank(para_cd)) {
            asmSql.addLikeParam("lower(para_cd)", "%" + para_cd.toLowerCase() + "%");
        }
        asmSql.addSql(" order by etl_sys_id,para_cd");
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> etlParaList = Dbo.queryPagedList(page, asmSql.sql(), asmSql.params());
        Map<String, Object> etlParaMap = new HashMap<>();
        etlParaMap.put("etlParaList", etlParaList);
        etlParaMap.put("totalSize", page.getTotalSize());
        return etlParaMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "resource_type", desc = "", range = "", nullable = true)
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> searchEtlResourceByPage(Long etl_sys_id, String resource_type, int currPage, int pageSize, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + EtlResource.TableName + " where etl_sys_id = ?");
        asmSql.addParam(etl_sys_id);
        if (StringUtil.isNotBlank(resource_type)) {
            asmSql.addLikeParam("lower(resource_type)", "%" + resource_type.toLowerCase() + "%");
        }
        asmSql.addSql(" order by etl_sys_id,resource_type");
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> etlResourceList = Dbo.queryPagedList(page, asmSql.sql(), asmSql.params());
        Map<String, Object> etlResourceMap = new HashMap<>();
        etlResourceMap.put("etlResourceList", etlResourceList);
        etlResourceMap.put("totalSize", page.getTotalSize());
        return etlResourceMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<String> searchEtlResourceType(Long etl_sys_id, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        return Dbo.queryOneColumnList("select concat(resource_name,'(',resource_type,')') as resource_type from " + EtlResource.TableName + " where etl_sys_id=?", etl_sys_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "sub_sys_cd", desc = "", range = "", nullable = true)
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> searchEtlSubSysByPage(Long etl_sys_id, String sub_sys_cd, int currPage, int pageSize, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + EtlSubSysList.TableName + " where etl_sys_id = ?");
        asmSql.addParam(etl_sys_id);
        if (StringUtil.isNotBlank(sub_sys_cd)) {
            asmSql.addLikeParam("lower(sub_sys_cd)", "%" + sub_sys_cd.toLowerCase() + "%");
        }
        asmSql.addSql(" order by etl_sys_id,sub_sys_cd");
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> etlSubSysList = Dbo.queryPagedList(page, asmSql.sql(), asmSql.params());
        Map<String, Object> etlSubSysMap = new HashMap<>();
        etlSubSysMap.put("etlSubSysList", etlSubSysList);
        etlSubSysMap.put("totalSize", page.getTotalSize());
        return etlSubSysMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<EtlSubSysList> searchEtlSubSys(Long etl_sys_id, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        return Dbo.queryList(EtlSubSysList.class, "select * from " + EtlSubSysList.TableName + " where etl_sys_id=? order by etl_sys_id, sub_sys_id", etl_sys_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> searchEtlJob(Long etl_sys_id, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        return Dbo.queryList("select etl_job,etl_job_id from " + EtlJobDef.TableName + " where etl_sys_id=?", etl_sys_id);
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
    @Param(name = "etl_job", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Long> searchJobDependency(Long etl_sys_id, Long[] etl_job_id) {
        Map<String, Long> map = new HashMap<>();
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.clean();
        assembler.addSql("select count(1) from " + EtlDependency.TableName + " where etl_sys_id = ?").addParam(etl_sys_id);
        assembler.addORParam("etl_job_id", etl_job_id);
        long dependcy_count = Dbo.queryNumber(assembler.sql(), assembler.params()).orElseThrow(() -> new BusinessException("查询失败!"));
        assembler.clean();
        assembler.addSql("select count(1) from " + EtlJobResourceRela.TableName + " where etl_sys_id = ?").addParam(etl_sys_id);
        assembler.addORParam("etl_job_id", etl_job_id);
        long resource_count = Dbo.queryNumber(assembler.sql(), assembler.params()).orElseThrow(() -> new BusinessException("查询失败!"));
        map.put("dependcy_count", dependcy_count);
        map.put("resource_count", resource_count);
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etlDependency", desc = "", range = "", isBean = true)
    @Param(name = "oldEtlJob", desc = "", range = "")
    @Param(name = "oldPreEtlJob", desc = "", range = "")
    public void updateEtlDependency(EtlDependency etlDependency, Long oldEtlJobId, Long oldPreEtlJobId, Long userId) {
        checkEtlDependencyField(etlDependency, userId);
        if (etlDependency.getEtl_job_id().equals(oldEtlJobId) && oldPreEtlJobId.equals(etlDependency.getPre_etl_job_id())) {
            DboExecute.updatesOrThrow("更新作业依赖失败", "update " + EtlDependency.TableName + " set status=? " + " where etl_sys_id=? and pre_etl_sys_id=? and etl_job_id=? and pre_etl_job_id=?", etlDependency.getStatus(), etlDependency.getEtl_sys_id(), etlDependency.getPre_etl_sys_id(), etlDependency.getEtl_job_id(), etlDependency.getPre_etl_job_id());
        } else {
            if (EtlJobUtil.isEtlDependencyExist(etlDependency.getEtl_sys_id(), etlDependency.getPre_etl_sys_id(), oldEtlJobId, etlDependency.getPre_etl_job_id(), Dbo.db())) {
                throw new BusinessException("更新前作业名称对应更新后上游作业名称对应依赖已存在");
            }
            DboExecute.updatesOrThrow("更新作业依赖失败", "update " + EtlDependency.TableName + " set etl_job_id=?,pre_etl_sys_id=?,pre_etl_job_id=?," + "status=? where etl_sys_id=? and etl_job_id=? and pre_etl_job_id=?", etlDependency.getEtl_job_id(), etlDependency.getPre_etl_sys_id(), etlDependency.getPre_etl_job_id(), etlDependency.getStatus(), etlDependency.getEtl_sys_id(), oldEtlJobId, oldPreEtlJobId);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_job_def", desc = "", range = "", isBean = true)
    @Param(name = "etl_dependency", desc = "", range = "", isBean = true)
    @Param(name = "old_disp_freq", desc = "", range = "")
    @Param(name = "old_pre_etl_job", desc = "", range = "", nullable = true)
    @Param(name = "old_dispatch_type", desc = "", range = "", nullable = true)
    @Param(name = "pre_etl_job", desc = "", range = "", nullable = true)
    public void updateEtlJobDef(EtlJobDef etl_job_def, EtlDependency etl_dependency, String old_disp_freq, Long[] old_pre_etl_job, String old_dispatch_type, Long[] pre_etl_job_id) {
        checkEtlJobDefField(etl_job_def);
        if (Dispatch_Frequency.ofEnumByCode(old_disp_freq) != Dispatch_Frequency.PinLv) {
            if (StringUtil.isBlank(old_dispatch_type)) {
                throw new BusinessException("更新前调度频率不是频率时old_dispatch_type不可以为空！");
            }
            Dispatch_Type.ofEnumByCode(old_dispatch_type);
            if (Dispatch_Frequency.ofEnumByCode(etl_job_def.getDisp_freq()) != Dispatch_Frequency.PinLv) {
                if (Dispatch_Type.DEPENDENCE == Dispatch_Type.ofEnumByCode(old_dispatch_type)) {
                    if (Dispatch_Type.DEPENDENCE == Dispatch_Type.ofEnumByCode(etl_job_def.getDisp_type())) {
                        updateDependencyFromEtlJobDef(etl_dependency, old_pre_etl_job, pre_etl_job_id);
                    } else {
                        if (old_pre_etl_job != null && old_pre_etl_job.length != 0) {
                            deleteOldDependency(etl_dependency, old_pre_etl_job);
                        }
                    }
                } else {
                    if (Dispatch_Type.DEPENDENCE == Dispatch_Type.ofEnumByCode(etl_job_def.getDisp_type())) {
                        saveEtlDependencyFromEtlJobDef(etl_dependency, pre_etl_job_id);
                    }
                }
            }
            isThriftOrYarnProType(etl_job_def.getEtl_sys_id(), etl_job_def.getEtl_job_id(), etl_job_def.getPro_type());
            isDispatchFrequency(etl_job_def);
            etl_job_def.setUpd_time(DateUtil.parseStr2DateWith8Char(DateUtil.getSysDate()) + " " + DateUtil.parseStr2TimeWith6Char(DateUtil.getSysTime()));
            etl_job_def.update(Dbo.db());
        } else {
            deleteEtlJobDef(etl_job_def.getEtl_sys_id(), etl_job_def.getEtl_job_id());
            saveEtlJobDef(etl_job_def, etl_dependency, pre_etl_job_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "jobResourceRelation", desc = "", range = "", isBean = true)
    public void updateEtlJobResourceRela(EtlJobResourceRela jobResourceRelation) {
        checkEtlJobResourceRelaField(jobResourceRelation);
        EtlJobUtil.isResourceDemandTooLarge(jobResourceRelation.getEtl_sys_id(), jobResourceRelation.getResource_type(), jobResourceRelation.getResource_req(), Dbo.db());
        jobResourceRelation.update(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_para", desc = "", range = "")
    public void updateEtlPara(EtlPara etl_para, Long userId) {
        checkEtlParaField(etl_para);
        if (EtlJobUtil.isEtlSysExistById(etl_para.getEtl_sys_id(), userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        etl_para.update(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "resource_type", desc = "", range = "")
    @Param(name = "resource_max", desc = "", range = "")
    @Param(name = "resource_name", desc = "", range = "", nullable = true)
    public void updateEtlResource(Long etl_sys_id, String resource_type, long resource_max, String resource_name) {
        if (!EtlJobUtil.isEtlResourceExist(etl_sys_id, resource_type, Dbo.db())) {
            throw new BusinessException("当前工程对应的资源已不存在！");
        }
        DboExecute.updatesOrThrow("更新资源失败，etl_sys_id=" + etl_sys_id + ",resource_type=" + resource_type, "update " + EtlResource.TableName + " set resource_max=? , resource_name=? " + " where etl_sys_id=? and resource_type=?", resource_max, resource_name, etl_sys_id, resource_type);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sub_sys_list", desc = "", range = "", isBean = true)
    public void updateEtlSubSys(EtlSubSysList etl_sub_sys_list, Long userId) {
        checkEtlSubSysField(etl_sub_sys_list);
        if (EtlJobUtil.isEtlSysExistById(etl_sub_sys_list.getEtl_sys_id(), userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        etl_sub_sys_list.update(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    private void deleteJobResourceRelationIfExist(Long etl_sys_id, Long etl_job_id) {
        if (EtlJobUtil.isEtlJobResourceRelaExist(etl_sys_id, etl_job_id, Dbo.db())) {
            Dbo.execute("delete from " + EtlJobResourceRela.TableName + " where etl_sys_id =? AND etl_job_id = ?", etl_sys_id, etl_job_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "etl_job", desc = "", range = "")
    private void deleteJobDependencyIfExist(Long etl_sys_id, Long etl_job_id) {
        if (Dbo.queryNumber("select count(*) from " + EtlDependency.TableName + " where etl_sys_id=?" + " and etl_job_id=?", etl_sys_id, etl_job_id).orElseThrow(() -> new BusinessException("sql查询错误")) > 0) {
            Dbo.execute("delete from " + EtlDependency.TableName + " where etl_sys_id=? AND etl_job_id=?", etl_sys_id, etl_job_id);
        }
        if (Dbo.queryNumber("select count(*) from " + EtlDependency.TableName + " where etl_sys_id=?" + " and pre_etl_job_id=?", etl_sys_id, etl_job_id).orElseThrow(() -> new BusinessException("sql查询错误")) > 0) {
            Dbo.execute("delete from " + EtlDependency.TableName + " where etl_sys_id=? AND pre_etl_job_id=?", etl_sys_id, etl_job_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "type", desc = "", range = "")
    @Return(desc = "", range = "")
    private String getCodeValueByColumn(String type) {
        StringBuilder sb = new StringBuilder();
        switch(type.toLowerCase()) {
            case pro_type:
                Pro_Type[] proTypes = Pro_Type.values();
                sb.append("详细说明：");
                for (Pro_Type proType : proTypes) {
                    sb.append(System.lineSeparator()).append(proType.getCode()).append(" ：").append(proType.getValue());
                }
                break;
            case disp_freq:
                Dispatch_Frequency[] dispatchFrequencies = Dispatch_Frequency.values();
                sb.append("详细说明：");
                for (Dispatch_Frequency frequency : dispatchFrequencies) {
                    sb.append(System.lineSeparator()).append(frequency.getCode()).append(" ：").append(frequency.getValue());
                }
                break;
            case disp_type:
                Dispatch_Type[] dispatchTypes = Dispatch_Type.values();
                sb.append("详细说明：");
                for (Dispatch_Type dispatchType : dispatchTypes) {
                    sb.append(System.lineSeparator()).append(dispatchType.getCode()).append(" ：").append(dispatchType.getValue());
                }
                break;
            case job_eff_flag:
                Job_Effective_Flag[] effectiveFlags = Job_Effective_Flag.values();
                sb.append("详细说明：");
                for (Job_Effective_Flag effectiveFlag : effectiveFlags) {
                    sb.append(System.lineSeparator()).append(effectiveFlag.getCode()).append(" ：").append(effectiveFlag.getValue());
                }
                break;
            case job_disp_status:
                Job_Status[] jobStatuses = Job_Status.values();
                sb.append("详细说明：");
                for (Job_Status jobStatus : jobStatuses) {
                    sb.append(System.lineSeparator()).append(jobStatus.getCode()).append(" ：").append(jobStatus.getValue());
                }
                break;
            case today_disp:
                Today_Dispatch_Flag[] todayDispatchFlags = Today_Dispatch_Flag.values();
                sb.append("详细说明：");
                for (Today_Dispatch_Flag todayDispatchFlag : todayDispatchFlags) {
                    sb.append(System.lineSeparator()).append(todayDispatchFlag.getCode()).append(" ：").append(todayDispatchFlag.getValue());
                }
                break;
            case main_serv_sync:
                Main_Server_Sync[] mainServerSyncs = Main_Server_Sync.values();
                sb.append("详细说明：");
                for (Main_Server_Sync mainServerSync : mainServerSyncs) {
                    sb.append(System.lineSeparator()).append(mainServerSync.getCode()).append(" ：").append(mainServerSync.getValue());
                }
                break;
            case status:
                Status[] statuses = Status.values();
                sb.append("详细说明：");
                for (Status status : statuses) {
                    sb.append(System.lineSeparator()).append(status.getCode()).append(" ：").append(status.getValue());
                }
                break;
            case para_type:
                ParamType[] paramTypes = ParamType.values();
                sb.append("详细说明：");
                for (ParamType paramType : paramTypes) {
                    sb.append(System.lineSeparator()).append(paramType.getCode()).append(" ：").append(paramType.getValue());
                }
                break;
            case job_datasource:
                ETLDataSource[] etlDataSources = ETLDataSource.values();
                sb.append("详细说明：");
                for (ETLDataSource etlDataSource : etlDataSources) {
                    sb.append(System.lineSeparator()).append(etlDataSource.getCode()).append(" ：").append(etlDataSource.getValue());
                }
                break;
            default:
                sb.append("详细说明：编辑时请注意单元格格式，比如单元格内容为时间请设置单元格格式为文本类型");
        }
        return sb.toString();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<Map<String, Object>> getTableInfo(Long etl_sys_id, String tableName) {
        if (EtlPara.TableName.equalsIgnoreCase(tableName)) {
            return Dbo.queryList("select * from " + tableName + " where etl_sys_id in(?,?)", etl_sys_id, DefaultEtlSysId);
        } else {
            return Dbo.queryList("select * from " + tableName + " where etl_sys_id = ?", etl_sys_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_dependency", desc = "", range = "", isBean = true)
    private void isPinLvDependency(EtlDependency etl_dependency) {
        Map<String, Object> etlJobDef = EtlJobUtil.getEtlJobByJob(etl_dependency.getEtl_sys_id(), etl_dependency.getEtl_job_id(), Dbo.db());
        if (etlJobDef.isEmpty()) {
            throw new BusinessException("配置依赖错误,作业: " + etl_dependency.getEtl_job_id() + " 不存在!");
        } else {
            if (Dispatch_Frequency.PinLv == Dispatch_Frequency.ofEnumByCode(etlJobDef.get("disp_freq").toString())) {
                throw new BusinessException("频率作业不能配置依赖关系：" + etl_dependency.getEtl_job_id());
            }
        }
        Map<String, Object> preEtlJobDef = EtlJobUtil.getEtlJobByJob(etl_dependency.getEtl_sys_id(), etl_dependency.getPre_etl_job_id(), Dbo.db());
        if (preEtlJobDef.isEmpty()) {
            throw new BusinessException("配置依赖错误,作业: " + etl_dependency.getPre_etl_job_id() + " 不存在!");
        } else {
            if (Dispatch_Frequency.PinLv == Dispatch_Frequency.ofEnumByCode(preEtlJobDef.get("disp_freq").toString())) {
                throw new BusinessException("频率作业不能配置依赖关系：" + etl_dependency.getPre_etl_job_id());
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_temp_id", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<String, Object> searchEtlJobTemplateById(long etl_temp_id) {
        Map<String, Object> etlJobTemp = Dbo.queryOneObject("select * from " + EtlJobTemp.TableName + " where etl_temp_id=?", etl_temp_id);
        if (etlJobTemp.isEmpty()) {
            throw new BusinessException("通过模板ID没有获取到获取模板信息！");
        }
        return etlJobTemp;
    }

    @Param(name = "etl_resource", desc = "", range = "", isBean = true)
    private void checkEtlResourceField(EtlResource etl_resource) {
        if (etl_resource.getEtl_sys_id() == null) {
            throw new BusinessException("工程编号不能为空！");
        }
        Validator.notBlank(etl_resource.getResource_type(), "资源类型不能为空！");
        Validator.notNull(etl_resource.getResource_max(), "资源阈值不能为空！");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_dependency", desc = "", range = "", isBean = true)
    private void checkEtlDependencyField(EtlDependency etl_dependency, Long userId) {
        if (etl_dependency.getEtl_sys_id() == null) {
            throw new BusinessException("系统主键不能为空！");
        }
        if (etl_dependency.getPre_etl_sys_id() == null) {
            throw new BusinessException("上游系统主键不能为空！");
        }
        if (etl_dependency.getEtl_job_id() == null) {
            throw new BusinessException("作业主键不能为空！");
        }
        if (etl_dependency.getPre_etl_job_id() == null) {
            throw new BusinessException("上游作业主键不能为空！");
        }
        Validator.notBlank(etl_dependency.getStatus(), "状态不能为空！");
        Status.ofEnumByCode(etl_dependency.getStatus());
        if (EtlJobUtil.isEtlSysExistById(etl_dependency.getPre_etl_sys_id(), userId, Dbo.db())) {
            throw new BusinessException("上游工程不存在！");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_job_def", desc = "", range = "", isBean = true)
    private void checkEtlJobDefField(EtlJobDef etl_job_def) {
        if (etl_job_def.getEtl_sys_id() == null) {
            throw new BusinessException("工程编号不能为空以及不能为空格");
        }
        Validator.notBlank(etl_job_def.getEtl_job(), "作业名称不能为空以及不能为空格！");
        if (etl_job_def.getSub_sys_id() == null) {
            throw new BusinessException("任务编号不能为空以及不能为空格");
        }
        if (!EtlJobUtil.isEtlSubSysExist(etl_job_def.getEtl_sys_id(), etl_job_def.getSub_sys_id(), Dbo.db())) {
            throw new BusinessException("任务编号不存在！" + etl_job_def.getSub_sys_id());
        }
        Pro_Type.ofEnumByCode(etl_job_def.getPro_type());
        Dispatch_Frequency.ofEnumByCode(etl_job_def.getDisp_freq());
        if (Dispatch_Frequency.ofEnumByCode(etl_job_def.getDisp_freq()) != Dispatch_Frequency.PinLv) {
            Dispatch_Type.ofEnumByCode(etl_job_def.getDisp_type());
        }
        Job_Effective_Flag.ofEnumByCode(etl_job_def.getJob_eff_flag());
        if (StringUtil.isNotBlank(etl_job_def.getToday_disp())) {
            Today_Dispatch_Flag.ofEnumByCode(etl_job_def.getToday_disp());
        }
        Validator.notBlank(etl_job_def.getPro_name(), "作业程序名称不能为空或空格！");
        Validator.notBlank(etl_job_def.getPro_dic(), "验证作业程序目录不能为空或空格！");
        Validator.notBlank(etl_job_def.getLog_dic(), "日志目录不能为空或空格！");
        Validator.notBlank(etl_job_def.getEtl_job_desc(), "作业描述不能为空或空格！");
        ETLDataSource.ofEnumByCode(etl_job_def.getJob_datasource());
        if (StringUtil.isNotBlank(etl_job_def.getDisp_time()) && !RegexConstant.matcher(RegexConstant.TIME_FORMAT, etl_job_def.getDisp_time())) {
            throw new BusinessException(etl_job_def.getDisp_time() + "调度时间格式不正确：HH:mm:ss");
        }
        if (StringUtil.isNotBlank(etl_job_def.getStar_time()) && !DateUtil.validDateStr(etl_job_def.getStar_time(), DateUtil.DATETIME_DEFAULT)) {
            throw new BusinessException(etl_job_def.getStar_time() + "开始日期格式不正确：yyyyMMdd HHmmss");
        }
        if (StringUtil.isNotBlank(etl_job_def.getEnd_time()) && !DateUtil.validDateStr(etl_job_def.getEnd_time(), DateUtil.DATETIME_DEFAULT)) {
            throw new BusinessException(etl_job_def.getEnd_time() + "结束日期格式不正确：yyyyMMdd HHmmss");
        }
        if (StringUtil.isNotBlank(etl_job_def.getUpd_time()) && !DateUtil.validDateStr(etl_job_def.getUpd_time(), "yyyy-MM-dd HH:mm:ss")) {
            throw new BusinessException(etl_job_def.getUpd_time() + "更新时间格式不正确：yyyy-MM-dd HH:mm:ss");
        }
        if (StringUtil.isNotBlank(etl_job_def.getLast_exe_time())) {
            if (!DateUtil.validDateStr(etl_job_def.getLast_exe_time(), DateUtil.DATETIME_DEFAULT)) {
                throw new BusinessException(etl_job_def.getLast_exe_time() + "最后执行时间格式不正确：yyyyMMdd HHmmss");
            }
        }
        if (StringUtil.isNotBlank(etl_job_def.getCurr_bath_date()) && !DateUtil.validDateStr(etl_job_def.getCurr_bath_date(), DateUtil.DATE_DEFAULT)) {
            throw new BusinessException(etl_job_def.getCurr_bath_date() + "跑批日期格式不正确：yyyyMMdd");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_job_def", desc = "", range = "", isBean = true)
    @Param(name = "etl_dependency", desc = "", range = "", isBean = true)
    @Param(name = "old_pre_etl_job", desc = "", range = "", nullable = true)
    @Param(name = "pre_etl_job", desc = "", range = "", nullable = true)
    private void updateDependencyFromEtlJobDef(EtlDependency etl_dependency, Long[] old_pre_etl_job, Long[] pre_etl_job_id) {
        if (old_pre_etl_job != null && old_pre_etl_job.length != 0) {
            deleteOldDependency(etl_dependency, old_pre_etl_job);
        }
        saveEtlDependencyFromEtlJobDef(etl_dependency, pre_etl_job_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_dependency", desc = "", range = "", isBean = true)
    @Param(name = "old_pre_etl_job", desc = "", range = "", nullable = true)
    private void deleteOldDependency(EtlDependency etl_dependency, Long[] old_pre_etl_job) {
        for (Long oldPreEtlJob : old_pre_etl_job) {
            if (!EtlJobUtil.isEtlJobDefExist(etl_dependency.getEtl_sys_id(), oldPreEtlJob, Dbo.db())) {
                throw new BusinessException("修改前的上游作业名称已不存在，pre_etl_job=" + oldPreEtlJob);
            }
            if (etl_dependency.getPre_etl_sys_id() == null) {
                etl_dependency.setPre_etl_sys_id(etl_dependency.getEtl_sys_id());
            }
            deleteEtlDependency(etl_dependency.getEtl_sys_id(), etl_dependency.getPre_etl_sys_id(), etl_dependency.getEtl_job_id(), oldPreEtlJob);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_dependency", desc = "", range = "", isBean = true)
    @Param(name = "pre_etl_job_id", desc = "", range = "", nullable = true)
    private void saveEtlDependencyFromEtlJobDef(EtlDependency etl_dependency, Long[] pre_etl_job_id) {
        if (pre_etl_job_id != null && pre_etl_job_id.length != 0) {
            for (Long preEtlJobId : pre_etl_job_id) {
                if (etl_dependency.getEtl_job_id().equals(preEtlJobId)) {
                    continue;
                }
                if (!EtlJobUtil.isEtlJobDefExist(etl_dependency.getEtl_sys_id(), preEtlJobId, Dbo.db())) {
                    throw new BusinessException("修改后的上游作业名称已不存在!");
                }
                etl_dependency.setPre_etl_job_id(preEtlJobId);
                if (null == etl_dependency.getPre_etl_sys_id()) {
                    etl_dependency.setPre_etl_sys_id(etl_dependency.getEtl_sys_id());
                }
                if (!EtlJobUtil.isEtlDependencyExist(etl_dependency.getEtl_sys_id(), etl_dependency.getPre_etl_sys_id(), etl_dependency.getEtl_job_id(), etl_dependency.getPre_etl_job_id(), Dbo.db())) {
                    etl_dependency.add(Dbo.db());
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Param(name = "pro_type", desc = "", range = "")
    private void isThriftOrYarnProType(Long etl_sys_id, Long etl_job_id, String pro_type) {
        if (Pro_Type.Thrift == Pro_Type.ofEnumByCode(pro_type) || Pro_Type.Yarn == Pro_Type.ofEnumByCode(pro_type)) {
            saveEtlJobResource(etl_sys_id, etl_job_id, pro_type);
        } else {
            List<String> proTypeList = Dbo.queryOneColumnList("select pro_type from etl_job_def where " + " etl_sys_id=? and etl_job_id=?", etl_sys_id, etl_job_id);
            if (!proTypeList.isEmpty()) {
                if (Pro_Type.Thrift == Pro_Type.ofEnumByCode(proTypeList.get(0)) || Pro_Type.Yarn == Pro_Type.ofEnumByCode(proTypeList.get(0))) {
                    DboExecute.deletesOrThrow("当作业程序类型由thrift或yarn更改为其他类型时需删除新增时" + "分配的资源，删除资源失败！", "delete from " + EtlJobResourceRela.TableName + " where etl_sys_id=? AND etl_job_id=? and resource_type in (?,?)", etl_sys_id, etl_job_id, Pro_Type.Thrift.getValue(), Pro_Type.Yarn.getValue());
                }
            }
            saveEtlJobResource(etl_sys_id, etl_job_id, Constant.NORMAL_DEFAULT_RESOURCE_TYPE);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Param(name = "pro_type", desc = "", range = "")
    public void saveEtlJobResource(Long etl_sys_id, Long etl_job_id, String pro_type) {
        EtlJobResourceRela etlJobResourceRela = new EtlJobResourceRela();
        etlJobResourceRela.setEtl_sys_id(etl_sys_id);
        etlJobResourceRela.setEtl_job_id(etl_job_id);
        etlJobResourceRela.setResource_type(pro_type);
        etlJobResourceRela.setResource_req(1);
        if (!EtlJobUtil.isEtlJobResourceRelaExist(etl_sys_id, etl_job_id, Dbo.db())) {
            saveEtlJobResourceRela(etlJobResourceRela);
        } else {
            updateEtlJobResourceRela(etlJobResourceRela);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_job_def", desc = "", range = "", isBean = true)
    private void isDispatchFrequency(EtlJobDef etl_job_def) {
        if (Dispatch_Frequency.PinLv == Dispatch_Frequency.ofEnumByCode(etl_job_def.getDisp_freq())) {
            etl_job_def.setDisp_offset(0);
            etl_job_def.setDisp_time("");
            etl_job_def.setJob_priority(0);
            etl_job_def.setCom_exe_num(0);
            etl_job_def.setLast_exe_time(DateUtil.getDateTime());
            Integer exe_num = etl_job_def.getExe_num();
            if (exe_num == null) {
                etl_job_def.setExe_num(Integer.MAX_VALUE);
            }
        } else {
            etl_job_def.setExe_frequency(0);
            etl_job_def.setExe_num(0);
            etl_job_def.setCom_exe_num(0);
            etl_job_def.setStar_time("");
            etl_job_def.setEnd_time("");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "jobResourceRelation", desc = "", range = "", isBean = true)
    private void checkEtlJobResourceRelaField(EtlJobResourceRela jobResourceRelation) {
        if (jobResourceRelation.getEtl_sys_id() == null) {
            throw new BusinessException("系统工程主键不能为空");
        }
        Validator.notBlank(jobResourceRelation.getResource_type(), "资源类型不能为空！");
        if (jobResourceRelation.getEtl_job_id() == null) {
            throw new BusinessException("作业ID不能为空!");
        }
        Validator.notNull(jobResourceRelation.getResource_req(), "资源需求数不能为空！");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_para", desc = "", range = "", isBean = true)
    private void checkEtlParaField(EtlPara etl_para) {
        if (etl_para.getEtl_sys_id() == null) {
            throw new BusinessException("etl_sys_id工程主键不能为空以及空格！");
        }
        Validator.notBlank(etl_para.getPara_cd(), "para_cd变量名称不能为空以及空格！");
        Validator.notBlank(etl_para.getPara_val(), "para_val变量值不能为空以及空格！");
        ParamType.ofEnumByCode(etl_para.getPara_type());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sub_sys_list", desc = "", range = "", isBean = true)
    private void checkEtlSubSysField(EtlSubSysList etl_sub_sys_list) {
        if (etl_sub_sys_list.getEtl_sys_id() == null) {
            throw new BusinessException("工程编号不能为空以及不能为空格");
        }
        Validator.notBlank(etl_sub_sys_list.getSub_sys_cd(), "任务编号不能为空以及不能为空格");
        Validator.notBlank(etl_sub_sys_list.getSub_sys_desc(), "任务名称不能为空以及不能为空格");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "sub_sys_id", desc = "", range = "")
    public void batchDeleteEtlSubSys(Long etl_sys_id, Long[] sub_sys_ids) {
        for (Long subSysId : sub_sys_ids) {
            EtlJobUtil.isEtlJobDefExistUnderEtlSubSys(etl_sys_id, subSysId, Dbo.db());
            DboExecute.deletesOrThrow("删除任务失败，etl_sys_id=" + etl_sys_id + ",sub_sys_id=" + subSysId, "delete from " + EtlSubSysList.TableName + " where etl_sys_id=? " + " and sub_sys_id=?", etl_sys_id, subSysId);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "resource_type", desc = "", range = "")
    public void batchDeleteEtlResource(Long etl_sys_id, String resource_type) {
        String[] resourceTypes = getStrings(resource_type);
        for (String resourceType : resourceTypes) {
            if (EtlJobUtil.isEtlJobResourceRelaExistByType(etl_sys_id, resourceType, Dbo.db())) {
                logger.info(resourceType + "资源下已经分配了作业资源不能删除");
                continue;
            }
            DboExecute.deletesOrThrow("删除删除作业资源定义信息失败，etl_sys_cd=" + etl_sys_id + ",resource_type=" + resourceType, "delete from " + EtlResource.TableName + " where etl_sys_id = ? AND resource_type = ?", etl_sys_id, resourceType);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "etl_job", desc = "", range = "")
    public void batchDeleteEtlJobResourceRela(Long etl_sys_id, String etl_job_ids) {
        Long[] etlJobIds = getLongs(etl_job_ids);
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.clean();
        assembler.addSql("delete from " + EtlJobResourceRela.TableName + " where etl_sys_id =? ").addParam(etl_sys_id);
        assembler.addORParam("etl_job_id", etlJobIds);
        DboExecute.deletesOrThrow(etlJobIds.length, "删除资源分配信息失败", assembler.sql(), assembler.params());
    }

    @Method(desc = "", logicStep = "")
    public void batchDeleteEtlPara(BatchDeleteEtlParaDTO dto) {
        List<String> paraCds = dto.getPara_cd();
        for (String paraCd : paraCds) {
            DboExecute.deletesOrThrow("删除作业系统参数失败，etl_sys_id=" + dto.getEtl_sys_id() + ",para_cd=" + paraCd, "delete from " + EtlPara.TableName + " where etl_sys_id = ? AND para_cd = ?", dto.getEtl_sys_id(), paraCd);
        }
    }

    private Long[] getLongs(String param) {
        Validator.notBlank(param);
        if (param != null) {
            return fd.ng.core.utils.JsonUtil.toObject(param, new TypeReference<Long[]>() {
            });
        }
        return null;
    }

    private String[] getStrings(String param) {
        Validator.notBlank(param);
        return fd.ng.core.utils.JsonUtil.toObject(param, new TypeReference<String[]>() {
        });
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "etl_job", desc = "", range = "")
    public void batchDeleteEtlJobDef(Long etl_sys_id, Long[] etl_job_ids) {
        for (Long etlJob : etl_job_ids) {
            deleteJobResourceRelationIfExist(etl_sys_id, etlJob);
            deleteJobDependencyIfExist(etl_sys_id, etlJob);
            Dbo.execute("delete from " + TakeRelationEtl.TableName + " where etl_sys_id=? and etl_job_id=?", etl_sys_id, etlJob);
            deleteEtlJobDispHisIfExist(etl_sys_id, etlJob);
            deleteEtlJobCurIfExist(etl_sys_id, etlJob);
            deleteEtlJobHandIfExist(etl_sys_id, etlJob);
            deleteEtlJobHandHisIfExixt(etl_sys_id, etlJob);
            deleteEtlJobCpidIfExist(etl_sys_id, etlJob);
        }
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.clean();
        assembler.addSql("delete from " + EtlJobDef.TableName + " where etl_sys_id=? ").addParam(etl_sys_id);
        assembler.addORParam("etl_job_id", etl_job_ids);
        DboExecute.deletesOrThrow(etl_job_ids.length, "删除作业信息失败", assembler.sql(), assembler.params());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileName", desc = "", range = "")
    public void downloadFile(String fileName) {
        String downloadPath = WebinfoProperties.FileUpload_SavedDirName + File.separator + fileName;
        FileDownloadUtil.downloadFile(downloadPath);
        try {
            FileUtil.forceDelete(new File(downloadPath));
        } catch (IOException e) {
            logger.error(e);
            throw new BusinessException("删除文件失败！" + e.getMessage());
        }
    }

    public void uploadExcelFile(MultipartFile file, String table_name) {
        Workbook workBook = null;
        try {
            if (file == null) {
                throw new BusinessException("上传文件不存在！");
            }
            String originalFilename = file.getOriginalFilename();
            String uploadExcelPath = WebinfoProperties.FileUpload_SavedDirName + File.separator + originalFilename;
            File destFile = new File(uploadExcelPath);
            file.transferTo(new File(uploadExcelPath).toPath());
            workBook = ExcelUtil.getWorkbookFromExcel(destFile);
            int numberOfSheets = Objects.requireNonNull(workBook).getNumberOfSheets();
            List<Map<String, Object>> listMap = new ArrayList<>();
            for (int sheetNum = 0; sheetNum < numberOfSheets; sheetNum++) {
                Sheet sheet = workBook.getSheetAt(sheetNum);
                Row row;
                Object cellVal;
                List<String> columnList = new ArrayList<>();
                int physicalNumberOfRows = sheet.getPhysicalNumberOfRows();
                for (int i = sheet.getFirstRowNum(); i < physicalNumberOfRows; i++) {
                    row = sheet.getRow(i);
                    Map<String, Object> map = new HashMap<>();
                    int physicalNumberOfCells = sheet.getRow(0).getPhysicalNumberOfCells();
                    for (int j = sheet.getRow(0).getFirstCellNum(); j < physicalNumberOfCells; j++) {
                        if (j == -1) {
                            continue;
                        }
                        Cell cell = row.getCell(j);
                        if (null == cell) {
                            cellVal = "";
                        } else {
                            cellVal = ExcelUtil.getValue(cell);
                        }
                        if (i == 0) {
                            List<String> cellValList = StringUtil.split(cellVal.toString(), Constant.LXKH);
                            columnList.add(cellValList.get(0));
                        } else {
                            if (physicalNumberOfCells > columnList.size()) {
                                throw new BusinessException("excel表格格式有问题，表头单元格个数大于等于与表身有效单元格个数");
                            }
                            map.put(columnList.get(j).trim(), cellVal);
                        }
                    }
                    if (!map.isEmpty()) {
                        listMap.add(map);
                    }
                }
            }
            insertData(listMap, table_name);
            FileUtil.forceDelete(destFile);
        } catch (FileNotFoundException e) {
            throw new BusinessException("导入excel文件数据失败！");
        } catch (IOException e) {
            throw new BusinessException("获取excel对象失败，文件类型错误");
        } finally {
            try {
                if (workBook != null) {
                    workBook.close();
                }
            } catch (IOException e) {
                logger.error(e);
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "listMap", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    private void insertData(List<Map<String, Object>> listMap, String tableName) {
        for (int i = 0; i < listMap.size(); i++) {
            Map<String, Object> mapInfo = listMap.get(i);
            if (mapInfo != null && !mapInfo.isEmpty()) {
                switch(tableName.toLowerCase()) {
                    case EtlJobDef.TableName:
                        EtlJobDef etl_job_def = parseJsonToEtlJobDef(mapInfo);
                        if (!EtlJobUtil.isEtlSysExist(etl_job_def.getEtl_sys_id(), Dbo.db())) {
                            throw new BusinessException("第" + (i + 1) + "行" + etl_job_def.getEtl_sys_id() + "工程编号不存在！！！");
                        }
                        checkEtlJobDefField(etl_job_def);
                        if (!EtlJobUtil.isEtlJobDefExist(etl_job_def.getEtl_sys_id(), etl_job_def.getEtl_job_id(), Dbo.db())) {
                            etl_job_def.add(Dbo.db());
                        } else {
                            try {
                                etl_job_def.update(Dbo.db());
                            } catch (Exception e) {
                                if (!(e instanceof ProEntity.EntityDealZeroException)) {
                                    throw new BusinessException("第" + (i + 1) + "行，更新失败：" + e.getMessage());
                                }
                            }
                        }
                        break;
                    case EtlSubSysList.TableName:
                        EtlSubSysList etl_sub_sys_list = parseJsonToEtlSubSysList(mapInfo);
                        if (!EtlJobUtil.isEtlSysExist(etl_sub_sys_list.getEtl_sys_id(), Dbo.db())) {
                            throw new BusinessException("第" + (i + 1) + "行" + etl_sub_sys_list.getEtl_sys_id() + "工程编号不存在！！！");
                        }
                        checkEtlSubSysField(etl_sub_sys_list);
                        if (!isEtlSubSysExist(etl_sub_sys_list.getEtl_sys_id(), etl_sub_sys_list.getSub_sys_cd(), Dbo.db())) {
                            etl_sub_sys_list.add(Dbo.db());
                        } else {
                            try {
                                etl_sub_sys_list.update(Dbo.db());
                            } catch (Exception e) {
                                throw new BusinessException("第" + (i + 1) + "行，更新失败：" + e.getMessage());
                            }
                        }
                        break;
                    case EtlResource.TableName:
                        EtlResource etl_resource = parseJsonToEtl(mapInfo);
                        if (!EtlJobUtil.isEtlSysExist(etl_resource.getEtl_sys_id(), Dbo.db())) {
                            throw new BusinessException("第" + (i + 1) + "行" + etl_resource.getEtl_sys_id() + "工程编号不存在！！！");
                        }
                        checkEtlResourceField(etl_resource);
                        if (!EtlJobUtil.isEtlResourceExist(etl_resource.getEtl_sys_id(), etl_resource.getResource_type(), Dbo.db())) {
                            etl_resource.add(Dbo.db());
                        } else {
                            try {
                                etl_resource.update(Dbo.db());
                            } catch (Exception e) {
                                if (!(e instanceof ProEntity.EntityDealZeroException)) {
                                    throw new BusinessException("第" + (i + 1) + "行，更新失败：" + e.getMessage());
                                }
                            }
                        }
                        break;
                    case EtlJobResourceRela.TableName:
                        EtlJobResourceRela etl_job_resource_rela = parseJsonToEtlJobResourceRela(mapInfo);
                        if (!EtlJobUtil.isEtlSysExist(etl_job_resource_rela.getEtl_sys_id(), Dbo.db())) {
                            throw new BusinessException("第" + (i + 1) + "行" + etl_job_resource_rela.getEtl_sys_id() + "工程编号不存在！！！");
                        }
                        checkEtlJobResourceRelaField(etl_job_resource_rela);
                        if (!EtlJobUtil.isEtlJobResourceRelaExist(etl_job_resource_rela.getEtl_sys_id(), etl_job_resource_rela.getEtl_job_id(), Dbo.db())) {
                            etl_job_resource_rela.add(Dbo.db());
                        } else {
                            try {
                                etl_job_resource_rela.update(Dbo.db());
                            } catch (Exception e) {
                                if (!(e instanceof ProEntity.EntityDealZeroException)) {
                                    throw new BusinessException("第" + (i + 1) + "行，更新失败：" + e.getMessage());
                                }
                            }
                        }
                        break;
                    case EtlPara.TableName:
                        EtlPara etl_para = parseJsonToEtlPara(mapInfo);
                        List<String> paraCdList = Dbo.queryOneColumnList("select para_cd from " + EtlPara.TableName + " where etl_sys_id=?", DefaultEtlSysId);
                        if (!EtlJobUtil.isEtlSysExist(etl_para.getEtl_sys_id(), Dbo.db())) {
                            throw new BusinessException("第" + (i + 1) + "行" + etl_para.getEtl_sys_id() + "工程编号不存在！！！");
                        }
                        checkEtlParaField(etl_para);
                        if (!EtlJobUtil.isEtlParaExist(etl_para.getEtl_sys_id(), etl_para.getPara_cd(), Dbo.db()) && !paraCdList.contains(etl_para.getPara_cd())) {
                            etl_para.add(Dbo.db());
                        } else {
                            try {
                                etl_para.update(Dbo.db());
                            } catch (Exception e) {
                                if (!(e instanceof ProEntity.EntityDealZeroException)) {
                                    throw new BusinessException("第" + (i + 1) + "行，更新失败：" + e.getMessage());
                                }
                            }
                        }
                        break;
                    case EtlDependency.TableName:
                        EtlDependency etl_dependency = parseJsonToEtlDependency(mapInfo);
                        if (!EtlJobUtil.isEtlSysExist(etl_dependency.getEtl_sys_id(), Dbo.db())) {
                            throw new BusinessException("第" + (i + 1) + "行" + etl_dependency.getEtl_sys_id() + "工程编号不存在！！！");
                        }
                        checkEtlDependencyField(etl_dependency, UserUtil.getUserId());
                        isPinLvDependency(etl_dependency);
                        if (!EtlJobUtil.isEtlDependencyExist(etl_dependency.getEtl_sys_id(), etl_dependency.getPre_etl_sys_id(), etl_dependency.getEtl_job_id(), etl_dependency.getPre_etl_job_id(), Dbo.db())) {
                            etl_dependency.add(Dbo.db());
                        } else {
                            try {
                                etl_dependency.update(Dbo.db());
                            } catch (Exception e) {
                                if (!(e instanceof ProEntity.EntityDealZeroException)) {
                                    throw new BusinessException("第" + (i + 1) + "行，更新失败：" + e.getMessage());
                                }
                            }
                        }
                        break;
                    default:
                        throw new BusinessException("导入的数据不知道是什么表的信息，表名：" + tableName);
                }
            }
        }
    }

    private EtlDependency parseJsonToEtlDependency(Map<String, Object> mapInfo) {
        EtlDependency etlDependency = new EtlDependency();
        etlDependency.setEtl_sys_id(getValue(mapInfo, "etl_sys_id", Long.class, 0L));
        etlDependency.setEtl_job_id(getValue(mapInfo, "etl_job_id", Long.class, 0L));
        etlDependency.setPre_etl_sys_id(getValue(mapInfo, "pre_etl_sys_id", Long.class, 0L));
        etlDependency.setPre_etl_job_id(getValue(mapInfo, "pre_etl_job_id", Long.class, 0L));
        etlDependency.setStatus(getValue(mapInfo, "status", String.class, ""));
        etlDependency.setMain_serv_sync(getValue(mapInfo, "main_serv_sync", String.class, ""));
        return etlDependency;
    }

    private EtlPara parseJsonToEtlPara(Map<String, Object> mapInfo) {
        EtlPara etlPara = new EtlPara();
        etlPara.setEtl_sys_id(getValue(mapInfo, "etl_sys_id", Long.class, 0L));
        etlPara.setPara_cd(getValue(mapInfo, "para_cd", String.class, ""));
        etlPara.setPara_val(getValue(mapInfo, "para_val", String.class, ""));
        etlPara.setPara_type(getValue(mapInfo, "para_type", String.class, ""));
        etlPara.setPara_desc(getValue(mapInfo, "para_desc", String.class, ""));
        return etlPara;
    }

    private EtlSubSysList parseJsonToEtlSubSysList(Map<String, Object> mapInfo) {
        EtlSubSysList etlSubSysList = new EtlSubSysList();
        etlSubSysList.setSub_sys_id(getValue(mapInfo, "sub_sys_id", Long.class, 0L));
        etlSubSysList.setEtl_sys_id(getValue(mapInfo, "etl_sys_id", Long.class, 0L));
        etlSubSysList.setSub_sys_cd(getValue(mapInfo, "sub_sys_cd", String.class, ""));
        etlSubSysList.setSub_sys_desc(getValue(mapInfo, "sub_sys_desc", String.class, ""));
        etlSubSysList.setComments(getValue(mapInfo, "comments", String.class, ""));
        return etlSubSysList;
    }

    private EtlJobDef parseJsonToEtlJobDef(Map<String, Object> mapInfo) {
        EtlJobDef etlJobDef = new EtlJobDef();
        etlJobDef.setEtl_job(getValue(mapInfo, "etl_job", String.class, ""));
        etlJobDef.setEtl_job_id(getValue(mapInfo, "etl_job_id", Long.class, 0L));
        etlJobDef.setEtl_sys_id(getValue(mapInfo, "etl_sys_id", Long.class, 0L));
        etlJobDef.setSub_sys_id(getValue(mapInfo, "sub_sys_id", Long.class, 0L));
        etlJobDef.setEtl_job_desc(getValue(mapInfo, "etl_job_desc", String.class, ""));
        etlJobDef.setPro_type(getValue(mapInfo, "pro_type", String.class, ""));
        etlJobDef.setPro_dic(getValue(mapInfo, "pro_dic", String.class, ""));
        etlJobDef.setPro_name(getValue(mapInfo, "pro_name", String.class, ""));
        etlJobDef.setPro_para(getValue(mapInfo, "pro_para", String.class, ""));
        etlJobDef.setLog_dic(getValue(mapInfo, "log_dic", String.class, ""));
        etlJobDef.setDisp_freq(getValue(mapInfo, "disp_freq", String.class, ""));
        etlJobDef.setDisp_offset(getValue(mapInfo, "disp_offset", Long.class, 0L).intValue());
        etlJobDef.setDisp_type(getValue(mapInfo, "disp_type", String.class, ""));
        etlJobDef.setDisp_time(getValue(mapInfo, "disp_time", String.class, ""));
        etlJobDef.setJob_eff_flag(getValue(mapInfo, "job_eff_flag", String.class, ""));
        etlJobDef.setJob_priority(getValue(mapInfo, "job_priority", Long.class, 0L).intValue());
        etlJobDef.setJob_disp_status(getValue(mapInfo, "job_disp_status", String.class, ""));
        etlJobDef.setCurr_st_time(getValue(mapInfo, "curr_st_time", String.class, ""));
        etlJobDef.setCurr_end_time(getValue(mapInfo, "curr_end_time", String.class, ""));
        etlJobDef.setOverlength_val(getValue(mapInfo, "overlength_val", Long.class, 0L).intValue());
        etlJobDef.setOvertime_val(getValue(mapInfo, "overtime_val", Long.class, 0L).intValue());
        etlJobDef.setCurr_bath_date(getValue(mapInfo, "curr_bath_date", String.class, ""));
        etlJobDef.setComments(getValue(mapInfo, "comments", String.class, ""));
        etlJobDef.setToday_disp(getValue(mapInfo, "today_disp", String.class, ""));
        etlJobDef.setMain_serv_sync(getValue(mapInfo, "main_serv_sync", String.class, ""));
        etlJobDef.setJob_process_id(getValue(mapInfo, "job_process_id", String.class, ""));
        etlJobDef.setJob_priority_curr(getValue(mapInfo, "job_priority_curr", Long.class, 0L).intValue());
        etlJobDef.setJob_return_val(getValue(mapInfo, "job_return_val", Long.class, 0L).intValue());
        etlJobDef.setUpd_time(getValue(mapInfo, "upd_time", String.class, ""));
        etlJobDef.setExe_frequency(getValue(mapInfo, "exe_frequency", Long.class, 0L).intValue());
        etlJobDef.setExe_num(getValue(mapInfo, "exe_num", Long.class, 0L).intValue());
        etlJobDef.setCom_exe_num(getValue(mapInfo, "com_exe_num", Long.class, 0L).intValue());
        etlJobDef.setLast_exe_time(getValue(mapInfo, "last_exe_time", String.class, ""));
        etlJobDef.setStar_time(getValue(mapInfo, "star_time", String.class, ""));
        etlJobDef.setEnd_time(getValue(mapInfo, "end_time", String.class, ""));
        etlJobDef.setSuccess_job(getValue(mapInfo, "success_job", String.class, ""));
        etlJobDef.setFail_job(getValue(mapInfo, "fail_job", String.class, ""));
        etlJobDef.setJob_datasource(getValue(mapInfo, "job_datasource", String.class, ""));
        return etlJobDef;
    }

    private EtlResource parseJsonToEtl(Map<String, Object> mapInfo) {
        EtlResource etlResource = new EtlResource();
        etlResource.setEtl_sys_id(getValue(mapInfo, "etl_sys_id", Long.class, 0L));
        etlResource.setResource_name(getValue(mapInfo, "resource_name", String.class, ""));
        etlResource.setResource_type(getValue(mapInfo, "resource_type", String.class, ""));
        etlResource.setResource_max(getValue(mapInfo, "resource_max", Long.class, 0L).intValue());
        etlResource.setResource_used(getValue(mapInfo, "resource_used", Long.class, 0L).intValue());
        etlResource.setMain_serv_sync(getValue(mapInfo, "main_serv_sync", String.class, ""));
        return etlResource;
    }

    private EtlJobResourceRela parseJsonToEtlJobResourceRela(Map<String, Object> mapInfo) {
        EtlJobResourceRela etlJobResourceRela = new EtlJobResourceRela();
        etlJobResourceRela.setEtl_sys_id(getValue(mapInfo, "etl_sys_id", Long.class, 0L));
        etlJobResourceRela.setEtl_job_id(getValue(mapInfo, "etl_job_id", Long.class, 0l));
        etlJobResourceRela.setResource_type(getValue(mapInfo, "resource_type", String.class, ""));
        etlJobResourceRela.setResource_req(getValue(mapInfo, "resource_req", Long.class, 0L).intValue());
        return etlJobResourceRela;
    }

    public static <K, V> V getValue(Map<K, Object> map, K key, Class<V> clazz, V defaultValue) {
        if (map != null) {
            if (map.get(key) != null && !map.get(key).toString().trim().equals("")) {
                if (clazz.isInstance(map.get(key))) {
                    return clazz.cast(map.get(key));
                } else {
                    String number = map.get(key).toString();
                    return clazz.cast(Long.valueOf(number));
                }
            }
        }
        return defaultValue;
    }
}
