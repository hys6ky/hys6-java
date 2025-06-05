package hyren.serv6.b.datadistribution;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.CodecUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.datadistribution.bean.DistributeJobBean;
import hyren.serv6.b.receive.distribute.DistributeService;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.datatree.WebTreeData;
import hyren.serv6.base.datatree.background.bean.TreeConf;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.datatree.tree.TreePageSource;
import hyren.serv6.base.entity.DataDistribute;
import hyren.serv6.base.entity.EtlDependency;
import hyren.serv6.base.entity.EtlJobDef;
import hyren.serv6.base.entity.TakeRelationEtl;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.utils.fileutil.FileDownloadUtil;
import hyren.serv6.commons.compress.ZipUtils;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.commons.jobUtil.EtlJobUtil;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import hyren.serv6.commons.utils.datatable.DataTableUtil;
import hyren.serv6.commons.utils.xlstoxml.util.ExcelUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import java.io.*;
import java.util.*;

@Slf4j
@DocClass(desc = "", author = "yec", createdate = "2021/10/13")
@Api("数据分发操作类")
@Service
public class DataDistributionService {

    @Method(desc = "", logicStep = "")
    @Param(name = "data_distribute", desc = "", range = "", isBean = true)
    public void saveDistributeData(DataDistribute data_distribute) {
        DistributeService.checkoutParams(data_distribute);
        Long key = PrimayKeyGener.getNextId();
        data_distribute.setDd_id(key);
        data_distribute.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getDistributeData(int currPage, int pageSize) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + DataDistribute.TableName);
        Map<String, Object> distributeMap = new HashMap<>();
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<DataDistribute> dataInfo = Dbo.queryPagedList(DataDistribute.class, page, asmSql.sql());
        distributeMap.put("dataInfo", dataInfo);
        distributeMap.put("totalSize", page.getTotalSize());
        return distributeMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dd_ids", desc = "", range = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getIsReleaseData(Long[] dd_ids, int currPage, int pageSize) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT dd_id,sql_table,is_header,database_code,row_separator,database_separatorr,dbfile_format," + " plane_url,concat (file_name , '_' ,dd_id) AS etl_job,file_suffix,is_upper,is_compress,is_flag,is_release" + " FROM " + DataDistribute.TableName);
        asmSql.addORParam("dd_id", dd_ids);
        Page page = new DefaultPageImpl(currPage, pageSize);
        Map<String, Object> data_distributes = new HashMap<>();
        List<Map<String, Object>> releaseInfo = Dbo.queryPagedList(page, asmSql.sql(), asmSql.params());
        data_distributes.put("releaseInfo", releaseInfo);
        data_distributes.put("totalSize", page.getTotalSize());
        String path = PropertyParaValue.getString("agentpath", "");
        data_distributes.put("sysDir", StringUtil.isBlank(path) ? System.getProperty("user.dir") : new File(path).getParent());
        return data_distributes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dd_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public DataDistribute getDataInfoMsg(long dd_id) {
        return Dbo.queryOneObject(DataDistribute.class, "select * from " + DataDistribute.TableName + " where dd_id = ?", dd_id).orElseThrow(() -> new BusinessException("获取数据信息失败!"));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "data_distribute", desc = "", range = "", isBean = true)
    public void updateDistributeData(DataDistribute data_distribute) {
        int ret = data_distribute.update(Dbo.db());
        if (ret != 1) {
            throw new BusinessException("修改数据分发信息失败!");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dd_ids", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<List<Map<String, Object>>> getJobMsg(Long[] dd_ids) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT concat (file_name , '_' ,dd_id) AS etl_job,dd_id FROM " + DataDistribute.TableName);
        asmSql.addORParam("dd_id", dd_ids);
        List<Map<String, Object>> mapList = Dbo.queryList(asmSql.sql(), asmSql.params());
        List<List<Map<String, Object>>> objMapList = new ArrayList<>();
        for (Map<String, Object> map : mapList) {
            String etl_job = map.get("etl_job").toString();
            Map<String, Object> dd_msg = new HashMap<>();
            List<Map<String, Object>> jobList = Dbo.queryList("select * from " + EtlJobDef.TableName + " where etl_job = ?", etl_job);
            Result result = Dbo.queryResult(" select etl_job from " + EtlJobDef.TableName + "  where etl_job_id in (select pre_etl_job_id from " + EtlJobDef.TableName + " t1 join " + EtlDependency.TableName + " t2 on t1.etl_job_id=t2.etl_job_id where etl_job=? ) ", etl_job);
            if (!result.isEmpty()) {
                Map<String, Object> msgJob = new HashMap<>();
                List<String> msgPre = new ArrayList<>();
                for (int i = 0; i < result.getRowCount(); i++) {
                    msgPre.add(i, result.getString(i, "etl_job"));
                }
                msgJob.put("pre_etl_job", msgPre);
                jobList.add(msgJob);
            }
            dd_msg.put("dd_id", map.get("dd_id").toString());
            jobList.add(dd_msg);
            objMapList.add(jobList);
        }
        return objMapList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileName", desc = "", range = "")
    public void packageJars(String fileName) {
        String distributePath = PropertyParaValue.getString("distributePath", "/data/project/hyshf/hrsapp/dist_6_0/java/b/hyren-serv6-b-6.0.jar");
        File distributeFile = new File(distributePath);
        if (!distributeFile.exists()) {
            throw new BusinessException(String.format("数据分发jar包不存在:%s", distributePath));
        }
        String path = System.getProperty("user.dir");
        File file = new File(path);
        ZipUtils.compress(path + File.separator + fileName, distributePath, path + File.separator + "resources", path + File.separator + "distribution-job.sh", file.getParent() + File.separator + "jre", file.getParent() + File.separator + "jdbc", file.getParent() + File.separator + "jars");
        FileDownloadUtil.downloadFile(path + File.separator + fileName);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileName", desc = "", range = "")
    public void downloadDistributeFile(String fileName) {
        String rootPath = System.getProperty("user.dir");
        FileInputStream in = null;
        try (OutputStream out = ContextDataHolder.getResponse().getOutputStream()) {
            String filePath = rootPath + File.separator + fileName;
            ContextDataHolder.getResponse().reset();
            if (ContextDataHolder.getRequest().getHeader("User-Agent").toLowerCase().indexOf("firefox") > 0) {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + new String(fileName.getBytes(CodecUtil.UTF8_CHARSET), DataBaseCode.ISO_8859_1.getCode()));
            } else {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + Base64.getEncoder().encodeToString(fileName.getBytes(CodecUtil.UTF8_CHARSET)));
            }
            ContextDataHolder.getResponse().setContentType("APPLICATION/OCTET-STREAM");
            in = new FileInputStream(filePath);
            byte[] bytes = new byte[1024];
            int len;
            while ((len = in.read(bytes)) > 0) {
                out.write(bytes, 0, len);
            }
            out.flush();
        } catch (UnsupportedEncodingException e) {
            throw new BusinessException("不支持的编码异常");
        } catch (FileNotFoundException e) {
            throw new BusinessException("文件不存在，可能目录不存在！");
        } catch (IOException e) {
            throw new BusinessException("下载文件失败！");
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    log.error("下载文件关闭流失败", e);
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dd_id", desc = "", range = "")
    public void deleteDistributeData(long dd_id) {
        Result result = Dbo.queryResult("select * from " + DataDistribute.TableName + " where dd_id = ?", dd_id);
        String etl_job = result.getString(0, "file_name") + "_" + dd_id;
        Result jobResult = Dbo.queryResult("select * from " + EtlJobDef.TableName + " where etl_job = ?", etl_job);
        if (!jobResult.isEmpty()) {
            List<Map<String, Object>> etlJobList = jobResult.toList();
            Long etl_job_id = Long.parseLong(etlJobList.get(0).get("etl_job_id").toString());
            DboExecute.deletesOrThrow("删除数据分发作业关系表失败，dd_id=" + dd_id, "delete from " + TakeRelationEtl.TableName + " where take_id = ?", dd_id);
            DboExecute.deletesOrThrow("删除作业表数据失败", "delete from " + EtlJobDef.TableName + " where etl_job = ?", etl_job);
            Result dependencyResult = Dbo.queryResult("select * from " + EtlDependency.TableName + " where etl_job_id = ?", etl_job_id);
            if (!dependencyResult.isEmpty()) {
                int execute = Dbo.execute("delete from " + EtlDependency.TableName + " where etl_job_id = ?", etl_job_id);
                if (execute < 1) {
                    throw new BusinessException("删除依赖作业信息失败!");
                }
            }
        }
        DboExecute.deletesOrThrow("删除数据分发信息失败，dd_id=" + dd_id, "delete from " + DataDistribute.TableName + " where dd_id = ?", dd_id);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "distributeJobBean", value = "", dataTypeClass = DistributeJobBean.class)
    public void saveDistributeDataJobRelation(List<List<String>> pre_etl_job_ids, List<Map<String, String>> dd_ids, List<EtlJobDef> relation) {
        List<List<Long>> preEtJobIdList = new ArrayList<>();
        if (pre_etl_job_ids != null) {
            for (List<String> strings : pre_etl_job_ids) {
                List<Long> ids = new ArrayList<>();
                for (String s : strings) {
                    Map<String, Object> id = Dbo.queryOneObject("select etl_job_id from " + EtlJobDef.TableName + " where etl_job=?", s);
                    if (id.get("etl_job_id") != null) {
                        ids.add(Long.parseLong(id.get("etl_job_id").toString()));
                    }
                }
                preEtJobIdList.add(ids);
            }
        }
        for (EtlJobDef etlJobDef : relation) {
            Validator.notBlank(etlJobDef.getEtl_job(), "作业名不能为空");
            Validator.notBlank(etlJobDef.getDisp_freq(), "ETL调度频率不能为空");
            if (etlJobDef.getEtl_sys_id() == null || etlJobDef.getEtl_sys_id().toString().isEmpty()) {
                etlJobDef.setEtl_sys_id(PrimayKeyGener.getNextId());
            }
            if (etlJobDef.getEtl_job_id() == null || etlJobDef.getEtl_job_id().toString().isEmpty()) {
                etlJobDef.setEtl_job_id(PrimayKeyGener.getNextId());
            }
        }
        EtlJobUtil.saveDistributeDataJob(relation, dd_ids, preEtJobIdList);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file", desc = "", range = "")
    public void uploadExcelFile(MultipartFile file) {
        Workbook workBook = null;
        try {
            String originalFilename = file.getOriginalFilename();
            File uploadedFile = new File(WebinfoProperties.FileUpload_SavedDirName + File.separator + originalFilename);
            file.transferTo(uploadedFile);
            if (!uploadedFile.exists()) {
                throw new BusinessException("上传文件不存在！");
            }
            workBook = ExcelUtil.getWorkbookFromExcel(uploadedFile);
            int numberOfSheets = Objects.requireNonNull(workBook).getNumberOfSheets();
            List<List<String>> dataList = new ArrayList<>();
            for (int sheetNum = 0; sheetNum < numberOfSheets; sheetNum++) {
                Sheet sheet = workBook.getSheetAt(sheetNum);
                Row row;
                String cellVal;
                int physicalNumberOfRows = sheet.getPhysicalNumberOfRows();
                for (int i = sheet.getFirstRowNum() + 1; i < physicalNumberOfRows; i++) {
                    List<String> list = new ArrayList<>();
                    row = sheet.getRow(i);
                    int physicalNumberOfCells = sheet.getRow(0).getPhysicalNumberOfCells();
                    for (int j = sheet.getRow(0).getFirstCellNum(); j < physicalNumberOfCells; j++) {
                        if (j == -1) {
                            continue;
                        }
                        Cell cell = row.getCell(j);
                        if (null == cell) {
                            cellVal = "";
                        } else {
                            cellVal = cell.toString();
                        }
                        if (cellVal.contains(".")) {
                            list.add(cellVal.substring(0, cellVal.lastIndexOf(".")));
                        } else {
                            list.add(cellVal);
                        }
                    }
                    dataList.add(list);
                }
            }
            for (List<String> dataInfo : dataList) {
                DataDistribute distribute = new DataDistribute();
                String sqlOrTb = dataInfo.get(0);
                Validator.notBlank(sqlOrTb, "sql语句或表名不能为空!");
                distribute.setSql_table(sqlOrTb);
                String header = dataInfo.get(1);
                Validator.notBlank(header, "是否为表头信息不能为空!");
                if (IsFlag.Shi == IsFlag.ofEnumByCode(header)) {
                    distribute.setIs_header(IsFlag.Shi.getCode());
                } else if (IsFlag.Fou == IsFlag.ofEnumByCode(header)) {
                    distribute.setIs_header(IsFlag.Fou.getCode());
                } else {
                    throw new BusinessException("是否需要表头信息的值只能为: '0'和'1',请检查输入数据是否正确");
                }
                String code = dataInfo.get(2);
                Validator.notBlank(code, "数据编码格式不能为空!");
                if (DataBaseCode.UTF_8 == DataBaseCode.ofEnumByCode(code)) {
                    distribute.setDatabase_code(DataBaseCode.UTF_8.getCode());
                } else if (DataBaseCode.GBK == DataBaseCode.ofEnumByCode(code)) {
                    distribute.setDatabase_code(DataBaseCode.GBK.getCode());
                } else if (DataBaseCode.GB2312 == DataBaseCode.ofEnumByCode(code)) {
                    distribute.setDatabase_code(DataBaseCode.GB2312.getCode());
                } else if (DataBaseCode.UTF_16 == DataBaseCode.ofEnumByCode(code)) {
                    distribute.setDatabase_code(DataBaseCode.UTF_16.getCode());
                } else if (DataBaseCode.ISO_8859_1 == DataBaseCode.ofEnumByCode(code)) {
                    distribute.setDatabase_code(DataBaseCode.ISO_8859_1.getCode());
                } else {
                    throw new BusinessException("请选择正确的文件编码格式");
                }
                distribute.setRow_separator(dataInfo.get(3));
                distribute.setDatabase_separatorr(dataInfo.get(4));
                String format = dataInfo.get(5);
                if (FileFormat.DingChang == FileFormat.ofEnumByCode(format)) {
                    distribute.setDbfile_format(FileFormat.DingChang.getCode());
                } else if (FileFormat.FeiDingChang == FileFormat.ofEnumByCode(format)) {
                    distribute.setDbfile_format(FileFormat.FeiDingChang.getCode());
                } else if (FileFormat.CSV == FileFormat.ofEnumByCode(format)) {
                    distribute.setDbfile_format(FileFormat.CSV.getCode());
                } else if (FileFormat.SEQUENCEFILE == FileFormat.ofEnumByCode(format)) {
                    distribute.setDbfile_format(FileFormat.SEQUENCEFILE.getCode());
                } else if (FileFormat.PARQUET == FileFormat.ofEnumByCode(format)) {
                    distribute.setDbfile_format(FileFormat.PARQUET.getCode());
                } else if (FileFormat.ORC == FileFormat.ofEnumByCode(format)) {
                    distribute.setDbfile_format(FileFormat.ORC.getCode());
                } else {
                    throw new BusinessException("请选择正确的文件格式");
                }
                distribute.setPlane_url(dataInfo.get(6));
                String file_name = dataInfo.get(7);
                Validator.notBlank(file_name, "文件名称不能为空!");
                distribute.setFile_name(file_name);
                String suffix = dataInfo.get(8);
                Validator.notBlank(suffix, "文件后缀不能为空!");
                distribute.setFile_suffix(suffix);
                String upper = dataInfo.get(9);
                Validator.notBlank(upper, "文件名是否大写信息不能为空!");
                if (IsFlag.Shi == IsFlag.ofEnumByCode(upper)) {
                    distribute.setIs_upper(IsFlag.Shi.getCode());
                } else if (IsFlag.Fou == IsFlag.ofEnumByCode(upper)) {
                    distribute.setIs_upper(IsFlag.Fou.getCode());
                } else {
                    throw new BusinessException("文件名是否大写信息的值只能为: '0'和'1',请检查输入数据是否正确");
                }
                String compress = dataInfo.get(10);
                Validator.notBlank(compress, "文件是否压缩信息不能为空!");
                if (IsFlag.Shi == IsFlag.ofEnumByCode(compress)) {
                    distribute.setIs_compress(IsFlag.Shi.getCode());
                } else if (IsFlag.Fou == IsFlag.ofEnumByCode(compress)) {
                    distribute.setIs_compress(IsFlag.Fou.getCode());
                } else {
                    throw new BusinessException("文件是否压缩信息的值只能为: '0'和'1',请检查输入数据是否正确");
                }
                String flag = dataInfo.get(11);
                Validator.notBlank(flag, "是否标识文件信息不能为空!");
                if (IsFlag.Shi == IsFlag.ofEnumByCode(flag)) {
                    distribute.setIs_flag(IsFlag.Shi.getCode());
                } else if (IsFlag.Fou == IsFlag.ofEnumByCode(flag)) {
                    distribute.setIs_flag(IsFlag.Fou.getCode());
                } else {
                    throw new BusinessException("文件是否被标识信息的值只能为: '0'和'1',请检查输入数据是否正确");
                }
                distribute.setIs_release(IsFlag.Fou.getCode());
                distribute.setDd_remark(dataInfo.get(12));
                distribute.setDd_id(PrimayKeyGener.getNextId());
                distribute.add(Dbo.db());
            }
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
                log.error("导入excel文件关闭流失败！", e);
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "source", desc = "", range = "")
    @Param(name = "id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> queryAllColumnOnTableName(String source, String id) {
        Validator.notBlank(source, "查询数据层为空!");
        Validator.notBlank(id, "查询数据表id为空!");
        return DataTableUtil.getTableInfoAndColumnInfo(source, id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<String> searchEtlJob(long etl_sys_id, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        return Dbo.queryOneColumnList("select etl_job,etl_job_id from " + EtlJobDef.TableName + " where etl_sys_id=?", etl_sys_id);
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<Node> getWebSQLTreeData() {
        TreeConf treeConf = new TreeConf();
        treeConf.setShowFileCollection(Boolean.FALSE);
        treeConf.setIsIntoHBase("");
        return new WebTreeData().getTreeData(TreePageSource.WEB_SQL, treeConf);
    }
}
