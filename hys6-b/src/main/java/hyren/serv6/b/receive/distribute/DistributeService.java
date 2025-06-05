package hyren.serv6.b.receive.distribute;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.FileUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.DataDistribute;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.utils.DruidParseQuerySql;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.compress.ZipUtils;
import hyren.serv6.commons.utils.SqlParamReplace;
import hyren.serv6.commons.utils.constant.CommonVariables;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.springframework.stereotype.Service;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

@Slf4j
@Service
public class DistributeService {

    @ApiOperation(value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "dd_id", value = "", type = "不可为空", dataTypeClass = Long.class), @ApiImplicitParam(name = "curr_bath_date", value = "", type = "不可为空", dataTypeClass = String.class), @ApiImplicitParam(name = "sqlParams", value = "", type = "可为空", dataTypeClass = String.class) })
    public void unloadDistributeData(Long dd_id, String curr_bath_date, String sqlParams) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            DataDistribute data_distribute = SqlOperator.queryOneObject(db, DataDistribute.class, "select * from " + DataDistribute.TableName + " where dd_id = ?", dd_id).orElseThrow(() -> new AppSystemException("获取数据表信息失败!"));
            checkoutParams(data_distribute);
            log.info("sqlParams:{}", sqlParams);
            String rel_sql;
            if (StringUtil.isNotBlank(curr_bath_date)) {
                rel_sql = SqlParamReplace.replaceSqlParam(data_distribute.getSql_table(), sqlParams);
            } else {
                rel_sql = data_distribute.getSql_table();
            }
            log.info("最终执行sql:{}", rel_sql);
            String fileName = data_distribute.getFile_name();
            if (IsFlag.Shi == IsFlag.ofEnumByCode(data_distribute.getIs_upper())) {
                fileName = fileName.toUpperCase();
            }
            String separator = File.separator;
            String filePath = data_distribute.getPlane_url() + separator + curr_bath_date + separator;
            filePath = FilenameUtils.normalize(filePath);
            String suffix = FileUtil.FILE_EXT_CHAR + data_distribute.getFile_suffix();
            File parentFile = new File(filePath);
            if (parentFile.exists()) {
                if (!FileUtil.deleteDirectoryFiles(filePath)) {
                    throw new AppSystemException("删除目录失败:" + parentFile.getAbsolutePath());
                }
            } else {
                if (!parentFile.mkdirs()) {
                    throw new AppSystemException("创建父目录失败:" + parentFile.getAbsolutePath());
                }
            }
            String fullFilePath = filePath + fileName + suffix;
            writeFile(data_distribute, rel_sql, fullFilePath);
            String signalFile = filePath + data_distribute.getFile_name() + ".ok";
            if (IsFlag.Shi == IsFlag.ofEnumByCode(data_distribute.getIs_flag())) {
                FileUtil.createFileIfAbsent(signalFile, "");
            }
            String outPath = parentFile.getAbsolutePath() + separator + data_distribute.getFile_name() + ".zip";
            if (IsFlag.Shi == IsFlag.ofEnumByCode(data_distribute.getIs_compress())) {
                packFile(data_distribute, parentFile, outPath);
            }
        } catch (Exception e) {
            log.error("数据分发失败：" + e);
            throw new AppSystemException("数据分发失败：" + e);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "data_distribute", desc = "", range = "", isBean = true)
    @Param(name = "parentFile", desc = "", range = "")
    @Param(name = "outPath", desc = "", range = "")
    public static void packFile(DataDistribute data_distribute, File parentFile, String outPath) throws IOException {
        outPath = FilenameUtils.normalize(outPath);
        File[] resourceList = parentFile.listFiles();
        if (resourceList == null || resourceList.length < 1) {
            throw new AppSystemException("压缩文件失败，该目录下没有文件：" + parentFile.getAbsolutePath());
        }
        Path path = Paths.get(outPath);
        try {
            for (File resourceFile : resourceList) {
                if (!resourceFile.isFile()) {
                    continue;
                }
                String name = resourceFile.getName().toLowerCase();
                if ((name.contains(data_distribute.getFile_name().toLowerCase()) && name.endsWith(data_distribute.getFile_suffix())) || (name.contains(data_distribute.getFile_name().toLowerCase()) && name.endsWith("ok"))) {
                    ZipUtils.compress(outPath, resourceFile.getPath());
                }
            }
        } catch (Exception e) {
            Files.delete(path);
            throw new AppSystemException("文件压缩至 " + outPath + " 执行异常!" + e);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "data_distribute", desc = "", range = "", isBean = true)
    @Param(name = "sql", desc = "", range = "")
    @Param(name = "filePath", desc = "", range = "")
    private static void writeFile(DataDistribute data_distribute, String sql, String filePath) {
        try (DatabaseWrapper db = new DatabaseWrapper();
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(Paths.get(filePath)), DataBaseCode.ofValueByCode(data_distribute.getDatabase_code())))) {
            new ProcessingData() {

                long lineCounter = 0;

                @Override
                public void dealLine(Map<String, Object> map) throws IOException {
                    lineCounter++;
                    StringBuffer sbCol = new StringBuffer();
                    StringBuffer sbVal = new StringBuffer();
                    map.forEach((k, v) -> {
                        if (lineCounter == 1) {
                            sbCol.append(k).append(data_distribute.getDatabase_separatorr());
                        }
                        sbVal.append(v).append(data_distribute.getDatabase_separatorr());
                    });
                    if (lineCounter == 1 && IsFlag.Shi == IsFlag.ofEnumByCode(data_distribute.getIs_header())) {
                        writeData(bufferedWriter, sbCol, data_distribute);
                    }
                    writeData(bufferedWriter, sbVal, data_distribute);
                    if (lineCounter % CommonVariables.DB_BATCH_ROW == 0) {
                        bufferedWriter.flush();
                    }
                    bufferedWriter.flush();
                }
            }.getDataLayer(sql, db);
        } catch (Exception e) {
            throw new AppSystemException(e.getMessage());
        }
    }

    private static void writeData(BufferedWriter bufferedWriter, StringBuffer sb, DataDistribute data_distribute) throws IOException {
        sb.delete(sb.length() - data_distribute.getDatabase_separatorr().length(), sb.length() + data_distribute.getDatabase_separatorr().length());
        if (data_distribute.getRow_separator().equals("\\r\\n") || data_distribute.getRow_separator().equals("\\r") || data_distribute.getRow_separator().equals("\\n")) {
            data_distribute.setRow_separator(System.lineSeparator());
        }
        sb.append(data_distribute.getRow_separator());
        bufferedWriter.write(sb.toString());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "data_distribute", desc = "", range = "", isBean = true)
    public static void checkoutParams(DataDistribute data_distribute) {
        Validator.notBlank(data_distribute.getSql_table(), "sql语句或表名不能为空!");
        Validator.notBlank(data_distribute.getIs_header(), "是否为表头信息不能为空!");
        Validator.notBlank(data_distribute.getDatabase_code(), "数据编码格式不能为空!");
        Validator.notBlank(data_distribute.getDbfile_format(), "文件格式不能为空!");
        Validator.notBlank(data_distribute.getFile_name(), "文件名称不能为空!");
        Validator.notBlank(data_distribute.getFile_suffix(), "文件后缀不能为空!");
        Validator.notBlank(data_distribute.getIs_upper(), "文件名是否大写信息不能为空!");
        Validator.notBlank(data_distribute.getIs_compress(), "文件是否压缩信息不能为空!");
        Validator.notBlank(data_distribute.getIs_flag(), "是否标识文件信息不能为空!");
        Validator.notBlank(data_distribute.getIs_release(), "是否发布信息不能为空!");
    }
}
