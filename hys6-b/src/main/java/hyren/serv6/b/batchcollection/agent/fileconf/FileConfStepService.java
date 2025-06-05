package hyren.serv6.b.batchcollection.agent.fileconf;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.agent.CheckParam;
import hyren.serv6.base.codes.DataExtractType;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.constant.Constant;
import io.swagger.annotations.Api;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;

@Service
@Api("定义卸数文件配置")
@DocClass(desc = "", author = "WangZhengcheng")
public class FileConfStepService {

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getInitInfo(long colSetId) {
        List<Map<String, Object>> table_infos = Dbo.queryList("SELECT table_id,table_name,table_ch_name FROM " + TableInfo.TableName + " WHERE database_id = ? ORDER BY table_name", colSetId);
        table_infos.forEach(tableInfo -> {
            List<Map<String, Object>> list = Dbo.queryList(" select * " + " from " + TableInfo.TableName + " ti left join " + DataExtractionDef.TableName + " ded " + " on ti.table_id = ded.table_id where ti.database_id = ? AND ti.table_id = ? ", colSetId, tableInfo.get("table_id"));
            list.forEach(item -> {
                if (item.get("row_separator") != null) {
                    item.put("row_separator", StringUtil.unicode2String(String.valueOf(item.get("row_separator"))));
                }
                if (item.get("database_separatorr") != null) {
                    item.put("database_separatorr", StringUtil.unicode2String(String.valueOf(item.get("database_separatorr"))));
                }
            });
            tableInfo.put("tableData", list);
        });
        return table_infos;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "extractionDefString", desc = "", range = "", isBean = true)
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "dedId", desc = "", range = "", nullable = true, valueIfNull = "")
    @Return(desc = "", range = "")
    public long saveFileConf(DataExtractionDef[] extractionDefString, long colSetId, String dedId) {
        if (extractionDefString == null || extractionDefString.length == 0) {
            throw new BusinessException("未获取到卸数文件信息");
        }
        if (StringUtil.isNotBlank(dedId)) {
            List<String> split = StringUtil.split(dedId, "^");
            deleteDataExtractionDef(split);
        }
        for (int i = 0; i < extractionDefString.length; i++) {
            DataExtractionDef def = extractionDefString[i];
            if (def.getTable_id() == null) {
                throw new BusinessException("保存卸数文件配置，第" + (i + 1) + "数据必须关联表ID");
            } else {
            }
            if (StringUtil.isBlank(def.getDbfile_format())) {
                throw new BusinessException("第 " + (i + 1) + " 条的数据抽取方式不能为空!");
            }
            FileFormat fileFormat = FileFormat.ofEnumByCode(def.getDbfile_format());
            if (fileFormat == FileFormat.FeiDingChang || fileFormat == FileFormat.DingChang) {
                String row_separator = def.getRow_separator();
                if (StringUtil.isBlank(row_separator)) {
                    throw new BusinessException("第 " + (i + 1) + " 条的数据行分割符不能为空!");
                } else {
                    def.setRow_separator(StringUtil.string2Unicode(row_separator));
                }
                String database_separatorr = def.getDatabase_separatorr();
                if (StringUtil.isBlank(database_separatorr)) {
                    if (fileFormat != FileFormat.DingChang) {
                        throw new BusinessException("第 " + (i + 1) + " 条的数据分隔符不能为空!");
                    }
                } else {
                    def.setDatabase_separatorr(StringUtil.string2Unicode(database_separatorr));
                }
            } else if (fileFormat == FileFormat.ORC || fileFormat == FileFormat.SEQUENCEFILE || fileFormat == FileFormat.PARQUET) {
                def.setDatabase_separatorr("");
                def.setRow_separator("");
                def.setIs_header(IsFlag.Fou.getCode());
            }
            if (StringUtil.isBlank(def.getDatabase_code())) {
                throw new BusinessException("第 " + (i + 1) + " 条的数据字符集不能为空!");
            }
            if (StringUtil.isBlank(def.getPlane_url())) {
                throw new BusinessException("第 " + (i + 1) + " 条的数据落地目录不能为空!");
            }
            def.setData_extract_type(DataExtractType.ShuJuKuChouQuLuoDi.getCode());
            if (def.getDed_id() == null) {
                def.setDed_id(PrimayKeyGener.getNextId());
                def.add(Dbo.db());
            } else {
                def.update(Dbo.db());
            }
        }
        return colSetId;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_id", desc = "", range = "")
    @Param(name = "newDedId", desc = "", range = "")
    private void updateOldTakeId(long newDedId, long table_id) {
        Dbo.execute("UPDATE " + TakeRelationEtl.TableName + " SET take_id = ? WHERE take_id in(" + "SELECT ded_id FROM " + DataExtractionDef.TableName + " WHERE table_id = ?)", newDedId, table_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "def", desc = "", range = "")
    private void verifySeqConf(DataExtractionDef[] dataExtractionDefs) {
        for (int i = 0; i < dataExtractionDefs.length; i++) {
            DataExtractionDef def = dataExtractionDefs[i];
            if (def.getTable_id() == null) {
                throw new BusinessException("保存卸数文件配置，第" + (i + 1) + "数据必须关联表ID");
            }
            DataExtractType extractType = DataExtractType.ofEnumByCode(def.getData_extract_type());
            FileFormat fileFormat = FileFormat.ofEnumByCode(def.getDbfile_format());
            if (extractType == DataExtractType.ShuJuKuChouQuLuoDi) {
                if (fileFormat == FileFormat.ORC || fileFormat == FileFormat.PARQUET || fileFormat == FileFormat.SEQUENCEFILE) {
                    throw new BusinessException("仅抽取操作，只能指定非定长|定长|CSV三种存储格式");
                }
                if (fileFormat == FileFormat.FeiDingChang) {
                    if (StringUtil.isEmpty(def.getRow_separator())) {
                        throw new BusinessException("数据抽取保存为非定长文件，请填写行分隔符");
                    }
                    if (StringUtil.isEmpty(def.getDatabase_separatorr())) {
                        throw new BusinessException("数据抽取保存为非定长文件，请填写列分隔符");
                    }
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dedId", desc = "", range = "")
    private void deleteDataExtractionDef(List<String> dedId) {
        for (String s : dedId) {
            long ded_id = Long.parseLong(s);
            long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + DataExtractionDef.TableName + " WHERE ded_id = ?", ded_id).orElseThrow(() -> new BusinessException("SQL查询异常"));
            if (countNum == 0) {
                CheckParam.throwErrorMsg("数据抽取定义方式不存在");
            }
            Map<String, Object> map = Dbo.queryOneObject("SELECT etl_job_id,etl_sys_id,sub_sys_id FROM " + DataExtractionDef.TableName + " t1 join " + TakeRelationEtl.TableName + " t2 on t1.ded_id = t2.take_id WHERE t1.ded_id = ?", ded_id);
            Dbo.execute("DELETE FROM " + EtlJobDef.TableName + " WHERE etl_job_id = ( SELECT etl_job_id FROM " + DataExtractionDef.TableName + " t1 join " + TakeRelationEtl.TableName + " t2 on t1.ded_id = t2.take_id WHERE t1.ded_id = ?) AND etl_sys_id = ?", ded_id, map.get("etl_sys_id"));
            Dbo.execute("DELETE FROM " + EtlDependency.TableName + " WHERE pre_etl_job_id = ( SELECT etl_job_id FROM " + DataExtractionDef.TableName + " t1 join " + TakeRelationEtl.TableName + " t2 on t1.ded_id = t2.take_id WHERE t1.ded_id = ?) AND pre_etl_sys_id = ? ", ded_id, map.get("etl_sys_id"));
            Dbo.execute("DELETE FROM " + EtlJobResourceRela.TableName + " WHERE etl_job_id = ( SELECT etl_job_id FROM " + DataExtractionDef.TableName + " t1 join " + TakeRelationEtl.TableName + " t2 on t1.ded_id = t2.take_id WHERE t1.ded_id = ?) AND etl_sys_id = ?", ded_id, map.get("etl_sys_id"));
            Dbo.execute("DELETE FROM " + DataExtractionDef.TableName + " WHERE ded_id = ?", ded_id);
            Dbo.execute("DELETE FROM " + TakeRelationEtl.TableName + " WHERE take_id = ?", ded_id);
        }
    }

    public String getSqlParamPlaceholder() {
        return Constant.SQLDELIMITER;
    }
}
