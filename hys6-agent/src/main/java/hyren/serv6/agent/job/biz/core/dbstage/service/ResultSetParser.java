package hyren.serv6.agent.job.biz.core.dbstage.service;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.core.dbstage.writer.FileWriterFactory;
import hyren.serv6.agent.job.biz.utils.InvokeMethod;
import hyren.serv6.agent.job.biz.utils.WriterFile;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@DocClass(desc = "", author = "WangZhengcheng")
public class ResultSetParser {

    @Method(desc = "", logicStep = "")
    @Param(name = "rs", desc = "", range = "")
    @Param(name = "jobInfo", desc = "", range = "")
    @Param(name = "pageNum", desc = "", range = "")
    @Param(name = "pageRow", desc = "", range = "")
    @Return(desc = "", range = "")
    public String parseResultSet(ResultSet rs, CollectTableBean collectTableBean, int pageNum, TableBean tableBean, DataExtractionDef data_extraction_def, boolean writeHeaderFlag) {
        if (StringUtil.isEmpty(data_extraction_def.getFile_suffix())) {
            data_extraction_def.setFile_suffix("dat");
        }
        return FileWriterFactory.getFileWriterImpl(rs, collectTableBean, pageNum, tableBean, data_extraction_def, writeHeaderFlag);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "sql", desc = "", range = "")
    @Param(name = "data_extraction_def", desc = "", range = "")
    @Return(desc = "", range = "")
    public String copyToFile(DatabaseWrapper db, DataExtractionDef data_extraction_def, CollectTableBean collectTableBean, String sql) {
        List<String> selectColumnList = appendOperateInfo(db, collectTableBean.getIs_md5(), collectTableBean.getUser_id(), collectTableBean.getEtlDate());
        if (StringUtil.isEmpty(data_extraction_def.getFile_suffix())) {
            data_extraction_def.setFile_suffix("dat");
        }
        String eltDate = collectTableBean.getEtlDate();
        String database_separatorr = data_extraction_def.getDatabase_separatorr();
        FileFormat format = FileFormat.ofEnumByCode(data_extraction_def.getDbfile_format());
        if (FileFormat.FeiDingChang != format && FileFormat.DingChang != format) {
            throw new AppSystemException("人大金仓全量方式卸数,只支持定长文件和非定长文件抽取");
        }
        String database_code = data_extraction_def.getDatabase_code();
        database_code = DataBaseCode.ofValueByCode(database_code);
        String midName = data_extraction_def.getPlane_url() + File.separator + eltDate + File.separator + collectTableBean.getTable_name() + File.separator + Constant.fileFormatMap.get(format.getCode()) + File.separator + collectTableBean.getStorage_table_name() + "." + data_extraction_def.getFile_suffix();
        long tableCountNum = copyDatabaseToFile(db.getConnection(), selectColumnList, midName, database_separatorr, database_code, sql, collectTableBean.getTable_name());
        return midName + Constant.METAINFOSPLIT + tableCountNum;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sql", desc = "", range = "")
    @Param(name = "path", desc = "", range = "")
    @Param(name = "delimiter", desc = "", range = "")
    @Param(name = "fileEncoding", desc = "", range = "")
    private long copyDatabaseToFile(Connection conn, List<String> selectColumnList, String path, String delimiter, String fileEncoding, String sql, String tableName) {
        WriterFile writerFile = new WriterFile(path);
        try {
            sql = String.format("SELECT *,%s FROM ( %s ) tmp", StringUtil.join(selectColumnList, ","), sql);
            sql = String.format("copy (%s) to stdout CSV DELIMITER '%s' ENCODING '%s' ", sql, delimiter, fileEncoding);
            return InvokeMethod.executeKingBaseCopyOut(conn, sql, path, tableName);
        } catch (Exception e) {
            throw new AppSystemException("数据库增量抽取卸数文件失败", e);
        } finally {
            writerFile.incrementBufferedWriterClose();
        }
    }

    private List<String> appendOperateInfo(DatabaseWrapper db, String isMd5, Long user_id, String etlDate) {
        List<String> selectColumnList = new ArrayList<>();
        selectColumnList.add("'" + etlDate + "' " + Constant._HYREN_S_DATE);
        if (IsFlag.Shi.getCode().equals(isMd5)) {
            selectColumnList.add("'" + Constant._MAX_DATE_8 + "' " + Constant._HYREN_E_DATE);
            selectColumnList.add("'" + db.getDbtype().ofColMd5(db, selectColumnList) + "' " + Constant._HYREN_MD5_VAL);
        }
        if (JobConstant.ISADDOPERATEINFO) {
            String operateDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            String operateTime = new SimpleDateFormat("HH:mm:ss").format(new Date());
            selectColumnList.add("'" + operateDate + "' " + Constant._HYREN_OPER_DATE);
            selectColumnList.add("'" + operateTime + "' " + Constant._HYREN_OPER_TIME);
            selectColumnList.add(user_id + " " + Constant._HYREN_OPER_PERSON);
        }
        return selectColumnList;
    }
}
