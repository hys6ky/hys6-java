package hyren.serv6.commons.hadoop.fileparser;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.MD5Util;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.commons.dataclean.Clean;
import hyren.serv6.commons.dataclean.CleanFactory;
import hyren.serv6.commons.dataclean.DataCleanInterface;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.writer.AbstractFileWriter;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", createdate = "2020/4/21 16:44", author = "zxz")
public abstract class FileParserAbstract implements FileParserInterface {

    protected TableBean tableBean;

    private final CollectTableBean collectTableBean;

    protected String readFile;

    protected BufferedWriter writer;

    protected List<String> dictionaryColumnList;

    private final List<String> allColumnList;

    protected List<String> dictionaryTypeList;

    private final Map<String, String> mergeIng;

    private final Clean cl;

    private final StringBuilder mergeStringTmp;

    private final Map<String, Boolean> md5Col;

    private final StringBuilder md5StringTmp;

    private final StringBuilder lineSb;

    private final DataCleanInterface allClean;

    private final boolean isMd5;

    protected String unloadFileAbsolutePath;

    private final String etl_date;

    private final String operateDate;

    private final String operateTime;

    private final long user_id;

    @SuppressWarnings("unchecked")
    protected FileParserAbstract(TableBean tableBean, CollectTableBean collectTableBean, String readFile) throws Exception {
        this.collectTableBean = collectTableBean;
        this.readFile = readFile;
        this.tableBean = tableBean;
        this.unloadFileAbsolutePath = FileNameUtils.normalize(Constant.DBFILEUNLOADFOLDER + collectTableBean.getDatabase_id() + File.separator + collectTableBean.getStorage_table_name() + File.separator + collectTableBean.getEtlDate() + File.separator + FileNameUtils.getName(readFile), true);
        this.writer = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(Paths.get(unloadFileAbsolutePath)), DataBaseCode.ofValueByCode(tableBean.getDbFileArchivedCode())));
        allClean = CleanFactory.getInstance().getObjectClean("clean_database");
        this.allColumnList = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
        this.dictionaryColumnList = StringUtil.split(tableBean.getAllColumns(), Constant.METAINFOSPLIT);
        this.mergeIng = (Map<String, String>) tableBean.getParseJson().get("mergeIng");
        this.cl = new Clean(tableBean.getParseJson(), allClean);
        this.mergeStringTmp = new StringBuilder();
        this.lineSb = new StringBuilder();
        this.dictionaryTypeList = StringUtil.split(tableBean.getAllType(), Constant.METAINFOSPLIT);
        this.isMd5 = IsFlag.Shi.getCode().equals(collectTableBean.getIs_md5()) || (IsFlag.Shi.getCode().equals(collectTableBean.getIs_zipper()) && StorageType.ZengLiang.getCode().equals(collectTableBean.getStorage_type())) || (IsFlag.Shi.getCode().equals(collectTableBean.getIs_zipper()) && StorageType.QuanLiang.getCode().equals(collectTableBean.getStorage_type()));
        this.etl_date = collectTableBean.getEtlDate();
        this.operateDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        this.operateTime = new SimpleDateFormat("HH:mm:ss").format(new Date());
        this.user_id = collectTableBean.getUser_id();
        this.md5Col = transMd5ColMap(tableBean.getIsZipperFieldInfo());
        this.md5StringTmp = new StringBuilder(1024 * 1024);
    }

    @Override
    public void stopStream() throws IOException {
        writer.close();
    }

    @Override
    public void dealLine(List<String> lineList) throws IOException {
        String columnName;
        String columnData;
        md5StringTmp.delete(0, md5StringTmp.length());
        mergeStringTmp.delete(0, mergeStringTmp.length());
        lineSb.delete(0, lineSb.length());
        for (int i = 0; i < lineList.size(); i++) {
            columnName = dictionaryColumnList.get(i);
            if (Constant._HYREN_S_DATE.equalsIgnoreCase(columnName) || Constant._HYREN_E_DATE.equalsIgnoreCase(columnName) || Constant._HYREN_MD5_VAL.equalsIgnoreCase(columnName) || Constant._HYREN_OPER_DATE.equalsIgnoreCase(columnName) || Constant._HYREN_OPER_TIME.equalsIgnoreCase(columnName) || Constant._HYREN_OPER_PERSON.equalsIgnoreCase(columnName)) {
                continue;
            }
            if (null == lineList.get(i)) {
                columnData = "";
            } else {
                columnData = lineList.get(i).trim();
            }
            if (md5Col.get(columnName) != null && md5Col.get(columnName)) {
                md5StringTmp.append(columnData);
            }
            columnData = cl.cleanColumn(columnData, columnName, dictionaryTypeList.get(i), FileFormat.FeiDingChang.getCode(), null, null, Constant.DATADELIMITER);
            columnData = AbstractFileWriter.clearIrregularData(columnData);
            mergeStringTmp.append(columnData).append(Constant.DATADELIMITER);
            lineSb.append(columnData).append(Constant.DATADELIMITER);
        }
        if (!mergeIng.isEmpty()) {
            List<String> arrColString = StringUtil.split(mergeStringTmp.toString(), Constant.DATADELIMITER);
            String merge = allClean.merge(mergeIng, arrColString.toArray(new String[0]), allColumnList.toArray(new String[0]), null, FileFormat.FeiDingChang.getCode(), null, Constant.DATADELIMITER);
            mergeStringTmp.append(merge);
            lineSb.append(merge).append(Constant.DATADELIMITER);
        }
        lineSb.append(etl_date);
        if (isMd5) {
            lineSb.append(Constant.DATADELIMITER).append(Constant._MAX_DATE_8);
            lineSb.append(Constant.DATADELIMITER).append(MD5Util.md5String(md5StringTmp.toString()));
        }
        appendOperateInfo(lineSb);
        lineSb.append(Constant.DEFAULTLINESEPARATOR);
        writer.write(lineSb.toString());
    }

    protected void checkData(List<String> valueList, long fileRowCount) {
        if (dictionaryColumnList.size() != valueList.size()) {
            String mss = "第 " + fileRowCount + " 行数据 ，数据字典表(" + collectTableBean.getTable_name() + " )定义字段数量和数据列列数不匹配！" + "\n" + "数据字典定义的数据字段数量是  " + dictionaryColumnList.size() + " 现在获取的数据文件列数是  " + valueList.size() + "\n" + "数据为 " + valueList;
            throw new AppSystemException(mss);
        }
    }

    private void appendOperateInfo(StringBuilder sb) {
        if (JobConstant.ISADDOPERATEINFO) {
            sb.append(Constant.DATADELIMITER).append(operateDate).append(Constant.DATADELIMITER).append(operateTime).append(Constant.DATADELIMITER).append(user_id);
        }
    }

    public static Map<String, Boolean> transMd5ColMap(Map<String, Boolean> md5ColMap) {
        Map<String, Boolean> map = new HashMap<>();
        boolean flag = true;
        for (String key : md5ColMap.keySet()) {
            if (md5ColMap.get(key)) {
                flag = false;
                break;
            }
        }
        if (flag) {
            for (String key : md5ColMap.keySet()) {
                map.put(key, true);
            }
        } else {
            map = md5ColMap;
        }
        return map;
    }
}
