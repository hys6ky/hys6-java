package hyren.serv6.agent.job.biz.core.metaparse.impl;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.agent.job.biz.bean.ObjectCollectParamBean;
import hyren.serv6.agent.job.biz.bean.ObjectTableBean;
import hyren.serv6.agent.job.biz.core.jdbcdirectstage.JdbcDirectUnloadDataStageImpl;
import hyren.serv6.commons.utils.TypeTransLength;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.ObjectCollectStruct;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.constant.Constant;
import java.util.List;

@DocClass(desc = "", author = "zxz", createdate = "2019/12/4 11:17")
public class ObjectCollectTableHandleParse {

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceDataConfBean", desc = "", range = "")
    @Param(name = "collectTableBean", desc = "", range = "")
    @Return(desc = "", range = "")
    public TableBean generateTableInfo(ObjectCollectParamBean objectCollectParamBean, ObjectTableBean objectTableBean) {
        TableBean tableBean = new TableBean();
        StringBuilder allColumns = new StringBuilder();
        StringBuilder allType = new StringBuilder();
        StringBuilder columnMetaInfo = new StringBuilder();
        StringBuilder colTypeMetaInfo = new StringBuilder();
        StringBuilder colLengthInfo = new StringBuilder();
        StringBuilder primaryKeyInfo = new StringBuilder();
        List<ObjectCollectStruct> object_collect_structList = objectTableBean.getObject_collect_structList();
        for (ObjectCollectStruct object_collect_struct : object_collect_structList) {
            if (IsFlag.Shi.getCode().equals(object_collect_struct.getIs_operate())) {
                tableBean.setOperate_column(object_collect_struct.getColumn_name());
            } else {
                String colName = object_collect_struct.getColumn_name();
                String colType = object_collect_struct.getColumn_type();
                allColumns.append(colName).append(Constant.METAINFOSPLIT);
                allType.append(colType).append(Constant.METAINFOSPLIT);
                columnMetaInfo.append(colName).append(Constant.METAINFOSPLIT);
                colTypeMetaInfo.append(colType).append(Constant.METAINFOSPLIT);
                colLengthInfo.append(TypeTransLength.getLength(colType)).append(Constant.METAINFOSPLIT);
                primaryKeyInfo.append(object_collect_struct.getIs_zipper_field()).append(Constant.METAINFOSPLIT);
            }
        }
        if (colLengthInfo.length() > 0) {
            colLengthInfo.delete(colLengthInfo.length() - 1, colLengthInfo.length());
            allType.delete(allType.length() - 1, allType.length());
            allColumns.delete(allColumns.length() - 1, allColumns.length());
            colTypeMetaInfo.delete(colTypeMetaInfo.length() - 1, colTypeMetaInfo.length());
            columnMetaInfo.delete(columnMetaInfo.length() - 1, columnMetaInfo.length());
            primaryKeyInfo.delete(primaryKeyInfo.length() - 1, primaryKeyInfo.length());
        }
        columnMetaInfo.append(Constant.METAINFOSPLIT).append(Constant._HYREN_S_DATE).append(Constant.METAINFOSPLIT).append(Constant._HYREN_E_DATE).append(Constant.METAINFOSPLIT).append(Constant._HYREN_MD5_VAL);
        colTypeMetaInfo.append(Constant.METAINFOSPLIT).append("char(8)").append(Constant.METAINFOSPLIT).append("char(8)").append(Constant.METAINFOSPLIT).append("char(32)");
        colLengthInfo.append(Constant.METAINFOSPLIT).append("8").append(Constant.METAINFOSPLIT).append("8").append(Constant.METAINFOSPLIT).append("32");
        tableBean.setDbFileArchivedCode(JdbcDirectUnloadDataStageImpl.getStoreDataBaseCode(objectTableBean.getEn_name(), objectTableBean.getDataStoreConfBean(), objectCollectParamBean.getDatabase_code()));
        tableBean.setAllColumns(allColumns.toString());
        tableBean.setAllType(allType.toString());
        tableBean.setColLengthInfo(colLengthInfo.toString());
        tableBean.setColTypeMetaInfo(colTypeMetaInfo.toString());
        tableBean.setColumnMetaInfo(columnMetaInfo.toString());
        tableBean.setPrimaryKeyInfo(primaryKeyInfo.toString());
        tableBean.setColumn_separator(Constant.DATADELIMITER);
        tableBean.setIs_header(IsFlag.Fou.getCode());
        tableBean.setRow_separator(Constant.DEFAULTLINESEPARATOR);
        tableBean.setFile_format(FileFormat.FeiDingChang.getCode());
        tableBean.setFile_code(tableBean.getDbFileArchivedCode());
        return tableBean;
    }
}
