package hyren.serv6.agent.job.biz.core.metaparse.impl;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.bean.*;
import hyren.serv6.agent.job.biz.core.metaparse.AbstractCollectTableHandle;
import hyren.serv6.agent.job.biz.utils.CollectTableBeanUtil;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.CollectTableColumnBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.bean.TbColTarTypeMapBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@DocClass(desc = "", author = "zxz", createdate = "2019/12/4 11:17")
@Slf4j
public class JdbcDirectCollectTableHandleParse extends AbstractCollectTableHandle {

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceDataConfBean", desc = "", range = "")
    @Param(name = "collectTableBean", desc = "", range = "")
    @Return(desc = "", range = "")
    public TableBean generateTableInfo(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean) {
        return getFullAmountExtractTableBean(sourceDataConfBean, collectTableBean);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceDataConfBean", desc = "", range = "")
    @Param(name = "collectTableBean", desc = "", range = "")
    @Return(desc = "", range = "")
    private TableBean getFullAmountExtractTableBean(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean) {
        TableBean tableBean = new TableBean();
        ResultSet resultSet = null;
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(CollectTableBeanUtil.setJdbcBean(sourceDataConfBean))) {
            String collectSQL = getCollectSQL(collectTableBean, db, sourceDataConfBean.getDatabase_name());
            tableBean.setCollectSQL(collectSQL);
            if (collectSQL.contains(Constant.SQLDELIMITER)) {
                resultSet = getResultSet(StringUtil.split(collectSQL, Constant.SQLDELIMITER).get(0), db);
            } else {
                resultSet = getResultSet(collectSQL, db);
            }
            StringBuilder columnMetaInfo = new StringBuilder();
            StringBuilder allColumns = new StringBuilder();
            StringBuilder colTypeMetaInfo = new StringBuilder();
            StringBuilder allType = new StringBuilder();
            StringBuilder colLengthInfo = new StringBuilder();
            ResultSetMetaData rsMetaData = resultSet.getMetaData();
            int numberOfColumns = rsMetaData.getColumnCount();
            int[] typeArray = new int[numberOfColumns];
            for (int i = 1; i <= numberOfColumns; i++) {
                String columnTmp = rsMetaData.getColumnName(i);
                String[] names = columnTmp.split("\\.");
                columnTmp = names[names.length - 1];
                int columnType = rsMetaData.getColumnType(i);
                if (!columnTmp.equalsIgnoreCase("hyren_rn")) {
                    columnMetaInfo.append(columnTmp).append(STRSPLIT);
                    allColumns.append(columnTmp.toUpperCase()).append(STRSPLIT);
                }
                typeArray[i - 1] = columnType;
            }
            Map<String, String> tableColTypeAndLength = getTableColTypeAndLengthSql(resultSet);
            for (String key : tableColTypeAndLength.keySet()) {
                List<String> split = StringUtil.split(tableColTypeAndLength.get(key), "::");
                colTypeMetaInfo.append(split.get(0)).append(STRSPLIT);
                allType.append(split.get(0)).append(STRSPLIT);
                colLengthInfo.append(split.get(1)).append(STRSPLIT);
            }
            columnMetaInfo.deleteCharAt(columnMetaInfo.length() - 1);
            allColumns.deleteCharAt(allColumns.length() - 1);
            colLengthInfo.deleteCharAt(colLengthInfo.length() - 1);
            colTypeMetaInfo.deleteCharAt(colTypeMetaInfo.length() - 1);
            allType.deleteCharAt(allType.length() - 1);
            columnMetaInfo.append(STRSPLIT).append(Constant._HYREN_S_DATE);
            colTypeMetaInfo.append(STRSPLIT).append("char(8)");
            colLengthInfo.append(STRSPLIT).append("8");
            if (IsFlag.Shi == IsFlag.ofEnumByCode(collectTableBean.getIs_zipper())) {
                columnMetaInfo.append(STRSPLIT).append(Constant._HYREN_E_DATE).append(STRSPLIT).append(Constant._HYREN_MD5_VAL);
                colTypeMetaInfo.append(STRSPLIT).append("char(8)").append(STRSPLIT).append("char(32)");
                colLengthInfo.append(STRSPLIT).append("8").append(STRSPLIT).append("32");
                collectTableBean.setIs_md5(IsFlag.Shi.getCode());
            }
            StorageType storageType = StorageType.ofEnumByCode(collectTableBean.getStorage_type());
            if ((storageType == StorageType.TiHuan || storageType == StorageType.ZhuiJia) && IsFlag.Shi == IsFlag.ofEnumByCode(collectTableBean.getIs_md5())) {
                columnMetaInfo.append(STRSPLIT).append(Constant._HYREN_MD5_VAL);
                colTypeMetaInfo.append(STRSPLIT).append("char(32)");
                colLengthInfo.append(STRSPLIT).append("32");
            }
            if (JobConstant.ISADDOPERATEINFO) {
                columnMetaInfo.append(STRSPLIT).append(Constant._HYREN_OPER_DATE).append(STRSPLIT).append(Constant._HYREN_OPER_TIME).append(STRSPLIT).append(Constant._HYREN_OPER_PERSON);
                colTypeMetaInfo.append(STRSPLIT).append("char(10)").append(STRSPLIT).append("char(8)").append(STRSPLIT).append("char(4)");
                colLengthInfo.append(STRSPLIT).append("10").append(STRSPLIT).append("8").append(STRSPLIT).append("4");
            }
            StringBuilder primaryKeyInfo = new StringBuilder();
            Map<String, Boolean> isZipperFieldInfo = new HashMap<>();
            List<String> column_list = StringUtil.split(columnMetaInfo.toString(), STRSPLIT);
            log.info("=============================数据库的Meta信息==============================: " + column_list);
            List<CollectTableColumnBean> collectTableColumnBeanList = collectTableBean.getCollectTableColumnBeanList();
            Map<Long, String> tbColTarMap = new HashMap<>();
            List<TbColTarTypeMapBean> tbcol_srctgt_maps = collectTableBean.getTbColTarTypeMaps();
            Map<Long, List<TbColTarTypeMapBean>> tbColMap = new HashMap<>();
            if (!tbcol_srctgt_maps.isEmpty()) {
                tbColMap = tbcol_srctgt_maps.stream().collect(Collectors.groupingBy(TbColTarTypeMapBean::getDsl_id));
            }
            CollectTableBeanUtil.setColTarType(tbColTarMap, tbColMap, column_list, AgentType.ShuJuKu);
            for (String col : column_list) {
                boolean flag = true;
                for (CollectTableColumnBean columnBean : collectTableColumnBeanList) {
                    if (columnBean.getColumn_name().equalsIgnoreCase(col)) {
                        primaryKeyInfo.append(columnBean.getIs_primary_key()).append(STRSPLIT);
                        flag = false;
                        break;
                    }
                }
                if (flag) {
                    primaryKeyInfo.append(IsFlag.Fou.getCode()).append(STRSPLIT);
                }
                boolean zipper_flag = true;
                for (CollectTableColumnBean columnBean : collectTableColumnBeanList) {
                    if (columnBean.getColumn_name().equals(col)) {
                        isZipperFieldInfo.put(col.toUpperCase(), IsFlag.Shi.getCode().equals(columnBean.getIs_zipper_field()));
                        zipper_flag = false;
                        break;
                    }
                }
                if (zipper_flag) {
                    isZipperFieldInfo.put(col.toUpperCase(), false);
                }
            }
            primaryKeyInfo.deleteCharAt(primaryKeyInfo.length() - 1);
            tableBean.setAllColumns(allColumns.toString());
            tableBean.setAllType(allType.toString());
            tableBean.setColLengthInfo(colLengthInfo.toString());
            tableBean.setColTypeMetaInfo(colTypeMetaInfo.toString());
            tableBean.setColumnMetaInfo(columnMetaInfo.toString().toUpperCase());
            tableBean.setTypeArray(typeArray);
            tableBean.setPrimaryKeyInfo(primaryKeyInfo.toString());
            tableBean.setIsZipperFieldInfo(isZipperFieldInfo);
            tableBean.setTbColTarMap(tbColTarMap);
            tableBean.setStorage_type(collectTableBean.getStorage_type());
            tableBean.setStorage_time(collectTableBean.getStorage_time());
        } catch (Exception e) {
            log.error("=====================", e);
            throw new AppSystemException("根据数据源信息和采集表信息得到卸数元信息失败！", e);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return tableBean;
    }
}
