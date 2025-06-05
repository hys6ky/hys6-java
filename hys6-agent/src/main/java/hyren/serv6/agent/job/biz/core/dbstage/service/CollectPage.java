package hyren.serv6.agent.job.biz.core.dbstage.service;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.agent.job.biz.utils.CollectTableBeanUtil;
import hyren.serv6.base.codes.UnloadType;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.constant.Constant;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@DocClass(desc = "", author = "WangZhengcheng")
public class CollectPage implements Callable<Map<String, Object>> {

    private final SourceDataConfBean sourceDataConfBean;

    private final CollectTableBean collectTableBean;

    private final TableBean tableBean;

    private final String sql;

    private final int start;

    private final int end;

    private final int pageNum;

    public CollectPage(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean, TableBean tableBean, int start, int end, int pageNum) {
        this.sourceDataConfBean = sourceDataConfBean;
        this.collectTableBean = collectTableBean;
        this.tableBean = tableBean;
        this.start = start;
        this.end = end;
        this.pageNum = pageNum;
        this.sql = tableBean.getCollectSQL();
    }

    public CollectPage(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean, TableBean tableBean, int start, int end, int pageNum, String sql) {
        this.sourceDataConfBean = sourceDataConfBean;
        this.collectTableBean = collectTableBean;
        this.tableBean = tableBean;
        this.start = start;
        this.end = end;
        this.pageNum = pageNum;
        this.sql = sql;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public Map<String, Object> call() {
        ResultSet resultSet = null;
        sourceDataConfBean.setFetch_size(sourceDataConfBean.getFetch_size() == 0 ? 50 : sourceDataConfBean.getFetch_size());
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(CollectTableBeanUtil.setJdbcBean(sourceDataConfBean))) {
            List<DataExtractionDef> data_extraction_def_list = CollectTableBeanUtil.getTransSeparatorExtractionList(collectTableBean.getData_extraction_def_list());
            Map<String, Object> map = new HashMap<>();
            for (DataExtractionDef data_extraction_def : data_extraction_def_list) {
                if (!collectTableBean.getSelectFileFormat().equals(data_extraction_def.getDbfile_format())) {
                    continue;
                }
                UnloadType unload_type = UnloadType.ofEnumByCode(collectTableBean.getUnload_type());
                if (db.getDbtype() == Dbtype.KINGBASE && UnloadType.QuanLiangXieShu == unload_type) {
                    ResultSetParser parser = new ResultSetParser();
                    String unLoadInfo = parser.copyToFile(db, data_extraction_def, collectTableBean, sql);
                    List<String> unLoadInfoList = StringUtil.split(unLoadInfo, Constant.METAINFOSPLIT);
                    map.put("pageCount", unLoadInfoList.get(unLoadInfoList.size() - 1));
                    unLoadInfoList.remove(unLoadInfoList.size() - 1);
                    map.put("filePathList", unLoadInfoList);
                    return map;
                }
                resultSet = getPageData(db);
                if (resultSet != null) {
                    ResultSetParser parser = new ResultSetParser();
                    String unLoadInfo = parser.parseResultSet(resultSet, collectTableBean, pageNum, tableBean, data_extraction_def, true);
                    if (!StringUtil.isEmpty(unLoadInfo) && unLoadInfo.contains(Constant.METAINFOSPLIT)) {
                        List<String> unLoadInfoList = StringUtil.split(unLoadInfo, Constant.METAINFOSPLIT);
                        map.put("pageCount", unLoadInfoList.get(unLoadInfoList.size() - 1));
                        unLoadInfoList.remove(unLoadInfoList.size() - 1);
                        map.put("filePathList", unLoadInfoList);
                    }
                }
            }
            return map;
        } catch (Exception e) {
            throw new AppSystemException("执行分页卸数程序失败", e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    throw new AppSystemException(e);
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "strategy", desc = "", range = "")
    @Param(name = "strSql", desc = "", range = "")
    @Param(name = "start", desc = "", range = "")
    @Param(name = "end", desc = "", range = "")
    @Return(desc = "", range = "")
    private ResultSet getPageData(DatabaseWrapper db) {
        if (start == 1 && end == Integer.MAX_VALUE) {
            return db.queryGetResultSet(sql);
        } else {
            return db.queryPagedGetResultSet(sql, start, end, false);
        }
    }
}
