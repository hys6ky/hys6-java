package hyren.serv6.agent.job.biz.core.jdbcdirectstage.service;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.agent.job.biz.utils.CollectTableBeanUtil;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import lombok.extern.slf4j.Slf4j;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;

@Slf4j
@DocClass(desc = "", author = "zxz")
public class CollectPage implements Callable<Long> {

    private final SourceDataConfBean sourceDataConfBean;

    private final CollectTableBean collectTableBean;

    private final TableBean tableBean;

    private final String sql;

    private final int start;

    private final int end;

    private final DataStoreConfBean dataStoreConfBean;

    public CollectPage(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean, TableBean tableBean, int start, int end, DataStoreConfBean dataStoreConfBean) {
        this.sourceDataConfBean = sourceDataConfBean;
        this.collectTableBean = collectTableBean;
        this.tableBean = tableBean;
        this.start = start;
        this.end = end;
        this.sql = tableBean.getCollectSQL();
        this.dataStoreConfBean = dataStoreConfBean;
    }

    public CollectPage(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean, TableBean tableBean, int start, int end, DataStoreConfBean dataStoreConfBean, String sql) {
        this.sourceDataConfBean = sourceDataConfBean;
        this.collectTableBean = collectTableBean;
        this.tableBean = tableBean;
        this.start = start;
        this.end = end;
        this.sql = sql;
        this.dataStoreConfBean = dataStoreConfBean;
    }

    @Method(desc = "", logicStep = "")
    @Override
    public Long call() {
        sourceDataConfBean.setFetch_size(sourceDataConfBean.getFetch_size() == 0 ? 50 : sourceDataConfBean.getFetch_size());
        ResultSet resultSet = null;
        long start = System.currentTimeMillis();
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(CollectTableBeanUtil.setJdbcBean(sourceDataConfBean))) {
            long rowCount = 0L;
            log.info("执行SQL: {} 查询数据集,开始时间: {} ", sql, DateUtil.getDateTime());
            resultSet = getPageData(db);
            log.info("执行SQL结束时间: {}", DateUtil.getDateTime());
            collectTableBean.setDb_type(db.getDbtype());
            if (resultSet != null) {
                Store_type storeType = Store_type.ofEnumByCode(dataStoreConfBean.getStore_type());
                if (storeType == Store_type.DATABASE) {
                    rowCount = new ParseResultSetToDataBase(resultSet, tableBean, collectTableBean, dataStoreConfBean).parseResultSet();
                } else if (storeType == Store_type.SOLR) {
                    rowCount = new ParseResultSetToSolr(resultSet, tableBean, collectTableBean, dataStoreConfBean).parseResultSet();
                } else {
                    throw new BusinessException(String.format("解析分页数据结果集入存储层的类型: [ %s ] 未实现!", storeType));
                }
            }
            log.info(String.format("===================批量已经结束, 批量插入数据条数: [ %s ] 条, " + "执行耗时: [ %s ] 秒", rowCount, (System.currentTimeMillis() - start) / 1000));
            return rowCount;
        } catch (Exception e) {
            throw new AppSystemException("执行分页卸数程序失败", e);
        } finally {
            if (resultSet != null) {
                try {
                    start = System.currentTimeMillis();
                    log.info("===================rowCount开始关闭" + start);
                    resultSet.close();
                    log.info("===================rowCount关闭结束" + (System.currentTimeMillis() - start));
                } catch (SQLException e) {
                    log.error("关闭ResultSet发生异常! e: {}", e.getMessage(), e);
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
