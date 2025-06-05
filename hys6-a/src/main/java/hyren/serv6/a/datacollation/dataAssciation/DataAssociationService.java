package hyren.serv6.a.datacollation.dataAssciation;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.meta.MetaOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.SolrDataRelation;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.solr.ISolrOperator;
import hyren.serv6.commons.solr.factory.SolrFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Set;

@Slf4j
@Service("dataAssociationService")
public class DataAssociationService {

    @Method(desc = "", logicStep = "")
    @Param(name = "relationTableName", desc = "", range = "")
    public void executeSolrDataAssociation(String relationTableName) {
        if (StringUtil.isBlank(relationTableName))
            throw new BusinessException("关联表名不能为空!");
        try (DatabaseWrapper db = Dbo.db()) {
            if (!db.isExistTable(relationTableName))
                throw new BusinessException("待关联的表在配置库中不存在");
            Set<String> columnNames = MetaOperator.getTablesWithColumns(db, relationTableName).get(0).getColumnNames();
            columnNames.remove("file_id");
            Result result = Dbo.queryResult("select * from " + relationTableName);
            try (ISolrOperator os = SolrFactory.getSolrOperatorInstance()) {
                os.parseResultToSolr(result, columnNames);
            } catch (Exception e) {
                throw new BusinessException("Solr数据关联表失败! " + e);
            }
            saveRelationTable(columnNames);
        } catch (Exception e) {
            throw new BusinessException("Solr数据关联表失败! " + e);
        }
    }

    private void saveRelationTable(Set<String> columnNames) {
        try {
            List<String> field_name_s = Dbo.queryOneColumnList("select field_name from solr_data_relation");
            for (String columnName : columnNames) {
                if (!field_name_s.contains(columnName)) {
                    SolrDataRelation sdr = new SolrDataRelation();
                    sdr.setField_name(columnName);
                    if (1 != sdr.add(Dbo.db())) {
                        throw new BusinessException("入 Solr_data_relation 错误! 列名为：" + columnName);
                    }
                }
            }
        } catch (Exception e) {
            log.error("入 Solr_data_relation 失败 ！！", e);
        }
    }
}
