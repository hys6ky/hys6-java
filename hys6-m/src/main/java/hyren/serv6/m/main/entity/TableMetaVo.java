package hyren.serv6.m.main.entity;

import fd.ng.db.meta.ColumnMeta;
import fd.ng.db.meta.TableMeta;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@Data
@Slf4j
public class TableMetaVo extends TableMeta {

    private Map<String, ColumnMetaVo> columnMetasVo;

    public TableMetaVo() {
        this.columnMetasVo = new HashMap(0);
    }

    public TableMetaVo(int columnNums) {
        this.columnMetasVo = new HashMap(columnNums);
    }

    public Map<String, ColumnMetaVo> getColumnMetasVo() {
        return columnMetasVo;
    }

    public void setColumnMetasVo(Map<String, ColumnMetaVo> columnMetasVo) {
        this.columnMetasVo = columnMetasVo;
    }

    public void addColumnMetaVo(ColumnMetaVo columnMeta) {
        String name = columnMeta.getName();
        if (this.columnMetasVo.containsKey(columnMeta.getName())) {
            log.warn("tableName={} already exists", columnMeta.getName());
        }
        this.columnMetasVo.put(columnMeta.getName(), columnMeta);
    }

    public ColumnMetaVo getColumnMetaVo(String colunmName) {
        return this.columnMetasVo.get(colunmName);
    }
}
