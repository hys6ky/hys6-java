package hyren.serv6.b.realtimecollection.schema;

import java.util.HashMap;
import java.util.Map;
import hyren.serv6.b.realtimecollection.common.JSqlTable;
import hyren.serv6.commons.utils.stream.common.JSqlMapData;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import com.google.common.collect.Multimap;

public class JSqlSchema extends AbstractSchema {

    private String dbName;

    public JSqlSchema(String name) {
        this.dbName = name;
    }

    @Override
    public boolean isMutable() {
        return super.isMutable();
    }

    @Override
    public boolean contentsHaveChangedSince(long lastCheck, long now) {
        return super.contentsHaveChangedSince(lastCheck, now);
    }

    @Override
    public Expression getExpression(SchemaPlus parentSchema, String name) {
        return super.getExpression(parentSchema, name);
    }

    @Override
    protected Multimap<String, Function> getFunctionMultimap() {
        return super.getFunctionMultimap();
    }

    @Override
    protected Map<String, Schema> getSubSchemaMap() {
        return super.getSubSchemaMap();
    }

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tables = new HashMap<String, Table>();
        JSqlMapData.Database database = JSqlMapData.MAP.get(this.dbName);
        if (database == null)
            return tables;
        for (JSqlMapData.Table table : database.tables) {
            tables.put(table.tableName, new JSqlTable(table));
        }
        return tables;
    }
}
