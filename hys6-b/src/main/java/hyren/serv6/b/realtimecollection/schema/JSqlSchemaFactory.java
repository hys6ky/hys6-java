package hyren.serv6.b.realtimecollection.schema;

import java.util.Map;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.springframework.stereotype.Component;

@Component
public class JSqlSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        return new JSqlSchema(name);
    }
}
