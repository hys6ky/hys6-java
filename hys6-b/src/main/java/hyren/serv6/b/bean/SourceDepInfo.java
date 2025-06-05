package hyren.serv6.b.bean;

import fd.ng.db.entity.anno.Table;
import hyren.serv6.base.entity.DataSource;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
@ApiModel("数据源与部门信息表")
@Table(tableName = "source_dep_info")
public class SourceDepInfo {

    @ApiModelProperty(value = "", name = "数据源信息实体对象:", dataType = "DataSource", required = true)
    private DataSource dataSource;

    @ApiModelProperty(value = "", name = "数据源信息实体对象:", dataType = "Long[]", required = true)
    private Long[] depIds;
}
