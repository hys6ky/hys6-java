package hyren.serv6.m.vo;

import fd.ng.db.entity.anno.Table;
import hyren.serv6.base.entity.fdentity.ProEntity;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Data
@ApiModel("源系统数据库设置")
@Table(tableName = "database_set")
public class DatabaseSetVo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "database_set";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("database_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", name = "数据库设置id:", dataType = "Long", required = true)
    protected Long database_id;

    @ApiModelProperty(value = "", name = "Agent_id:", dataType = "Long", required = false)
    protected Long agent_id;

    @ApiModelProperty(value = "", name = "主机名:", dataType = "String", required = false)
    protected String host_name;

    @ApiModelProperty(value = "", name = "数据库设置编号:", dataType = "String", required = false)
    protected String database_number;

    @ApiModelProperty(value = "", name = "操作系统类型:", dataType = "String", required = false)
    protected String system_type;

    @ApiModelProperty(value = "", name = "数据库采集任务名称:", dataType = "String", required = false)
    protected String task_name;

    @ApiModelProperty(value = "", name = "数据库名称:", dataType = "String", required = false)
    protected String database_name;

    @ApiModelProperty(value = "", name = "数据库密码:", dataType = "String", required = false)
    protected String database_pad;

    @ApiModelProperty(value = "", name = "数据库驱动:", dataType = "String", required = false)
    protected String database_drive;

    @ApiModelProperty(value = "", name = "fetch_size大小:", dataType = "Integer", required = true)
    protected Integer fetch_size;

    @ApiModelProperty(value = "", name = "数据库类型:", dataType = "String", required = false)
    protected String database_type;

    @ApiModelProperty(value = "", name = "用户名称:", dataType = "String", required = false)
    protected String user_name;

    @ApiModelProperty(value = "", name = "数据库服务器IP:", dataType = "String", required = false)
    protected String database_ip;

    @ApiModelProperty(value = "", name = "数据库端口:", dataType = "String", required = false)
    protected String database_port;

    @ApiModelProperty(value = "", name = "是否DB文件数据采集:(是否标识)", dataType = "String", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String db_agent;

    @ApiModelProperty(value = "", name = "数据采用分隔符:", dataType = "String", required = false)
    protected String database_separatorr;

    @ApiModelProperty(value = "", name = "数据行分隔符:", dataType = "String", required = false)
    protected String row_separator;

    @ApiModelProperty(value = "", name = "DB文件数据字典位置:", dataType = "String", required = false)
    protected String plane_url;

    @ApiModelProperty(value = "", name = "是否设置完成并发送成功:(是否标识)", dataType = "String", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_sendok;

    @ApiModelProperty(value = "", name = "清洗顺序:", dataType = "String", required = false)
    protected String cp_or;

    @ApiModelProperty(value = "", name = "数据库连接地址:", dataType = "String", required = false)
    protected String jdbc_url;

    @ApiModelProperty(value = "", name = "数据库采集方式:(数据库采集方式)", dataType = "String", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String collect_type;

    @ApiModelProperty(value = "", name = "存储层配置ID:", dataType = "Long", required = false)
    protected Long dsl_id;

    @ApiModelProperty(value = "", name = "分类id:", dataType = "Long", required = true)
    protected Long classify_id;

    public void setDatabase_id(String database_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(database_id)) {
            this.database_id = new Long(database_id);
        }
    }

    public void setDatabase_id(Long database_id) {
        this.database_id = database_id;
    }

    public void setAgent_id(String agent_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(agent_id)) {
            this.agent_id = new Long(agent_id);
        }
    }

    public void setAgent_id(Long agent_id) {
        this.agent_id = agent_id;
    }

    public void setFetch_size(String fetch_size) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(fetch_size)) {
            this.fetch_size = new Integer(fetch_size);
        }
    }

    public void setFetch_size(Integer fetch_size) {
        this.fetch_size = fetch_size;
    }

    public void setDsl_id(String dsl_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dsl_id)) {
            this.dsl_id = new Long(dsl_id);
        }
    }

    public void setDsl_id(Long dsl_id) {
        this.dsl_id = dsl_id;
    }

    public void setClassify_id(String classify_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(classify_id)) {
            this.classify_id = new Long(classify_id);
        }
    }

    public void setClassify_id(Long classify_id) {
        this.classify_id = classify_id;
    }
}
