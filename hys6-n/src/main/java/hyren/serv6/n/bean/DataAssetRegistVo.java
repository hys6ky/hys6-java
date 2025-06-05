package hyren.serv6.n.bean;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
public class DataAssetRegistVo {

    @ApiModelProperty(name = "资产id", notes = "", dataType = "long", required = false)
    private long asset_id;

    @ApiModelProperty(name = "目录id", notes = "", dataType = "long", required = false)
    private long dir_id;

    @ApiModelProperty(name = "元数据业务主键", notes = "", dataType = "String", required = false)
    private String mdata_table_id;

    @ApiModelProperty(name = "资产编号", notes = "", dataType = "String", required = false)
    private String asset_code;

    @ApiModelProperty(name = "资产英文名称", notes = "", dataType = "String", required = false)
    private String asset_ename;

    @ApiModelProperty(name = "资产中文名称", notes = "", dataType = "String", required = false)
    private String asset_cname;

    @ApiModelProperty(name = "资产类别", notes = "", dataType = "String", required = false)
    private String asset_type;

    @ApiModelProperty(name = "业务主键", notes = "", dataType = "String", required = false)
    private String business_pk;

    @ApiModelProperty(name = "业务主键中文名", notes = "", dataType = "String", required = false)
    private String business_cname;

    @ApiModelProperty(name = "数据源类型", notes = "", dataType = "String", required = false)
    private String data_source_type;

    @ApiModelProperty(name = "所属主题", notes = "", dataType = "String", required = false)
    private String theme;

    @ApiModelProperty(name = "业务含义", notes = "", dataType = "String", required = false)
    private String business_remark;

    @ApiModelProperty(name = "所属层级", notes = "", dataType = "String", required = false)
    private String layer;

    @ApiModelProperty(name = "存储位置", notes = "", dataType = "String", required = false)
    private String store_path;

    @ApiModelProperty(name = "加工频率", notes = "", dataType = "String", required = false)
    private String process_frequen;

    @ApiModelProperty(name = "加工规则", notes = "", dataType = "String", required = false)
    private String process_rule;

    @ApiModelProperty(name = "技术主键", notes = "", dataType = "String", required = false)
    private String tech_pk;

    @ApiModelProperty(name = "技术主键中文名", notes = "", dataType = "String", required = false)
    private String tech_cname;

    @ApiModelProperty(name = "数据权限英文字段", notes = "", dataType = "String", required = false)
    private String data_auth_code;

    @ApiModelProperty(name = "归属部门", notes = "", dataType = "String", required = false)
    private String belong_depart;

    @ApiModelProperty(name = "归属人", notes = "", dataType = "String", required = false)
    private String belong_by;

    @ApiModelProperty(name = "管理部门", notes = "", dataType = "String", required = false)
    private String manage_depart;

    @ApiModelProperty(name = "管理人", notes = "", dataType = "String", required = false)
    private String manage_by;

    @ApiModelProperty(name = "资产状态", notes = "", dataType = "String", required = false)
    private String asset_status;

    @ApiModelProperty(name = "创建日期", notes = "", dataType = "String", required = false)
    private String create_date;

    @ApiModelProperty(name = "创建时间", notes = "", dataType = "String", required = false)
    private String create_time;

    @ApiModelProperty(name = "盘点人", notes = "", dataType = "String", required = false)
    private String asset_by;

    @ApiModelProperty(name = "盘点日期", notes = "", dataType = "String", required = false)
    private String asset_date;

    @ApiModelProperty(name = "盘点时间", notes = "", dataType = "String", required = false)
    private String asset_time;

    @ApiModelProperty(name = "是否为主数据", notes = "")
    private String is_master_data;

    @ApiModelProperty(name = "数据量", notes = "")
    private Long data_num;
}
