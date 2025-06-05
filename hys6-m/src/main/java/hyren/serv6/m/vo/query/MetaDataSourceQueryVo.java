package hyren.serv6.m.vo.query;

import hyren.serv6.m.entity.MetaDataSource;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
@ApiModel(value = "", description = "")
public class MetaDataSourceQueryVo extends MetaDataSource {

    private Boolean showTask = false;

    @ApiModelProperty(name = "tblTaskQueryVoList", value = "", notes = "", example = "", dataType = "java.util.List<hyren.serv6.m.vo.query.MetaTaskQueryVo>", required = false)
    private List<MetaTaskQueryVo> tblTaskQueryVoList;

    @ApiModelProperty(name = "viewTaskQueryVoList", value = "", notes = "", dataType = "List", required = false)
    private List<MetaTaskQueryVo> viewTaskQueryVoList;

    @ApiModelProperty(name = "meterViewTaskQueryVoList", value = "", notes = "", dataType = "List", required = false)
    private List<MetaTaskQueryVo> meterViewTaskQueryVoList;

    @ApiModelProperty(name = "procTaskQueryVoList", value = "", notes = "", dataType = "List", required = false)
    private List<MetaTaskQueryVo> procTaskQueryVoList;

    @ApiModelProperty(name = "moiTblNum", value = "", notes = "", dataType = "String", required = false)
    private String moiTblNum;

    @ApiModelProperty(name = "moiViewNum", value = "", notes = "", dataType = "String", required = false)
    private String moiViewNum;

    @ApiModelProperty(name = "moiMeterViewNum", value = "", notes = "", dataType = "String", required = false)
    private String moiMeterViewNum;

    @ApiModelProperty(name = "moiProcNum", value = "", notes = "", dataType = "String", required = false)
    private String moiProcNum;

    @ApiModelProperty(name = "dslName", value = "", notes = "", dataType = "String", required = false)
    private String dslName;
}
