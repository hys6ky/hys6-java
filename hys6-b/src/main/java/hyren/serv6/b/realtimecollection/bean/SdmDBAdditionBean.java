package hyren.serv6.b.realtimecollection.bean;

import fd.ng.core.annotation.DocBean;
import lombok.Data;

@Data
public class SdmDBAdditionBean {

    @DocBean(name = "field_ch_name", value = "", dataType = String.class)
    private String field_ch_name;

    @DocBean(name = "column_name", value = "", dataType = String.class)
    private String column_name;

    @DocBean(name = "column_type", value = "", dataType = String.class)
    private String column_type;

    @DocBean(name = "dq_remark", value = "", dataType = String.class)
    private String dq_remark;

    @DocBean(name = "dslad_id_s", value = "", dataType = Long[].class)
    private Long[] dslad_id_s;

    @DocBean(name = "csi_number", value = "", dataType = Long[].class)
    private String csi_number;

    @DocBean(name = "is_null", value = "", dataType = String.class, required = true)
    private String is_null;

    @DocBean(name = "is_custom", value = "", dataType = String.class, required = true)
    private String is_custom;
}
