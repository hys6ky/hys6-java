package hyren.serv6.commons.utils.agent.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.core.annotation.DocClass;
import hyren.serv6.base.entity.ColumnSplit;
import java.io.Serializable;
import java.util.List;

@DocClass(desc = "", author = "zxz")
public class ColumnCleanBean implements Serializable {

    private static final long serialVersionUID = 1L;

    @DocBean(name = "col_clean_id", value = "", dataType = Long.class, required = true)
    private Long col_clean_id;

    @DocBean(name = "clean_type", value = "", dataType = String.class, required = true)
    private String clean_type;

    @DocBean(name = "character_filling", value = "", dataType = String.class, required = false)
    private String character_filling;

    @DocBean(name = "filling_length", value = "", dataType = Long.class, required = false)
    private Long filling_length;

    @DocBean(name = "field", value = "", dataType = String.class, required = false)
    private String field;

    @DocBean(name = "replace_feild", value = "", dataType = String.class, required = false)
    private String replace_feild;

    @DocBean(name = "filling_type", value = "", dataType = String.class, required = false)
    private String filling_type;

    @DocBean(name = "convert_format", value = "", dataType = String.class, required = false)
    private String convert_format;

    @DocBean(name = "old_format", value = "", dataType = String.class, required = false)
    private String old_format;

    @DocBean(name = "codeTransform", value = "", dataType = String.class, required = false)
    private String codeTransform;

    @DocBean(name = "column_split", value = "", dataType = ColumnSplit.class, required = false)
    private List<ColumnSplit> column_split_list;

    public Long getCol_clean_id() {
        return col_clean_id;
    }

    public void setCol_clean_id(Long col_clean_id) {
        this.col_clean_id = col_clean_id;
    }

    public String getClean_type() {
        return clean_type;
    }

    public void setClean_type(String clean_type) {
        this.clean_type = clean_type;
    }

    public String getCharacter_filling() {
        return character_filling;
    }

    public void setCharacter_filling(String character_filling) {
        this.character_filling = character_filling;
    }

    public Long getFilling_length() {
        return filling_length;
    }

    public void setFilling_length(Long filling_length) {
        this.filling_length = filling_length;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getReplace_feild() {
        return replace_feild;
    }

    public void setReplace_feild(String replace_feild) {
        this.replace_feild = replace_feild;
    }

    public String getFilling_type() {
        return filling_type;
    }

    public void setFilling_type(String filling_type) {
        this.filling_type = filling_type;
    }

    public String getConvert_format() {
        return convert_format;
    }

    public void setConvert_format(String convert_format) {
        this.convert_format = convert_format;
    }

    public String getOld_format() {
        return old_format;
    }

    public void setOld_format(String old_format) {
        this.old_format = old_format;
    }

    public String getCodeTransform() {
        return codeTransform;
    }

    public void setCodeTransform(String codeTransform) {
        this.codeTransform = codeTransform;
    }

    public List<ColumnSplit> getColumn_split_list() {
        return column_split_list;
    }

    public void setColumn_split_list(List<ColumnSplit> column_split_list) {
        this.column_split_list = column_split_list;
    }
}
