package hyren.serv6.v.operate.req;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class VisualComponentInfoReq {

    private String componentBeanString;

    private String auto_comp_sumString;

    private String autoCompCondString;

    private String autoCompGroupString;

    private String autoCompDataSumString;

    private String titleFontString;

    private String axisStyleFontString;

    private String autoAxisInfoString;

    private String xAxisLabelString;

    private String yAxisLabelString;

    private String xAxisLineString;

    private String yAxisLineString;

    private String auto_table_infoString;

    private String auto_chartsconfigString;

    private String auto_labelString;

    private String auto_legend_infoString;
}
