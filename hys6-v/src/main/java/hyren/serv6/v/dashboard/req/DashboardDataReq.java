package hyren.serv6.v.dashboard.req;

import hyren.serv6.base.entity.AutoDashboardInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DashboardDataReq {

    private List<Object> widget;

    private Dashboard dashboard;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private class Dashboard {

        private Long id;

        private String title;

        private Long width;

        private Long height;

        private String description;

        private String backgroundColor;

        private String backgroundImage;

        private String[] presetLine;

        private String presetLineVisible;

        private String create_date;

        private String create_time;

        private String dashboard_status;

        private String user_id;
    }

    public AutoDashboardInfo getAutoDashboardInfo() {
        AutoDashboardInfo autoDashboardInfo = new AutoDashboardInfo();
        if (dashboard.getId() != null) {
            autoDashboardInfo.setDashboard_id(dashboard.getId());
        }
        autoDashboardInfo.setDashboard_name(dashboard.getTitle());
        autoDashboardInfo.setDashboard_desc(dashboard.getDescription());
        autoDashboardInfo.setBackground(dashboard.getBackgroundColor());
        autoDashboardInfo.setDashboard_theme(dashboard.getBackgroundImage());
        autoDashboardInfo.setBorder_width(dashboard.getWidth().toString());
        autoDashboardInfo.setBorder_height(dashboard.getHeight().toString());
        autoDashboardInfo.setUser_id(dashboard.getUser_id());
        autoDashboardInfo.setDashboard_status(dashboard.getDashboard_status());
        autoDashboardInfo.setCreate_date(dashboard.getCreate_date());
        autoDashboardInfo.setCreate_time(dashboard.create_time);
        return autoDashboardInfo;
    }

    public void setAutoDashboardInfo(Map<String, Object> map) {
        Dashboard dashboard = new Dashboard();
        if (map != null && !map.isEmpty()) {
            dashboard.setId(Long.parseLong(map.get("dashboard_id").toString()));
            dashboard.setTitle(map.get("dashboard_name").toString());
            dashboard.setWidth(Long.parseLong(map.get("border_width").toString()));
            dashboard.setHeight(Long.parseLong(map.get("border_height").toString()));
            dashboard.setDescription(map.get("dashboard_desc").toString());
            dashboard.setBackgroundColor(map.get("background").toString());
            dashboard.setBackgroundImage(map.get("dashboard_theme").toString());
            dashboard.setPresetLine(new String[1]);
            dashboard.setPresetLineVisible("");
        }
        this.dashboard = dashboard;
    }
}
