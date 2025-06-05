package hyren.serv6.k.scrap.tdbresult.graph;

public class GraphNode {

    private String id;

    private String name;

    private Double symbolSize;

    private Double x;

    private Double y;

    private String value;

    private Integer category;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getSymbolSize() {
        return symbolSize;
    }

    public void setSymbolSize(Double symbolSize) {
        this.symbolSize = symbolSize;
    }

    public Double getX() {
        return x;
    }

    public void setX(Double x) {
        this.x = x;
    }

    public Double getY() {
        return y;
    }

    public void setY(Double y) {
        this.y = y;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Integer getCategory() {
        return category;
    }

    public void setCategory(Integer category) {
        this.category = category;
    }
}
