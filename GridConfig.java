public class GridConfig {
    private long size;
    private long psize;
    private String name;

    public long getSize() {
        return this.size;
    }
    public void setSize(long size) {
        this.size = size;
    }

    public long getPSize() {
        return this.psize;
    }
    public void setPSize(long psize) {
        this.psize = psize;
    }

    public String getName() {
        return this.name;
    }
    public void setName(String name) {
        if (name != null || name != "")
            this.name = name;
    }
}
