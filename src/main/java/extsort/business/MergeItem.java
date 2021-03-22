package extsort.business;

public class MergeItem {
    private String value;
    private int chunkNumber;

    MergeItem(String value, int chunkNumber) {
        this.value = value;
        this.chunkNumber = chunkNumber;
    }

    public String getValue() {
        return value;
    }

    public int getChunkNumber() {
        return chunkNumber;
    }
}
