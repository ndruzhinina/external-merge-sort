package extsort.business;

public class MergeItem {
    private String _value;
    private int _chunkNumber;

    MergeItem(String value, int chunkNumber) {
        _value = value;
        _chunkNumber = chunkNumber;
    }

    public String getValue() {
        return _value;
    }

    public int getChunkNumber() {
        return _chunkNumber;
    }
}
