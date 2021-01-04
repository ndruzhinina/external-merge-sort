package extsort.dataaccess.in;

import java.io.IOException;
import java.util.List;

public interface IDataReader {
    List<String> readRecords(long maxMemoryBytes) throws IOException;
    String readRecord() throws IOException;

    void close()  throws IOException;

    long getLastMemoryBytes();

    boolean isEOF();
}
