package extsort.dataaccess.out;

import java.io.IOException;
import java.util.List;

public interface IDataWriter {
    void writeRecords (List<String> data) throws IOException;

    void writeRecord(String record) throws  IOException;

    void close() throws  IOException;
}
