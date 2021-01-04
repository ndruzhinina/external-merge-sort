package extsort.dataaccess.in;

import java.io.IOException;

public interface IDataReaderFactory {
    IDataReader CreateForFile(String fileName) throws IOException;
}
