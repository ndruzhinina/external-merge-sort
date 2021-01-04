package extsort.dataaccess.out;

import java.io.IOException;

public interface IDataWriterFactory {
    IDataWriter CreateForFile(String fileName) throws IOException;
}
