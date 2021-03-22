package extsort;

import extsort.dataaccess.out.DataLineWriter;
import extsort.dataaccess.out.IDataWriter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class DataLineWriterTests {

    @Test
    public void WriteRecordWritesLine() throws IOException {
        StringWriter stringWriter = new StringWriter();
        IDataWriter dataWriter = new DataLineWriter(stringWriter);
        String record = "abc";

        dataWriter.writeRecord(record);

        assertEquals(stringWriter.toString(), record + System.lineSeparator());
    }

    @Test
    public void WriteRecordsWritesLines() throws IOException {
        StringWriter stringWriter = new StringWriter();
        IDataWriter dataWriter = new DataLineWriter(stringWriter);
        List<String> records = Arrays.asList("abc", "def", "ghi");

        dataWriter.writeRecords(records);

        String expected = String.join(System.lineSeparator(), records) + System.lineSeparator();
        assertEquals(stringWriter.toString(), expected);
    }

    @Test
    public void closeClosesWriter() throws IOException {
        StringWriter stringWriter = mock(StringWriter.class);
        IDataWriter dataWriter = new DataLineWriter(stringWriter);

        dataWriter.close();

        verify(stringWriter, times(1)).close();
    }
}
