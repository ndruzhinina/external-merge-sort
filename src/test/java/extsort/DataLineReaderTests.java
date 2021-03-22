package extsort;

import extsort.dataaccess.in.DataLineReader;
import extsort.dataaccess.in.IDataReader;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class DataLineReaderTests {
    @Test
    public void readRecordsReturnsListOfRecords() throws IOException {
        Reader reader = new StringReader("abc\ndef\nghi");

        IDataReader dataLineReader = new DataLineReader(reader);

        assertEquals(Arrays.asList("abc", "def", "ghi"), dataLineReader.readRecords(100));
    }

    @Test
    public void readRecordReturnsString() throws IOException {
        Reader reader = new StringReader("abc\ndef");

        IDataReader dataLineReader = new DataLineReader(reader);

        assertEquals("abc", dataLineReader.readRecord());
        assertEquals("def", dataLineReader.readRecord());
    }

    @Test
    public void readRecordsCountsBytes() throws IOException {
        Reader reader = new StringReader("abc\ndef\nghi");

        IDataReader dataLineReader = new DataLineReader(reader);

        dataLineReader.readRecords(100);
        assertEquals(dataLineReader.getLastMemoryBytes(), 18);
    }

    @Test
    public void readRecordCountsBytes() throws IOException {
        Reader reader = new StringReader("abc\ndef\nghi");
        IDataReader dataLineReader = new DataLineReader(reader);

        dataLineReader.readRecord();
        dataLineReader.readRecord();

        assertEquals(dataLineReader.getLastMemoryBytes(), 6);
    }

    @Test
    public void readRecordsStopsReadingOnReachingByteLimit() throws IOException {
        Reader reader = new StringReader("abc\ndef\nghi");
        IDataReader dataLineReader = new DataLineReader(reader);

        assertEquals(Arrays.asList("abc", "def"), dataLineReader.readRecords(10));
    }

    @Test
    public void closeClosesReader() throws IOException {
        StringReader stringReader = mock(StringReader.class);
        IDataReader dataReader = new DataLineReader(stringReader);

        dataReader.close();

        verify(stringReader, times(1)).close();
    }

    @Test
    public void readRecordsDetectsEndOfFile() throws IOException {
        Reader reader = new StringReader("abc\ndef\nghi");
        IDataReader dataLineReader = new DataLineReader(reader);

        dataLineReader.readRecords(10);
        assertEquals(dataLineReader.isEOF(), false);
        dataLineReader.readRecords(10);
        assertEquals(dataLineReader.isEOF(), true);
    }

    @Test
    public void readRecordDetectsEndOfFile() throws IOException {
        Reader reader = new StringReader("abc\ndef");
        IDataReader dataLineReader = new DataLineReader(reader);

        dataLineReader.readRecord();
        assertEquals(dataLineReader.isEOF(), false);
        dataLineReader.readRecord();
        assertEquals(dataLineReader.isEOF(), false);
        dataLineReader.readRecord();
        assertEquals(dataLineReader.isEOF(), true);
    }
}
