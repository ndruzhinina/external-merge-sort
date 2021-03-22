package extsort;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import extsort.business.ExternalMergeSorter;
import extsort.dataaccess.IFileManager;
import extsort.dataaccess.in.IDataReader;
import extsort.dataaccess.in.IDataReaderFactory;
import extsort.dataaccess.out.IDataWriter;
import extsort.dataaccess.out.IDataWriterFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.*;

@ExtendWith(MockitoExtension.class)
public class ExternalMergeSorterTests {

    @Mock
    IDataReaderFactory dataReaderFactoryMock;

    @Mock
    IDataWriterFactory dataWriterFactoryMock;

    @Mock
    IDataReader srcDataReaderMock;

    @Mock
    IDataWriter destDataWriterMock;

    @Mock
    IDataWriter chunk0DataWriterMock;

    @Mock
    IDataWriter chunk1DataWriterMock;

    @Mock
    IDataReader chunk0DataReaderMock;

    @Mock
    IDataReader chunk1DataReaderMock;

    @Mock
    IFileManager fileManagerMock;

    @Test
    void runSortsData() throws IOException {
        String inFileName = "in";
        String outFileName = "out";
        String chunk0 = "chunk0";
        String chunk1 = "chunk1";

        List<String> datasetNames = Arrays.asList(inFileName, outFileName, chunk0, chunk1);

        StorageEmulator storageEmulator = new StorageEmulator(datasetNames);

        configureSrcDataReaderMock(storageEmulator, inFileName);
        configureChunkDataWriterMock(storageEmulator, chunk0DataWriterMock, chunk0);
        configureChunkDataReaderMock(storageEmulator, chunk0DataReaderMock, chunk0);
        configureChunkDataWriterMock(storageEmulator, chunk1DataWriterMock, chunk1);
        configureChunkDataReaderMock(storageEmulator, chunk1DataReaderMock, chunk1);
        configureDestDataWriterMock(storageEmulator, outFileName);
        configureDataReaderFactoryMock(inFileName);
        configureDataWriterFactoryMock(inFileName, outFileName);
        configureFileManagerMock();

        int chunkMemorySize = 100;

        ExternalMergeSorter sorter = new ExternalMergeSorter(
                inFileName,
                outFileName,
                chunkMemorySize,
                dataReaderFactoryMock,
                dataWriterFactoryMock,
                fileManagerMock);

        sorter.run();

        ArrayList<String> result = storageEmulator.data.get(outFileName);
        assertEquals(storageEmulator.data.get(inFileName).size(), result.size());
        assertTrue(storageEmulator.data.get(outFileName).containsAll(storageEmulator.data.get(inFileName)));
        for (int i = 1; i < result.size() - 1; i++) {
            assertTrue(result.get(i - 1).compareTo(result.get(i)) < 0);
        }
    }

    private void configureDataReaderFactoryMock(String inFileName) throws IOException {
        assertNotNull(dataReaderFactoryMock);
        when(dataReaderFactoryMock.CreateForFile(inFileName)).thenReturn(srcDataReaderMock);
        when(dataReaderFactoryMock.CreateForFile(inFileName + ".chunk_0.tmp")).thenReturn(chunk0DataReaderMock);
        when(dataReaderFactoryMock.CreateForFile(inFileName + ".chunk_1.tmp")).thenReturn(chunk1DataReaderMock);

    }

    private void configureDataWriterFactoryMock(String inFileName, String outFileName) throws IOException {
        assertNotNull(dataWriterFactoryMock);
        when(dataWriterFactoryMock.CreateForFile(outFileName)).thenReturn(destDataWriterMock);
        when(dataWriterFactoryMock.CreateForFile(inFileName + ".chunk_0.tmp")).thenReturn(chunk0DataWriterMock);
        when(dataWriterFactoryMock.CreateForFile(inFileName + ".chunk_1.tmp")).thenReturn(chunk1DataWriterMock);
    }

    private void configureSrcDataReaderMock(StorageEmulator storageEmulator, String inFileName) throws IOException {
        assertNotNull(srcDataReaderMock);

        Answer<List<String>> readRecordsAnswer = x -> storageEmulator.fillByRandomStrings(inFileName, 10000);
        doAnswer(readRecordsAnswer).when(srcDataReaderMock).readRecords(anyLong());

        Answer<Boolean> isEofAnswer = x -> (Boolean) storageEmulator.isSrcEof();
        doAnswer(isEofAnswer).when(srcDataReaderMock).isEOF();

        when(srcDataReaderMock.getLastMemoryBytes()).thenReturn((long) 0);
        doNothing().when(srcDataReaderMock).close();
    }

    private void configureDestDataWriterMock(StorageEmulator storageEmulator, String outFileName) throws IOException {
        assertNotNull(destDataWriterMock);

        doAnswer(x -> {
            String record = x.getArgument(0);
            storageEmulator.writeRecord(outFileName, record);
            return null;
        }).when(destDataWriterMock).writeRecord(anyString());

        Answer closeAnswer = x -> {
            storageEmulator.close(outFileName);
            return null;
        };
        doAnswer(closeAnswer).when(destDataWriterMock).close();
    }


    private void configureChunkDataReaderMock(StorageEmulator storageEmulator, IDataReader mock, String datasetName) throws IOException {
        assertNotNull(mock);

        Answer<String> readRecordAnswer = x -> storageEmulator.readRecord(datasetName);
        doAnswer(readRecordAnswer).when(mock).readRecord();

        Answer<Boolean> isEofAnswer = x -> storageEmulator.isEof(datasetName);
        doAnswer(isEofAnswer).when(mock).isEOF();

        Answer closeAnswer = x -> {
            storageEmulator.close(datasetName);
            return null;
        };
        doAnswer(closeAnswer).when(mock).close();
    }


    private void configureChunkDataWriterMock(StorageEmulator storageEmulator, IDataWriter mock, String datasetName) throws IOException {
        assertNotNull(mock);

        Answer writeRecordsAnswer = x -> {
            ArrayList<String> data = x.getArgument(0);
            storageEmulator.writeRecords(datasetName, data);
            return null;
        };
        doAnswer(writeRecordsAnswer).when(mock).writeRecords(any(ArrayList.class));

        Answer closeAnswer = x -> {
            storageEmulator.close(datasetName);
            return null;
        };
        doAnswer(closeAnswer).when(mock).close();
    }

    private void configureFileManagerMock() {
        doNothing().when(fileManagerMock).delete(anyString());
    }


    private class StorageEmulator {
        public HashMap<String, ArrayList<String>> data;
        public HashMap<String, Iterator<String>> iterators;

        public StorageEmulator(List<String> datasetNames) {
            this(datasetNames, 0, 20000, 100);
        }

        public StorageEmulator(List<String> datasetNames, int total, int srcDataSz, int recSz) {
            totalBytes = total;
            srcDataSize = srcDataSz;
            recordSize = recSz;

            data = new HashMap<>();
            iterators = new HashMap<>();

            for (String datasetName : datasetNames) {
                data.put(datasetName, new ArrayList<String>());
                iterators.put(datasetName, data.get(datasetName).iterator());
            }
        }

        private int totalBytes;
        private int srcDataSize;
        private int recordSize;

        public boolean isSrcEof() {
            return totalBytes >= srcDataSize;
        }

        private String getRandomString() {
            int leftLimit = 48; // numeral '0'
            int rightLimit = 122; // letter 'z'
            Random random = new Random();

            String generatedString = random.ints(leftLimit, rightLimit + 1)
                    .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                    .limit(recordSize)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();
            return generatedString;
        }

        public ArrayList<String> fillByRandomStrings(String datasetName, int maxSize) {
            ArrayList<String> dataset = data.get(datasetName);
            ArrayList<String> currentData = new ArrayList<>();

            int size = 0;
            while (size < maxSize && !isSrcEof()) {
                String randomString = getRandomString();
                size += randomString.length();
                totalBytes += randomString.length();
                currentData.add(randomString);
            }

            dataset.addAll(currentData);
            return currentData;
        }

        public void writeRecord(String datasetName, String record) {
            ArrayList<String> dataset = data.get(datasetName);
            dataset.add(record);
        }

        public void writeRecords(String datasetName, ArrayList<String> records) {
            ArrayList<String> dataset = data.get(datasetName);
            for (String record : records) {
                dataset.add(record);
            }
        }

        public String readRecord(String datasetName) {
            Iterator<String> iterator = iterators.get(datasetName);
            return iterator.next();
        }

        public void close(String datasetName) {
            iterators.put(datasetName, data.get(datasetName).iterator());
        }

        public ArrayList<String> readAllRecords(String datasetName) {
            return data.get(datasetName);
        }

        public boolean isEof(String datasetName) {
            return !iterators.get(datasetName).hasNext();
        }
    }
}
