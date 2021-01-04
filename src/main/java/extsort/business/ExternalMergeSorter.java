package extsort.business;

import java.io.File;
import java.io.IOException;
import java.util.*;

import extsort.dataaccess.in.*;
import extsort.dataaccess.out.*;

public class ExternalMergeSorter {

    private String _inFileName;
    private String _outFileName;
    private long _chunkMemorySize;

    private String _chunkFileNameFormatStr;

    private IDataReaderFactory _dataReaderFactory;
    private IDataWriterFactory _dataWriterFactory;

    public ExternalMergeSorter(String inFilename, String outFileName, int chunkMemorySize, IDataReaderFactory dataReaderFactory, IDataWriterFactory dataWriterFactory) {
        _inFileName = inFilename;
        _outFileName = outFileName;
        _chunkMemorySize = chunkMemorySize;
        _dataReaderFactory = dataReaderFactory;
        _dataWriterFactory = dataWriterFactory;
        _chunkFileNameFormatStr = _inFileName + ".chunk_%d" + ".tmp";
    }

    public void run() throws IllegalArgumentException, IOException {
        System.out.println("Starting sorting the file: " + _inFileName);

        IDataReader IDataReader = _dataReaderFactory.CreateForFile(_inFileName);
        int chunkIndex = 0;
        while(!IDataReader.isEOF()) {
            List<String> chunkData = readChunk(IDataReader, chunkIndex);
            sortChunk(chunkData, chunkIndex);
            writeChunk(chunkData, chunkIndex);
            chunkIndex++;
        }

        IDataReader.close();

        if(chunkIndex == 1) {
            System.out.println("All the data fit to a single chunk. Renaming the chunk to " + _outFileName);
            String chunkFileName = String.format(_chunkFileNameFormatStr, 0);
            File file = new File(chunkFileName);
            file.renameTo(new File(_outFileName));
        } else {
            merge(chunkIndex);
            deleteChunks(chunkIndex);
        }

        System.out.println("Finished sorting file: " + _inFileName);
    }

    private List<String> readChunk(IDataReader IDataReader, int chunkIndex) throws IOException {
        System.out.println("Reading chunk #" + chunkIndex);
        List<String> chunkData = IDataReader.readRecords(_chunkMemorySize);
        System.out.println("Reading chunk #" + chunkIndex + " finished. "
                + chunkData.size() + " records read. Size in memory: "
                + IDataReader.getLastMemoryBytes());

        return chunkData;
    }

    private void sortChunk(List<String> chunk, int chunkIndex) {
        System.out.println("Sorting chunk #" + chunkIndex);
        Collections.sort(chunk);
    }

    private void writeChunk(List<String> chunk, int chunkIndex)  throws IOException {
        System.out.println("Writing chunk #" + chunkIndex);

        String chunkFileName = String.format(_chunkFileNameFormatStr, chunkIndex);
        IDataWriter chunkWriter =_dataWriterFactory.CreateForFile(chunkFileName);
        chunkWriter.writeRecords(chunk);
        chunkWriter.close();
    }

    private void merge (int numChunks) throws IOException {
        System.out.println("Merging " + numChunks + " chunks to " + _outFileName + "...");

        PriorityQueue<MergeItem> pq = new PriorityQueue<MergeItem>(numChunks, new MergeItemComparator());
        IDataReader[] chunkReaders = new DataLineReader[numChunks];

        for(int chunkIndex = 0; chunkIndex < numChunks; chunkIndex++) {
            String chunkFileName = String.format(_chunkFileNameFormatStr, chunkIndex);
            IDataReader chunkReader = _dataReaderFactory.CreateForFile(chunkFileName);
            chunkReaders[chunkIndex] = chunkReader;
            pq.add(new MergeItem(chunkReader.readRecord(), chunkIndex));
        }

        IDataWriter dataWriter =_dataWriterFactory.CreateForFile(_outFileName);

        MergeItem currentItem;
        while((currentItem = pq.poll()) != null) {
            dataWriter.writeRecord(currentItem.getValue());
            int chunkNumber = currentItem.getChunkNumber();
            if(!chunkReaders[chunkNumber].isEOF()) {
                String nextRecord = chunkReaders[chunkNumber].readRecord();
                if(nextRecord != null) {
                    MergeItem newItem = new MergeItem(nextRecord, chunkNumber);
                    pq.add(newItem);
                }
            }
        }

        for(int chunkIndex = 0; chunkIndex < numChunks; chunkIndex++) {
            chunkReaders[chunkIndex].close();
        }

        dataWriter.close();
    }

    private void deleteChunks(int numChunks) {
        System.out.println("Deleting chunks ...");

        for(int chunkIndex = 0; chunkIndex < numChunks; chunkIndex++) {
            String chunkFileName = String.format(_chunkFileNameFormatStr, chunkIndex);
            File file = new File(chunkFileName);
            file.delete();
        }
    }
}
