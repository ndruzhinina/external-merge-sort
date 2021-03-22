package extsort.business;

import java.io.File;
import java.io.IOException;
import java.util.*;

import extsort.dataaccess.IFileManager;
import extsort.dataaccess.in.*;
import extsort.dataaccess.out.*;

public class ExternalMergeSorter {

    private String inFileName;
    private String outFileName;
    private long chunkMemorySize;

    private String chunkFileNameFormatStr;

    private IDataReaderFactory dataReaderFactory;
    private IDataWriterFactory dataWriterFactory;
    private IFileManager fileManager;

    public ExternalMergeSorter(String inFilename,
                               String outFileName,
                               int chunkMemorySize,
                               IDataReaderFactory dataReaderFactory,
                               IDataWriterFactory dataWriterFactory,
                               IFileManager fileManager) {
        this.inFileName = inFilename;
        this.outFileName = outFileName;
        this.chunkMemorySize = chunkMemorySize;
        this.dataReaderFactory = dataReaderFactory;
        this.dataWriterFactory = dataWriterFactory;
        chunkFileNameFormatStr = inFileName + ".chunk_%d" + ".tmp";
        this.fileManager = fileManager;
    }

    public void run() throws IllegalArgumentException, IOException {
        System.out.println("Starting sorting the file: " + inFileName);

        IDataReader IDataReader = dataReaderFactory.CreateForFile(inFileName);
        int chunkIndex = 0;
        while (!IDataReader.isEOF()) {
            List<String> chunkData = readChunk(IDataReader, chunkIndex);
            sortChunk(chunkData, chunkIndex);
            writeChunk(chunkData, chunkIndex);
            chunkIndex++;
        }

        IDataReader.close();

        if (chunkIndex == 0) {
            return;
        }

        if (chunkIndex == 1) {
            System.out.println("All the data fit to a single chunk. Renaming the chunk to " + outFileName);
            String chunkFileName = String.format(chunkFileNameFormatStr, 0);
            File file = new File(chunkFileName);
            file.renameTo(new File(outFileName));
        } else {
            merge(chunkIndex);
            deleteChunks(chunkIndex);
        }

        System.out.println("Finished sorting file: " + inFileName);
    }

    private List<String> readChunk(IDataReader dataReader, int chunkIndex) throws IOException {
        System.out.println("Reading chunk #" + chunkIndex);
        List<String> chunkData = dataReader.readRecords(chunkMemorySize);
        System.out.println("Reading chunk #" + chunkIndex + " finished. "
                + chunkData.size() + " records read. Size in memory: "
                + dataReader.getLastMemoryBytes());

        return chunkData;
    }

    private void sortChunk(List<String> chunk, int chunkIndex) {
        System.out.println("Sorting chunk #" + chunkIndex);
        Collections.sort(chunk);
    }

    private void writeChunk(List<String> chunk, int chunkIndex) throws IOException {
        System.out.println("Writing chunk #" + chunkIndex);

        String chunkFileName = String.format(chunkFileNameFormatStr, chunkIndex);
        IDataWriter chunkWriter = dataWriterFactory.CreateForFile(chunkFileName);
        chunkWriter.writeRecords(chunk);
        chunkWriter.close();
    }

    private void merge(int numChunks) throws IOException {
        System.out.println("Merging " + numChunks + " chunks to " + outFileName + "...");

        PriorityQueue<MergeItem> pq = new PriorityQueue<MergeItem>(numChunks, new MergeItemComparator());
        IDataReader[] chunkReaders = new IDataReader[numChunks];

        for (int chunkIndex = 0; chunkIndex < numChunks; chunkIndex++) {
            String chunkFileName = String.format(chunkFileNameFormatStr, chunkIndex);
            IDataReader chunkReader = dataReaderFactory.CreateForFile(chunkFileName);
            chunkReaders[chunkIndex] = chunkReader;
            pq.add(new MergeItem(chunkReader.readRecord(), chunkIndex));
        }

        IDataWriter dataWriter = dataWriterFactory.CreateForFile(outFileName);

        MergeItem currentItem;
        while ((currentItem = pq.poll()) != null) {
            dataWriter.writeRecord(currentItem.getValue());
            int chunkNumber = currentItem.getChunkNumber();
            if (!chunkReaders[chunkNumber].isEOF()) {
                String nextRecord = chunkReaders[chunkNumber].readRecord();
                if (nextRecord != null) {
                    MergeItem newItem = new MergeItem(nextRecord, chunkNumber);
                    pq.add(newItem);
                }
            }
        }

        for (int chunkIndex = 0; chunkIndex < numChunks; chunkIndex++) {
            chunkReaders[chunkIndex].close();
        }

        dataWriter.close();
    }

    private void deleteChunks(int numChunks) {
        System.out.println("Deleting chunks ...");

        for (int chunkIndex = 0; chunkIndex < numChunks; chunkIndex++) {
            String chunkFileName = String.format(chunkFileNameFormatStr, chunkIndex);
            fileManager.delete(chunkFileName);
        }
    }
}
