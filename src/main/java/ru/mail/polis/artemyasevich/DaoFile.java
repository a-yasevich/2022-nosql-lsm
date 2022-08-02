package ru.mail.polis.artemyasevich;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class DaoFile {
    private final long[] offsets;
    private final RandomAccessFile reader;
    private final long size;
    private final int entries;
    private final boolean isCompacted;
    private final Path pathToFile;
    private final Path pathToMeta;
    private final int maxEntrySize;

    private DaoFile(Path pathToFile, Path pathToMeta, Meta meta, boolean isCompacted)
            throws FileNotFoundException {
        this.pathToFile = pathToFile;
        this.offsets = meta.offsets;
        this.size = meta.fileSize();
        this.maxEntrySize = meta.maxEntrySize;
        this.entries = meta.entriesCount();
        this.isCompacted = isCompacted;
        this.reader = new RandomAccessFile(pathToFile.toFile(), "r");
        this.pathToMeta = pathToMeta;
    }

    public static DaoFile loadFile(Path pathToFile, Path pathToMeta, boolean isCompacted) throws IOException {
        Meta meta = processMetafile(pathToMeta);
        return new DaoFile(pathToFile, pathToMeta, meta, isCompacted);
    }


    public boolean isCompacted() {
        return isCompacted;
    }

    public FileChannel getChannel() {
        return reader.getChannel();
    }

    public int entrySize(int index) {
        return (int) (offsets[index + 1] - offsets[index]);
    }

    public long sizeOfFile() {
        return size;
    }

    public int maxEntrySize() {
        return maxEntrySize;
    }

    //Returns fileSize if index == entries count
    public long getOffset(int index) {
        return offsets[index];
    }

    public void close() throws IOException {
        reader.close();
    }

    public int getLastIndex() {
        return entries - 1;
    }

    private static Meta processMetafile(Path pathToMeta) throws IOException {
        long[] fileOffsets;
        int maxEntry;
        try (RandomAccessFile metaReader = new RandomAccessFile(pathToMeta.toFile(), "r")) {
            long metaFileSize = metaReader.length();
            metaReader.seek(metaFileSize - Integer.BYTES);
            int entriesTotal = metaReader.readInt();
            fileOffsets = new long[entriesTotal + 1];
            fileOffsets[0] = 0;
            metaReader.seek(0);
            int i = 1;
            maxEntry = 0;
            long currentOffset = 0;
            while (metaReader.getFilePointer() != metaFileSize - Integer.BYTES) {
                int numberOfEntries = metaReader.readInt();
                int entryBytesSize = metaReader.readInt();
                if (entryBytesSize > maxEntry) {
                    maxEntry = entryBytesSize;
                }
                for (int j = 0; j < numberOfEntries; j++) {
                    currentOffset += entryBytesSize;
                    fileOffsets[i] = currentOffset;
                    i++;
                }
            }
        }
        return new Meta(fileOffsets, maxEntry);
    }

    public Path pathToFile() {
        return pathToFile;
    }

    public Path pathToMeta() {
        return pathToMeta;
    }

    private record Meta(long[] offsets, int maxEntrySize) {
        public long fileSize() {
            return offsets[offsets.length - 1];
        }

        public int entriesCount() {
            return offsets.length - 1;
        }
    }

}
