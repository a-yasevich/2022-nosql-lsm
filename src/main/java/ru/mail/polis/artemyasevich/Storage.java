package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.CharBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class Storage {
    private static final String DATA_FILE = "data";
    private static final String META_FILE = "meta";
    private static final String FILE_EXTENSION = ".txt";
    private static final int DEFAULT_BUFFER_SIZE = 64;
    private static final OpenOption[] writeOptions = {StandardOpenOption.CREATE, StandardOpenOption.WRITE};
    private final AtomicInteger daoFilesCount = new AtomicInteger();
    private final Map<Thread, EntryReadWriter> entryReadWriter;
    private final Path pathToDirectory;
    private final List<DaoFile> filesToRemove;
    private final Deque<DaoFile> daoFiles;
    private int bufferSize;

    Storage(Config config) throws IOException {
        this.pathToDirectory = config.basePath();
        this.daoFiles = new ConcurrentLinkedDeque<>();
        int maxEntrySize = initFiles();
        this.daoFilesCount.set(daoFiles.size());
        this.filesToRemove = new ArrayList<>();
        this.bufferSize = maxEntrySize == 0 ? DEFAULT_BUFFER_SIZE : maxEntrySize;
        this.entryReadWriter = Collections.synchronizedMap(new WeakHashMap<>());
    }

    BaseEntry<String> get(String key) throws IOException {
        EntryReadWriter entryReader = getEntryReadWriter();
        if (key.length() > entryReader.maxKeyLength()) {
            return null;
        }
        for (DaoFile daoFile : daoFiles) {
            int entryIndex = getEntryIndex(key, daoFile);
            if (entryIndex > daoFile.getLastIndex()) {
                continue;
            }
            BaseEntry<String> entry = entryReader.readEntryFromChannel(daoFile, entryIndex);
            if (entry.key().equals(key)) {
                return entry.value() == null ? null : entry;
            }
            if (daoFile.isCompacted()) {
                break;
            }
        }
        return null;
    }

    Iterator<BaseEntry<String>> iterate(String from, String to) throws IOException {
        List<PeekIterator> peekIterators = new ArrayList<>(daoFiles.size());
        int i = 0;
        for (DaoFile daoFile : daoFiles) {
            int sourceNumber = i;
            peekIterators.add(new PeekIterator(new FileIterator(from, to, daoFile), sourceNumber));
            i++;
            if (daoFile.isCompacted()) {
                break;
            }
        }
        return new MergeIterator(peekIterators);
    }

    void compact() {
        if (daoFiles.peek() == null || daoFiles.peek().isCompacted()) {
            return;
        }
        int number = daoFilesCount.getAndIncrement();
        Path compactedData = pathToData(number);
        Path compactedMeta = pathToMeta(number);
        int sizeBefore = daoFiles.size();
        try {
            savaData(iterate(null, null), compactedData, compactedMeta);
            daoFiles.addFirst(new DaoFile(compactedData, compactedMeta, true));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        for (int i = 0; i < sizeBefore; i++) {
            DaoFile removed = daoFiles.removeLast();
            filesToRemove.add(removed);
        }
        //Теперь все новые запросы get будут идти на новый компакт файл, старые когда-нибудь завершатся
    }

    void flush(Iterator<BaseEntry<String>> dataIterator) throws IOException {
        int number = daoFilesCount.getAndIncrement();
        Path pathToData = pathToData(number);
        Path pathToMeta = pathToMeta(number);
        int maxEntrySize = savaData(dataIterator, pathToData, pathToMeta);

        if (maxEntrySize > bufferSize) {
            entryReadWriter.forEach((key, value) -> value.increaseBufferSize(maxEntrySize));
            bufferSize = maxEntrySize;
        }
        daoFiles.addFirst(new DaoFile(pathToData, pathToMeta, false));
    }

    void close() throws IOException {
        for (DaoFile fileToRemove : filesToRemove) {
            fileToRemove.close();
            Files.delete(fileToRemove.pathToMeta());
            Files.delete(fileToRemove.pathToFile());
        }
        boolean compactedPresent = false;
        Iterator<DaoFile> allFiles = daoFiles.iterator();
        while (allFiles.hasNext()) {
            DaoFile daoFile = allFiles.next();
            daoFile.close();
            if (compactedPresent) {
                Files.delete(daoFile.pathToFile());
                Files.delete(daoFile.pathToMeta());
                allFiles.remove();
            }
            if (daoFile.isCompacted()) {
                compactedPresent = true;
            }
        }
        filesToRemove.clear();
        daoFiles.clear();
    }

    private int savaData(Iterator<BaseEntry<String>> dataIterator,
                         Path pathToData, Path pathToMeta) throws IOException {
        int maxEntrySize = 0;
        try (DataOutputStream dataStream = new DataOutputStream(new BufferedOutputStream(
                Files.newOutputStream(pathToData, writeOptions)));
             DataOutputStream metaStream = new DataOutputStream(new BufferedOutputStream(
                     Files.newOutputStream(pathToMeta, writeOptions)
             ))) {
            EntryReadWriter entryWriter = getEntryReadWriter();
            BaseEntry<String> entry = dataIterator.next();
            int entriesCount = 1;
            int currentRepeats = 1;
            int currentBytes = entryWriter.writeEntryInStream(dataStream, entry);

            while (dataIterator.hasNext()) {
                entry = dataIterator.next();
                entriesCount++;
                int bytesWritten = entryWriter.writeEntryInStream(dataStream, entry);
                if (bytesWritten > maxEntrySize) {
                    maxEntrySize = bytesWritten;
                }
                if (bytesWritten == currentBytes) {
                    currentRepeats++;
                    continue;
                }
                metaStream.writeInt(currentRepeats);
                metaStream.writeInt(currentBytes);
                currentBytes = bytesWritten;
                currentRepeats = 1;
            }
            metaStream.writeInt(currentRepeats);
            metaStream.writeInt(currentBytes);
            metaStream.writeInt(entriesCount);
        }
        return maxEntrySize;
    }

    private int getEntryIndex(String key, DaoFile daoFile) throws IOException {
        EntryReadWriter entryReader = getEntryReadWriter();
        int left = 0;
        int right = daoFile.getLastIndex();
        while (left <= right) {
            int middle = (right - left) / 2 + left;
            CharBuffer middleKey = entryReader.bufferAsKeyOnly(daoFile, middle);
            CharBuffer keyToFind = entryReader.fillAndGetKeyBuffer(key);
            int comparison = keyToFind.compareTo(middleKey);
            if (comparison < 0) {
                right = middle - 1;
            } else if (comparison > 0) {
                left = middle + 1;
            } else {
                return middle;
            }
        }
        return left;
    }

    private EntryReadWriter getEntryReadWriter() {
        return entryReadWriter.computeIfAbsent(Thread.currentThread(), thread -> new EntryReadWriter(bufferSize));
    }

    private int initFiles() throws IOException {
        int maxSize = 0;
        Queue<Path> dataFiles = new PriorityQueue<>();
        Queue<Path> metaFiles = new PriorityQueue<>();
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(pathToDirectory)) {
            for (Path path : paths) {
                String fileName = path.getFileName().toString();
                if (fileName.startsWith(DATA_FILE)) {
                    dataFiles.add(path);
                }
                if (fileName.startsWith(META_FILE)) {
                    metaFiles.add(path);
                }
            }
        }
        Iterator<Path> dataFilesIterator = dataFiles.iterator();
        Iterator<Path> metaFilesIterator = metaFiles.iterator();
        while (dataFilesIterator.hasNext() && metaFilesIterator.hasNext()) {
            Path data = dataFilesIterator.next();
            Path meta = metaFilesIterator.next();
            DaoFile daoFile = new DaoFile(data, meta, false);
            if (daoFile.maxEntrySize() > maxSize) {
                maxSize = daoFile.maxEntrySize();
            }
            daoFiles.addFirst(daoFile);
        }
        return maxSize;
    }

    private Path pathToMeta(int fileNumber) {
        return pathToFile(META_FILE + fileNumber);
    }

    private Path pathToData(int fileNumber) {
        return pathToFile(DATA_FILE + fileNumber);
    }

    private Path pathToFile(String fileName) {
        return pathToDirectory.resolve(fileName + FILE_EXTENSION);
    }

    private class FileIterator implements Iterator<BaseEntry<String>> {
        private final EntryReadWriter entryReader;
        private final DaoFile daoFile;
        private final String to;
        private int entryToRead;
        private BaseEntry<String> next;

        public FileIterator(String from, String to, DaoFile daoFile) throws IOException {
            this.daoFile = daoFile;
            this.to = to;
            this.entryToRead = from == null ? 0 : getEntryIndex(from, daoFile);
            this.entryReader = getEntryReadWriter();
            this.next = getNext();
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public BaseEntry<String> next() {
            BaseEntry<String> nextToGive = next;
            try {
                next = getNext();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return nextToGive;
        }

        private BaseEntry<String> getNext() throws IOException {
            if (daoFile.getOffset(entryToRead) == daoFile.sizeOfFile()) {
                return null;
            }
            BaseEntry<String> entry = entryReader.readEntryFromChannel(daoFile, entryToRead);
            if (to != null && entry.key().compareTo(to) >= 0) {
                return null;
            }
            entryToRead++;
            return entry;
        }
    }
}
