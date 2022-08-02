package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class Storage {
    private static final String DATA_FILE = "data";
    private static final String META_FILE = "meta";
    private static final String FILE_EXTENSION = ".txt";
    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private static final OpenOption[] writeOptions = {StandardOpenOption.CREATE, StandardOpenOption.WRITE};

    private final Map<Thread, EntryIOManager> IOManagers = Collections.synchronizedMap(new WeakHashMap<>());
    private final List<DaoFile> filesToRemove = new ArrayList<>();
    private final Deque<DaoFile> daoFiles = new ConcurrentLinkedDeque<>();
    private final Path pathToDirectory;
    private final int initialBufferSize;
    private int daoFilesCount;

    Storage(Config config) throws IOException {
        this.pathToDirectory = config.basePath();
        int maxEntrySize = initFiles();
        this.initialBufferSize = maxEntrySize == 0 ? DEFAULT_BUFFER_SIZE : maxEntrySize;
    }

    BaseEntry<String> get(String key) throws IOException {
        EntryIOManager entryReader = getEntryIOManager();
        if (key.length() > entryReader.maxPossibleKeyLength()) {
            return null;
        }
        for (DaoFile daoFile : daoFiles) {
            int entryIndex = entryReader.getEntryIndex(key, daoFile);
            if (entryIndex > daoFile.getLastIndex()) {
                continue;
            }
            BaseEntry<String> entry = entryReader.readEntry(daoFile, entryIndex);
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
            peekIterators.add(new PeekIterator(new FileIterator(from, to, daoFile), i));
            i++;
            if (daoFile.isCompacted()) {
                break;
            }
        }
        return new MergeIterator(peekIterators);
    }

    void compact() throws IOException {
        if (daoFiles.size() <= 1 || daoFiles.peek().isCompacted()) {
            return;
        }
        Path compactedData = pathToData(daoFilesCount);
        Path compactedMeta = pathToMeta(daoFilesCount);
        daoFilesCount++;
        int sizeBefore = daoFiles.size();
        saveData(iterate(null, null), compactedData, compactedMeta);
        daoFiles.addFirst(DaoFile.loadFile(compactedData, compactedMeta, true));
        for (int i = 0; i < sizeBefore; i++) {
            DaoFile removed = daoFiles.removeLast();
            filesToRemove.add(removed);
        }
        //Теперь все новые запросы get будут идти на новый компакт файл, старые продолжают работу
    }

    void flush(Iterator<BaseEntry<String>> dataIterator) throws IOException {
        Path pathToData = pathToData(daoFilesCount);
        Path pathToMeta = pathToMeta(daoFilesCount);
        daoFilesCount++;
        saveData(dataIterator, pathToData, pathToMeta);
        daoFiles.addFirst(DaoFile.loadFile(pathToData, pathToMeta, false));
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

    private void saveData(Iterator<BaseEntry<String>> dataIterator,
                          Path pathToData, Path pathToMeta) throws IOException {
        try (FileChannel dataStream = FileChannel.open(pathToData, writeOptions);
             DataOutputStream metaStream = new DataOutputStream(new BufferedOutputStream(
                     Files.newOutputStream(pathToMeta, writeOptions)
             ))) {
            EntryIOManager entryWriter = getEntryIOManager();
            BaseEntry<String> entry = dataIterator.next();
            int entriesCount = 1;
            int currentRepeats = 1;
            int currentBytes = entryWriter.writeEntry(dataStream, entry);

            while (dataIterator.hasNext()) {
                entry = dataIterator.next();
                entriesCount++;
                int bytesWritten = entryWriter.writeEntry(dataStream, entry);
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
    }

    private EntryIOManager getEntryIOManager() {
        return IOManagers.computeIfAbsent(Thread.currentThread(), thread -> new EntryIOManager(initialBufferSize));
    }

    private int initFiles() throws IOException {
        int maxSize = 0;
        Comparator<Path> comparator = Comparator.comparingInt(this::extractFileNumber);
        Queue<Path> dataFiles = new PriorityQueue<>(comparator);
        Queue<Path> metaFiles = new PriorityQueue<>(comparator);
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
        if (dataFiles.size() > metaFiles.size()) {
            Files.delete(dataFiles.poll());
        } else if (metaFiles.size() > dataFiles.size()) {
            Files.delete(metaFiles.poll());
        }
        while (!dataFiles.isEmpty() && !metaFiles.isEmpty()) {
            DaoFile daoFile = DaoFile.loadFile(dataFiles.poll(), metaFiles.poll(), false);
            if (daoFile.maxEntrySize() > maxSize) {
                maxSize = daoFile.maxEntrySize();
            }
            daoFiles.addFirst(daoFile);
        }
        if (!daoFiles.isEmpty()) {
            this.daoFilesCount = extractFileNumber(daoFiles.peekFirst().pathToFile()) + 1;
        }
        return maxSize;
    }

    private int extractFileNumber(Path path) {
        String fileName = path.getFileName().toString();
        return Integer.parseInt(fileName.substring(DATA_FILE.length(), fileName.length() - FILE_EXTENSION.length()));
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
        private final EntryIOManager entryReader;
        private final DaoFile daoFile;
        private final String to;
        private int entryToRead;
        private BaseEntry<String> next;

        public FileIterator(String from, String to, DaoFile daoFile) throws IOException {
            this.daoFile = daoFile;
            this.to = to;
            this.entryReader = getEntryIOManager();
            this.entryToRead = from == null ? 0 : entryReader.getEntryIndex(from, daoFile);
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
            BaseEntry<String> entry = entryReader.readEntry(daoFile, entryToRead);
            if (to != null && entry.key().compareTo(to) >= 0) {
                return null;
            }
            entryToRead++;
            return entry;
        }
    }

}
