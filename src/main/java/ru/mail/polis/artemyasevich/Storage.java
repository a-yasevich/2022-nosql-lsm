package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
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

public class Storage {
    private static final String DATA_FILE = "data";
    private static final String META_FILE = "meta";
    private static final String FILE_EXTENSION = ".txt";
    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private static final OpenOption[] writeOptions = {StandardOpenOption.CREATE, StandardOpenOption.WRITE};

    private final Map<Thread, EntryIOManager> ioManagers;
    private final Deque<StorageFile> storageFiles; //immutable
    private final Path pathToDirectory;
    private final int maxEntrySize;

    private Storage(
            Deque<StorageFile> storageFiles,
            Map<Thread, EntryIOManager> ioManagers,
            Path pathToDirectory,
            int maxEntrySize
    ) {
        this.storageFiles = storageFiles;
        this.ioManagers = ioManagers;
        this.pathToDirectory = pathToDirectory;
        this.maxEntrySize = maxEntrySize;
    }

    static Storage load(Config config) throws IOException {
        Deque<StorageFile> storageFiles = new ArrayDeque<>();
        int maxEntrySize = initFiles(storageFiles, config.basePath());
        int buffersSize = Math.max(maxEntrySize, DEFAULT_BUFFER_SIZE);
        Map<Thread, EntryIOManager> ioManagers = Collections.synchronizedMap(new WeakHashMap<>());
        return new Storage(storageFiles, ioManagers, config.basePath(), buffersSize);
    }

    private Storage newState(Deque<StorageFile> files, StorageFile newFile) {
        files.addFirst(newFile);
        Map<Thread, EntryIOManager> ioManagers;
        int maxEntrySize = this.maxEntrySize;
        if (newFile.maxEntrySize() <= maxEntrySize) {
            ioManagers = this.ioManagers;
        } else {
            ioManagers = Collections.synchronizedMap(new WeakHashMap<>());
            maxEntrySize = newFile.maxEntrySize();
        }
        return new Storage(files, ioManagers, pathToDirectory, maxEntrySize);
    }

    BaseEntry<String> get(String key) throws IOException {
        EntryIOManager entryReader = getEntryIOManager();
        if (key.length() > entryReader.maxPossibleKeyLength()) {
            return null;
        }
        for (StorageFile storageFile : storageFiles) {
            int entryIndex = entryReader.getEntryIndex(key, storageFile);
            if (entryIndex > storageFile.getLastIndex()) {
                continue;
            }
            BaseEntry<String> entry = entryReader.readEntry(storageFile, entryIndex);
            if (entry.key().equals(key)) {
                return entry.value() == null ? null : entry;
            }
        }
        return null;
    }

    Iterator<BaseEntry<String>> iterate(String from, String to) throws IOException {
        List<PeekIterator> peekIterators = new ArrayList<>(storageFiles.size());
        int i = 0;
        for (StorageFile storageFile : storageFiles) {
            peekIterators.add(new PeekIterator(new FileIterator(from, to, storageFile, getEntryIOManager()), i));
            i++;
        }
        return new MergeIterator(peekIterators);
    }

    boolean noNeedForCompact() {
        return storageFiles.size() <= 1 || storageFiles.peek().isCompacted();
    }

    long sizeOfEntry(BaseEntry<String> entry) {
        return getEntryIOManager().sizeOfEntry(entry);
    }

    Storage compact() throws IOException {
        StorageFile compactedFile = saveFile(iterate(null, null), true);
        for (StorageFile storageFile : storageFiles) {
            Files.delete(storageFile.pathToFile());
            Files.delete(storageFile.pathToMeta());
            storageFile.close();
        }
        return newState(new ArrayDeque<>(), compactedFile);
    }

    Storage flush(Iterator<BaseEntry<String>> dataIterator) throws IOException {
        StorageFile newFile = saveFile(dataIterator, false);
        return newState(new ArrayDeque<>(storageFiles), newFile);
    }

    void close() throws IOException {
        for (StorageFile storageFile : storageFiles) {
            storageFile.close();
        }
        storageFiles.clear();
    }

    private int newFileNumber() {
        return storageFiles.isEmpty() ? 0 : extractFileNumber(storageFiles.peek().pathToFile()) + 1;
    }

    private StorageFile saveFile(Iterator<BaseEntry<String>> iterator, boolean fileIsCompacted) throws IOException {
        Path pathToData = pathToData(newFileNumber());
        Path pathToMeta = pathToMeta(newFileNumber());
        saveData(iterator, pathToData, pathToMeta);
        return StorageFile.loadFile(pathToData, pathToMeta, fileIsCompacted);
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
            int curRepeatingSize = entryWriter.writeEntry(dataStream, entry);

            while (dataIterator.hasNext()) {
                entry = dataIterator.next();
                entriesCount++;
                int bytesWritten = entryWriter.writeEntry(dataStream, entry);
                if (bytesWritten == curRepeatingSize) {
                    currentRepeats++;
                    continue;
                }
                metaStream.writeInt(currentRepeats);
                metaStream.writeInt(curRepeatingSize);
                curRepeatingSize = bytesWritten;
                currentRepeats = 1;
            }
            metaStream.writeInt(currentRepeats);
            metaStream.writeInt(curRepeatingSize);
            metaStream.writeInt(entriesCount);
        }
    }

    private EntryIOManager getEntryIOManager() {
        return ioManagers.computeIfAbsent(Thread.currentThread(), thread -> new EntryIOManager(maxEntrySize));
    }

    private static int initFiles(Deque<StorageFile> storageFiles, Path pathToDirectory) throws IOException {
        int maxEntrySize = 0;
        Comparator<Path> comparator = Comparator.comparingInt(Storage::extractFileNumber);
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
            StorageFile storageFile = StorageFile.loadFile(dataFiles.poll(), metaFiles.poll(), false);
            if (storageFile.maxEntrySize() > maxEntrySize) {
                maxEntrySize = storageFile.maxEntrySize();
            }
            storageFiles.addFirst(storageFile);
        }
        return maxEntrySize;
    }

    private static int extractFileNumber(Path path) {
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

}
