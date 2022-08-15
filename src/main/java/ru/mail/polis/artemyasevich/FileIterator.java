package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;

public class FileIterator implements Iterator<BaseEntry<String>> {
    private final EntryIOManager entryReader;
    private final StorageFile storageFile;
    private final String to;
    private int entryToRead;
    private BaseEntry<String> next;

    public FileIterator(String from, String to, StorageFile storageFile, EntryIOManager entryReader) throws IOException {
        this.storageFile = storageFile;
        this.to = to;
        this.entryReader = entryReader;
        this.entryToRead = from == null ? 0 : entryReader.getEntryIndex(from, storageFile);
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
        if (storageFile.getOffset(entryToRead) == storageFile.sizeOfFile()) {
            return null;
        }
        BaseEntry<String> entry = entryReader.readEntry(storageFile, entryToRead);
        if (to != null && entry.key().compareTo(to) >= 0) {
            return null;
        }
        entryToRead++;
        return entry;
    }
}
