package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StringDao implements Dao<String, BaseEntry<String>> {
    private final Config config;
    private final ReadWriteLock upsertLock = new ReentrantReadWriteLock();
    private final Lock storageLock = new ReentrantLock();
    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "StringDaoBg"));
    private final AtomicBoolean autoFlushing = new AtomicBoolean();
    private volatile State state;

    public StringDao(Config config) throws IOException {
        this.config = config;
        this.state = State.newState(config);
    }

    public StringDao() {
        config = null;
        state = State.newState();
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
        State memory = this.state;
        List<PeekIterator> iterators = new ArrayList<>(3);
        if (to != null && to.equals(from)) {
            return Collections.emptyIterator();
        }
        if (!memory.memory.isEmpty()) {
            iterators.add(new PeekIterator(memoryIterator(from, to, memory.memory), 0));
        }
        if (!memory.flushing.isEmpty()) {
            iterators.add(new PeekIterator(memoryIterator(from, to, memory.flushing), 1));
        }
        if (state.storage != null) {
            iterators.add(new PeekIterator(state.storage.iterate(from, to), 2));
        }
        return new MergeIterator(iterators);
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        State state = this.state;
        BaseEntry<String> entry;
        entry = state.memory.get(key);
        if (entry == null && !state.flushing.isEmpty()) {
            entry = state.flushing.get(key);
        }
        if (entry == null && state.storage != null) {
            entry = state.storage.get(key);
        }
        return entry == null || entry.value() == null ? null : entry;
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        if (config == null || config.flushThresholdBytes() == 0) {
            state.memory.put(entry.key(), entry);
            return;
        }
        State state = this.state;
        upsertLock.readLock().lock();
        try {
            BaseEntry<String> previous = state.memory.get(entry.key());
            long previousSize = previous == null ? 0 : state.storage.sizeOfEntry(previous);
            long entrySizeDelta = state.storage.sizeOfEntry(entry) - previousSize;
            long currentMemoryUsage = state.memoryUsage.addAndGet(entrySizeDelta);
            if (currentMemoryUsage > config.flushThresholdBytes()) {
                if (currentMemoryUsage > config.flushThresholdBytes() * 2) {
                    throw new IllegalStateException("Memory is full");
                }
                if (!autoFlushing.getAndSet(true)) {
                    executor.submit(this::flushMemory);
                }
            }
            state.memory.put(entry.key(), entry);
        } finally {
            upsertLock.readLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        if (state.storage == null) {
            return;
        }
        executor.submit(() -> {
            State state = this.state;
            storageLock.lock();
            try {
                if(state.storage.noNeedForCompact()){
                    return;
                }
                Storage storage = state.storage.compact();
                upsertLock.writeLock();
                try {
                    this.state = state.afterCompact(storage);
                } finally {
                    upsertLock.writeLock().unlock();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                storageLock.unlock();
            }
        });
    }

    @Override
    public void flush() throws IOException {
        flushMemory();
    }

    @Override
    public void close() throws IOException {
        State state = this.state;
        if (state.storage == null) {
            return;
        }
        executor.shutdown();
        try {
            boolean terminated;
            do {
                terminated = executor.awaitTermination(1, TimeUnit.DAYS);
            } while (!terminated);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        flushMemory();
        state.storage.close();
    }

    private void flushMemory() {
        if (state.storage == null) {
            return;
        }
        storageLock.lock();
        try {
            upsertLock.writeLock().lock();
            try {
                this.state = state.prepareForFlush();
            } finally {
                upsertLock.writeLock().unlock();
            }
            if (this.state.flushing.isEmpty()) {
                return;
            }
            System.out.println(state);
            Storage storage = state.storage.flush(this.state.flushing.values().iterator());
            upsertLock.writeLock().lock();
            try {
                this.state = state.afterFlush(storage);
            } finally {
                upsertLock.writeLock().unlock();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            autoFlushing.set(false);
            storageLock.unlock();
        }
    }

    private Iterator<BaseEntry<String>> memoryIterator(String from, String to,
                                                       NavigableMap<String, BaseEntry<String>> map) {
        Map<String, BaseEntry<String>> subMap;
        if (from == null && to == null) {
            subMap = map;
        } else if (from == null) {
            subMap = map.headMap(to);
        } else if (to == null) {
            subMap = map.tailMap(from);
        } else {
            subMap = map.subMap(from, to);
        }
        return subMap.values().iterator();
    }

    private static class State {
        final ConcurrentNavigableMap<String, BaseEntry<String>> memory;
        final ConcurrentNavigableMap<String, BaseEntry<String>> flushing;

        final Storage storage;
        final AtomicLong memoryUsage;

        private State(ConcurrentNavigableMap<String, BaseEntry<String>> memory,
                      ConcurrentNavigableMap<String, BaseEntry<String>> flushing, Storage storage) {
            this.memory = memory;
            this.flushing = flushing;
            this.storage = storage;
            this.memoryUsage = new AtomicLong();
        }

        private State(ConcurrentNavigableMap<String, BaseEntry<String>> memory,
                      ConcurrentNavigableMap<String, BaseEntry<String>> flushing,
                      Storage storage, AtomicLong memoryUsage) {
            this.memory = memory;
            this.flushing = flushing;
            this.storage = storage;
            this.memoryUsage = memoryUsage;
        }

        static State newState(Config config) throws IOException {
            Storage storage = Storage.load(config);
            return new State(new ConcurrentSkipListMap<>(), new ConcurrentSkipListMap<>(), storage);
        }

        static State newState() {
            return new State(new ConcurrentSkipListMap<>(), new ConcurrentSkipListMap<>(), null);
        }

        State prepareForFlush() {
            return new State(new ConcurrentSkipListMap<>(), memory, storage, memoryUsage);
        }

        State afterFlush(Storage afterFlushStorage) {
            return new State(memory, new ConcurrentSkipListMap<>(), afterFlushStorage);
        }

        State afterCompact(Storage afterCompactStorage) {
            return new State(memory, flushing, afterCompactStorage, memoryUsage);
        }

    }

}
