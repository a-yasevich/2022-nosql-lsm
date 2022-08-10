package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

public class EntryIOManager {
    private final CharsetEncoder encoder;
    private final CharsetDecoder decoder;
    private final EntryMeta entryMeta = new EntryMeta();
    private BuffersTriple buffersTriple;

    EntryIOManager(int bufferSize) {
        this.buffersTriple = BuffersTriple.newTriple(bufferSize);
        this.encoder = StandardCharsets.UTF_8.newEncoder();
        this.decoder = StandardCharsets.UTF_8.newDecoder();
    }

    long sizeOfEntry(BaseEntry<String> entry) {
        increaseBuffersIfNeeded(sizeAtWorst(entry));
        BuffersTriple bs = buffersTriple;
        return writeEntryInBuffer(entry, bs.buffer, bs.charBuffer);
    }

    private void increaseBuffersIfNeeded(int entrySize) {
        if (entrySize <= buffersTriple.capacity()) {
            return;
        }
        int newCapacity = (int) (entrySize * 1.5);
        buffersTriple = BuffersTriple.newTriple(newCapacity);
    }

    int maxPossibleKeyLength() {
        return buffersTriple.capacity();
    }

    //[key|value|keySize|valuePresent] or [key|keySize|valueNotPresent] if value == null
    int writeEntry(FileChannel channel, BaseEntry<String> entry) throws IOException {
        increaseBuffersIfNeeded(sizeAtWorst(entry));
        BuffersTriple bs = buffersTriple;
        writeEntryInBuffer(entry, bs.buffer, bs.charBuffer);
        return channel.write(bs.buffer);
    }

    BaseEntry<String> readEntry(StorageFile storageFile, int index) throws IOException {
        BuffersTriple bs = buffersTriple;
        readEntryFromChannel(storageFile, index, bs.buffer);
        return entryFromBuffer(bs.buffer, bs.charBuffer, storageFile.entrySize(index));
    }

    private void readKeyOnly(StorageFile storageFile, int index, ByteBuffer buffer, CharBuffer storeWhere) throws IOException {
        readEntryFromChannel(storageFile, index, buffer);
        EntryMeta meta = entryMetaFromBuffer(buffer);
        decodeKey(buffer, storeWhere, meta.keySize);
    }

    int getEntryIndex(String key, StorageFile storageFile) throws IOException {
        BuffersTriple bs = buffersTriple;
        putStringInChBuffer(bs.searchedKeyBuffer, key);
        int left = 0;
        int right = storageFile.getLastIndex();
        while (left <= right) {
            int middle = (right - left) / 2 + left;
            readKeyOnly(storageFile, middle, bs.buffer, bs.charBuffer);
            int comparison = bs.searchedKeyBuffer.compareTo(bs.charBuffer);
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

    private void readEntryFromChannel(StorageFile storageFile, int index, ByteBuffer buffer) throws IOException {
        buffer.clear();
        buffer.limit(storageFile.entrySize(index));
        storageFile.getChannel().read(buffer, storageFile.getOffset(index));
        buffer.flip();
    }

    private BaseEntry<String> entryFromBuffer(ByteBuffer decodeFrom, CharBuffer storeWhere, int entrySize) {
        EntryMeta meta = entryMetaFromBuffer(decodeFrom);
        decodeKey(decodeFrom, storeWhere, meta.keySize);
        String key = storeWhere.toString();
        if (!meta.valuePresent) {
            return new BaseEntry<>(key, null);
        }
        decodeValue(decodeFrom, storeWhere, meta, entrySize);
        String value = storeWhere.toString();
        return new BaseEntry<>(key, value);
    }

    private int writeEntryInBuffer(BaseEntry<String> entry, ByteBuffer buffer, CharBuffer charBuffer) {
        buffer.clear();
        encode(entry.key(), charBuffer, buffer);
        int keySize = buffer.position();
        boolean valuePresent = entry.value() != null;
        if (valuePresent) {
            encode(entry.value(), charBuffer, buffer);
        }
        buffer.putShort((short) keySize);
        buffer.put((byte) (valuePresent ? 1 : 0));
        buffer.flip();
        return buffer.limit();
    }


    private EntryMeta entryMetaFromBuffer(ByteBuffer buffer) {
        buffer.position(buffer.limit() - EntryMeta.META_SIZE);
        EntryMeta meta = entryMeta;
        meta.keySize = buffer.getShort();
        meta.valuePresent = buffer.get() != 0;
        return meta;
    }

    private void decodeKey(ByteBuffer decodeFrom, CharBuffer storeWhere, int keyLength) {
        decode(decodeFrom, storeWhere, 0, keyLength);
    }

    private void decodeValue(ByteBuffer decodeFrom, CharBuffer storeWhere, EntryMeta meta, int entrySize) {
        decode(decodeFrom, storeWhere, meta.keySize, entrySize - EntryMeta.META_SIZE);
    }

    private void decode(ByteBuffer decodeFrom, CharBuffer storeWhere, int from, int toExclusive) {
        decodeFrom.limit(toExclusive);
        decodeFrom.position(from);
        storeWhere.clear();
        decoder.decode(decodeFrom, storeWhere, true);
        storeWhere.flip();
    }

    private void encode(String string, CharBuffer encodeFrom, ByteBuffer storeWhere) {
        putStringInChBuffer(encodeFrom, string);
        encoder.encode(encodeFrom, storeWhere, true);
    }

    private void putStringInChBuffer(CharBuffer charBuffer, String string) {
        charBuffer.clear();
        charBuffer.put(string);
        charBuffer.flip();
    }

    private static int sizeAtWorst(BaseEntry<String> entry) {
        int entryStringLength = entry.key().length() + (entry.value() != null ? entry.value().length() : 0);
        return entryStringLength * 3 + EntryMeta.META_SIZE;
    }

    private record BuffersTriple(ByteBuffer buffer, CharBuffer searchedKeyBuffer, CharBuffer charBuffer) {
        public static BuffersTriple newTriple(int bufferSize) {
            return new BuffersTriple(
                    ByteBuffer.allocate(bufferSize),
                    CharBuffer.allocate(bufferSize),
                    CharBuffer.allocate(bufferSize)
            );
        }

        public int capacity() {
            return buffer.capacity();
        }
    }

    private static class EntryMeta {
        public static final int META_SIZE = Short.BYTES + Byte.BYTES;

        public int keySize;
        public boolean valuePresent;
    }

}
