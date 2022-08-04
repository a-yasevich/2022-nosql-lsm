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
    private ByteBuffer buffer;
    private CharBuffer searchedKeyBuffer;
    private CharBuffer charBuffer;
    private final EntryMeta entryMeta = new EntryMeta();

    EntryIOManager(int bufferSize) {
        this.buffer = ByteBuffer.allocate(bufferSize);
        this.searchedKeyBuffer = CharBuffer.allocate(bufferSize);
        this.charBuffer = CharBuffer.allocate(bufferSize);
        this.encoder = StandardCharsets.UTF_8.newEncoder();
        this.decoder = StandardCharsets.UTF_8.newDecoder();
    }

    //[key|value|keySize|valuePresent] or [key|keySize|valueNotPresent] if value == null
    int writeEntry(FileChannel channel, BaseEntry<String> entry) throws IOException {
        int bytesToWrite = writeEntryInBuffer(entry);
        channel.write(buffer);
        return bytesToWrite;
    }

    BaseEntry<String> readEntry(DaoFile daoFile, int index) throws IOException {
        readEntryInBuffer(daoFile, index, buffer);
        EntryMeta meta = entryMetaFromBuffer(buffer);
        String key = decodeKey(buffer, charBuffer, meta.keySize);
        String value = decodeValue(buffer, charBuffer, entryMeta, daoFile.entrySize(index));
        return new BaseEntry<>(key, value);
    }

    long sizeOfEntry(BaseEntry<String> entry) {
        return writeEntryInBuffer(entry);
    }

    int getEntryIndex(String key, DaoFile daoFile) throws IOException {
        putStringInChBuffer(searchedKeyBuffer, key);
        int left = 0;
        int right = daoFile.getLastIndex();
        while (left <= right) {
            int middle = (right - left) / 2 + left;
            readEntryInBuffer(daoFile, middle, buffer);
            decodeKeyOnly(buffer, charBuffer);
            int comparison = searchedKeyBuffer.compareTo(charBuffer);
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

    private int writeEntryInBuffer(BaseEntry<String> entry) {
        buffer.clear();
        boolean valuePresent = entry.value() != null;
        increaseBuffersIfNeeded(entry.key().length() + (valuePresent ? entry.value().length() : 0));
        encode(entry.key(), charBuffer, buffer);
        int keySize = buffer.position();
        if (valuePresent) {
            encode(entry.value(), charBuffer, buffer);
        }
        buffer.putShort((short) keySize);
        buffer.put((byte) (valuePresent ? 1 : 0));
        buffer.flip();
        return buffer.limit();
    }

    private void increaseBuffersIfNeeded(int entryStringLength) {
        int sizeAtWorst = entryStringLength * 3 + EntryMeta.META_SIZE;
        if (sizeAtWorst > buffer.capacity()) {
            int newCapacity = (int) (sizeAtWorst * 1.5);
            searchedKeyBuffer = CharBuffer.allocate(newCapacity);
            charBuffer = CharBuffer.allocate(newCapacity);
            buffer = ByteBuffer.allocate(newCapacity);
        }
    }

    private void encode(String string, CharBuffer encodeFrom, ByteBuffer storeWhere) {
        putStringInChBuffer(encodeFrom, string);
        encoder.encode(encodeFrom, storeWhere, true);
    }

    private EntryMeta entryMetaFromBuffer(ByteBuffer buffer) {
        buffer.position(buffer.limit() - EntryMeta.META_SIZE);
        entryMeta.keySize = buffer.getShort();
        entryMeta.valuePresent = buffer.get() != 0;
        return entryMeta;
    }

    private void readEntryInBuffer(DaoFile daoFile, int index, ByteBuffer buffer) throws IOException {
        buffer.clear();
        buffer.limit(daoFile.entrySize(index));
        daoFile.getChannel().read(buffer, daoFile.getOffset(index));
        buffer.flip();
    }

    private String decodeKey(ByteBuffer decodeFrom, CharBuffer storeWhere, int keyLength){
        decode(decodeFrom, storeWhere,0, keyLength);
        return storeWhere.toString();
    }
    private String decodeValue(ByteBuffer decodeFrom, CharBuffer storeWhere, EntryMeta meta, int entrySize){
        if (!meta.valuePresent) {
            return null;
        }
        decode(decodeFrom, storeWhere, meta.keySize, entrySize - EntryMeta.META_SIZE);
        return storeWhere.toString();
    }

    private void decodeKeyOnly(ByteBuffer decodeFrom, CharBuffer storeWhere){
        EntryMeta meta = entryMetaFromBuffer(decodeFrom);
        decode(decodeFrom, storeWhere, 0, meta.keySize);
    }

    private void decode(ByteBuffer decodeFrom, CharBuffer storeWhere, int from, int toExclusive) {
        decodeFrom.limit(toExclusive);
        decodeFrom.position(from);
        storeWhere.clear();
        decoder.decode(decodeFrom, storeWhere, true);
        storeWhere.flip();
    }

    private void putStringInChBuffer(CharBuffer charBuffer, String string) {
        charBuffer.clear();
        charBuffer.put(string);
        charBuffer.flip();
    }

    int maxPossibleKeyLength() {
        return charBuffer.capacity();
    }

    private static class EntryMeta{
        public static final int META_SIZE = Short.BYTES + Byte.BYTES;

        public int keySize;
        public boolean valuePresent;
    }

}
