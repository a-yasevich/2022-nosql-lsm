package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

public class EntryReadWriter {
    private static final int META_SIZE = Short.BYTES + Byte.BYTES;
    private final ByteBuffer buffer;
    private final CharBuffer keyToFindBuffer;
    private final CharBuffer charBuffer;
    private final CharsetEncoder encoder;
    private final CharsetDecoder decoder;

    EntryReadWriter(int bufferSize) {
        this.buffer = ByteBuffer.allocate(bufferSize);
        this.keyToFindBuffer = CharBuffer.allocate(bufferSize);
        this.charBuffer = CharBuffer.allocate(bufferSize);
        this.encoder = StandardCharsets.UTF_8.newEncoder();
        this.decoder = StandardCharsets.UTF_8.newDecoder();
    }

    //valueSize is redundant, the valuePresent marker is sufficient
    //|key|value|keySize|valuePresent or key|keySize|valueNotPresent if value == null
    int writeEntry(FileChannel channel, BaseEntry<String> entry) throws IOException {
        buffer.clear();
        putStringInBufferToEncode(entry.key(), charBuffer);
        encoder.encode(charBuffer, buffer, true);
        int keySize = buffer.position();
        byte valuePresent = 0;
        if (entry.value() != null) {
            putStringInBufferToEncode(entry.value(), charBuffer);
            encoder.encode(charBuffer, buffer, true);
            valuePresent = 1;
        }
        buffer.putShort((short) keySize);
        buffer.put(valuePresent);
        buffer.flip();
        channel.write(buffer);
        return buffer.limit();
    }


    BaseEntry<String> readEntry(DaoFile daoFile, int index) throws IOException {
        fillBufferWithEntry(daoFile, index);
        int keySize = seekBufferToMetaAndGetKeySize();
        boolean valuePresent = buffer.get() != 0;
        String key = getString(0, keySize);
        if (!valuePresent) {
            return new BaseEntry<>(key, null);
        }
        String value = getString(keySize, daoFile.entrySize(index) - META_SIZE);
        return new BaseEntry<>(key, value);
    }

    private int seekBufferToMetaAndGetKeySize() {
        buffer.position(buffer.limit() - META_SIZE);
        return buffer.getShort();
    }

    private void fillBufferWithEntry(DaoFile daoFile, int index) throws IOException {
        FileChannel fileChannel = daoFile.getChannel();
        int entrySize = daoFile.entrySize(index);
        long offset = daoFile.getOffset(index);
        buffer.clear();
        buffer.limit(entrySize);
        fileChannel.read(buffer, offset);
        buffer.flip();
    }

    private String getString(int from, int toExclusive) {
        fillCharBuffer(from, toExclusive);
        return charBuffer.toString();
    }

    private void fillCharBuffer(int from, int toExclusive) {
        buffer.limit(toExclusive);
        buffer.position(from);
        charBuffer.clear();
        decoder.decode(buffer, charBuffer, true);
        charBuffer.flip();
    }

    private void fillKeyBuffer(String key) {
        keyToFindBuffer.clear();
        keyToFindBuffer.put(key);
        keyToFindBuffer.flip();
    }

    int maxKeyLength() {
        return (buffer.capacity() / 2 - Short.BYTES / 2);
    }

    private void putStringInBufferToEncode(String string, CharBuffer charBuffer) {
        charBuffer.clear();
        charBuffer.put(string);
        charBuffer.flip();
    }


    int getEntryIndex(String key, DaoFile daoFile) throws IOException {
        fillKeyBuffer(key);
        int left = 0;
        int right = daoFile.getLastIndex();
        while (left <= right) {
            int middle = (right - left) / 2 + left;
            fillBufferWithEntry(daoFile, middle);
            fillCharBuffer(0, seekBufferToMetaAndGetKeySize());
            int comparison = keyToFindBuffer.compareTo(charBuffer);
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

}
