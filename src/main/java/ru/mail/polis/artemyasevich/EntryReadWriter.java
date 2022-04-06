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
    private final ByteBuffer buffer;
    private final CharBuffer searchedKeyBuffer;
    private final CharBuffer charBuffer;
    private final CharsetEncoder encoder;
    private final CharsetDecoder decoder;

    int keySize;
    boolean valuePresent;

    EntryReadWriter(int bufferSize) {
        this.buffer = ByteBuffer.allocate(1000000);
        this.searchedKeyBuffer = CharBuffer.allocate(1000000);
        this.charBuffer = CharBuffer.allocate(1000000);
        this.encoder = StandardCharsets.UTF_8.newEncoder();
        this.decoder = StandardCharsets.UTF_8.newDecoder();
    }

    //|key|value|keySize|valuePresent or key|keySize|valueNotPresent if value == null
    int writeEntryInChannel(FileChannel channel, BaseEntry<String> entry) throws IOException {
        fillBufferWithEntry(entry);
        channel.write(buffer);
        return buffer.limit();
    }

    BaseEntry<String> readEntryFromChannel(DaoFile daoFile, int index) throws IOException {
        return readEntryFromChannel(daoFile.getChannel(), daoFile.entrySize(index), daoFile.getOffset(index));
    }

    private BaseEntry<String> readEntryFromChannel(FileChannel channel, int entrySize, long offset) throws IOException {
        readKey(channel, entrySize, offset);
        String key = charBuffer.toString();
        if (!valuePresent) {
            return new BaseEntry<>(key, null);
        }
        fillCharBuffer(keySize, entrySize - 3);
        String value = charBuffer.toString();
        return new BaseEntry<>(key, value);
    }

    private void readKey(FileChannel channel, int entrySize, long offset) throws IOException {
        fillBufferWithEntry(channel, entrySize, offset);
        processMetaInBuffer();
        fillCharBuffer(0, keySize);
    }

    private void fillCharBuffer(int pos, int limit) {
        buffer.limit(limit);
        buffer.position(pos);
        charBuffer.clear();
        decoder.decode(buffer, charBuffer, true);
        charBuffer.flip();
    }

    CharBuffer bufferAsKeyOnly(DaoFile daoFile, int index) throws IOException {
        readKey(daoFile.getChannel(), daoFile.entrySize(index), daoFile.getOffset(index));
        return charBuffer;
    }

    CharBuffer fillAndGetKeyBuffer(String key) {
        searchedKeyBuffer.clear();
        searchedKeyBuffer.put(key);
        searchedKeyBuffer.flip();
        return searchedKeyBuffer;
    }

    int maxKeyLength() {
        return (buffer.capacity() / 2 - Short.BYTES / 2);
    }

    private void putStringInBuffer(String string, CharBuffer charBuffer) {
        charBuffer.clear();
        charBuffer.put(string);
        charBuffer.flip();
    }

    private void processMetaInBuffer() {
        buffer.position(buffer.limit() - 3);
        keySize = buffer.getShort();
        valuePresent = buffer.get() != 0;
    }

    private void fillBufferWithEntry(FileChannel channel, int entrySize, long offset) throws IOException {
        buffer.clear();
        buffer.limit(entrySize);
        channel.read(buffer, offset);
        buffer.flip();
    }

    private void fillBufferWithEntry(BaseEntry<String> entry) {
        buffer.clear();
        putStringInBuffer(entry.key(), charBuffer);
        encoder.encode(charBuffer, buffer, true);
        int keySize = buffer.position();
        byte valuePresent = 0;
        if (entry.value() != null) {
            putStringInBuffer(entry.value(), charBuffer);
            encoder.encode(charBuffer, buffer, true);
            valuePresent = 1;
        }
        buffer.putShort((short) keySize);
        buffer.put(valuePresent);
        buffer.flip();
    }

}
