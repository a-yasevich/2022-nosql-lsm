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
    private static final int META_SIZE = Short.BYTES + Byte.BYTES;
    private final CharsetEncoder encoder;
    private final CharsetDecoder decoder;
    private ByteBuffer buffer;
    private CharBuffer keyToFindBuffer;
    private CharBuffer charBuffer;

    EntryIOManager(int bufferSize) {
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
        boolean valuePresent = entry.value() != null;
        increaseBuffersIfNeeded(entry.key().length() + (valuePresent ? entry.value().length() : 0));
        putStringInBufferToEncode(entry.key());
        encoder.encode(charBuffer, buffer, true);
        int keySize = buffer.position();
        if (valuePresent) {
            putStringInBufferToEncode(entry.value());
            encoder.encode(charBuffer, buffer, true);
        }
        buffer.putShort((short) keySize);
        buffer.put((byte) (valuePresent ? 1 : 0));
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
        buffer.clear();
        buffer.limit(daoFile.entrySize(index));
        daoFile.getChannel().read(buffer, daoFile.getOffset(index));
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

    int maxPossibleKeyLength() {
        return charBuffer.capacity();
    }

    private void putStringInBufferToEncode(String string) {
        charBuffer.clear();
        charBuffer.put(string);
        charBuffer.flip();
    }

    private void increaseBuffersIfNeeded(int entryStringLength) {
        int sizeAtWorst = entryStringLength * 3 + META_SIZE;
        if (sizeAtWorst <= buffer.capacity()) {
            return;
        }
        int newCapacity = (int) (sizeAtWorst * 1.5);
        keyToFindBuffer = CharBuffer.allocate(newCapacity);
        charBuffer = CharBuffer.allocate(newCapacity);
        buffer = ByteBuffer.allocate(newCapacity);
    }

    static long sizeOfEntry(BaseEntry<String> entry) {
        int valueSize = entry.value() == null ? 0 : entry.value().length();
        return (entry.key().length() + valueSize) * 2L;
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
