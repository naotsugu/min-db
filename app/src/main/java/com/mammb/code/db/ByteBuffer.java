package com.mammb.code.db;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class ByteBuffer {

    private static final Charset charset = StandardCharsets.US_ASCII;
    private static final float maxBytesPerChar = charset.newEncoder().maxBytesPerChar();

    private final java.nio.ByteBuffer bb;

    public ByteBuffer(int blockSize) {
        bb = java.nio.ByteBuffer.allocateDirect(blockSize);
    }

    public ByteBuffer(byte[] b) {
        bb = java.nio.ByteBuffer.wrap(b);
    }

    public int getInt(int offset) {
        return bb.getInt(offset);
    }

    public void setInt(int offset, int n) {
        bb.putInt(offset, n);
    }

    public byte[] getBytes(int offset) {
        bb.position(offset);
        int length = bb.getInt();
        byte[] b = new byte[length];
        bb.get(b);
        return b;
    }

    public void setBytes(int offset, byte[] b) {
        bb.position(offset);
        bb.putInt(b.length);
        bb.put(b);
    }

    public String getString(int offset) {
        byte[] b = getBytes(offset);
        return new String(b, charset);
    }

    public void setString(int offset, String s) {
        byte[] b = s.getBytes(charset);
        setBytes(offset, b);
    }

    public static int maxLength(int strlen) {
        return Integer.BYTES + (strlen * (int) maxBytesPerChar);
    }

    int readFrom(FileChannel fc) throws IOException {
        bb.position(0);
        return fc.read(bb);
    }

    int writeTo(FileChannel fc) throws IOException {
        bb.position(0);
        return fc.write(bb);
    }

}
