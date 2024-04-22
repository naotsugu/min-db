package com.mammb.code.db;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {

    private static final Charset charset = StandardCharsets.US_ASCII;
    private static final float maxBytesPerChar = charset.newEncoder().maxBytesPerChar();

    private final ByteBuffer bb;

    public Page(int blockSize) {
        bb = ByteBuffer.allocateDirect(blockSize);
    }

    public Page(byte[] b) {
        bb = ByteBuffer.wrap(b);
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

    ByteBuffer contents() {
        bb.position(0);
        return bb;
    }

}
