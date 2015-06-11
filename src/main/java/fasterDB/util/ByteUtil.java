package fasterDB.util;

public class ByteUtil {

    public static void main(String[] args) {
        System.out.println(((byte)255) & 0xff);
        System.out.println((1 << 8) - 1);
    }

    public static final int getUnsignedByte(byte[] bytes, int from) {
        if (bytes == null) {
            throw new NullPointerException("bytes is null");
        }
        if (bytes.length - from < 1) {
            throw new IllegalArgumentException("there is no enough bytes to decode unsigned byte value");
        }
        return bytes[from] & 0xff;
    }

    public static final void getUnsignedByte(byte b, byte[] bytes, int from) {
        if (bytes == null) {
            throw new NullPointerException("bytes is null");
        }
        if (bytes.length - from < 1) {
            throw new IllegalArgumentException("there is no enough bytes to encode unsigned byte value");
        }
        bytes[from] = b;
    }

    public static final int getUnsignedShortBigEndian(byte[] bytes, int from) {
        if (bytes == null) {
            throw new NullPointerException("bytes is null");
        }
        if (bytes.length - from < 2) {
            throw new IllegalArgumentException("there is no enough bytes to decode unsigned short value");
        }
        return ((bytes[from] & 0xff) << 8) |
                ((bytes[from + 1] & 0xff));
    }

    public static final byte[] getUnsignedBytesBigEndian(short s) {
        int i = s & 0xffff;
        byte[] bytes = new byte[2];
        bytes[0] = (byte)(i >>  8);
        bytes[1] = (byte)(i      );
        return bytes;
    }

    public static final void getUnsignedBytesBigEndian(short s, byte[] bytes, int from) {
        if (bytes == null) {
            throw new NullPointerException("bytes is null");
        }
        if (bytes.length - from < 2) {
            throw new IllegalArgumentException("there is no enough bytes to encode short value");
        }
        int i = s & 0xffff;
        bytes[from    ] = (byte)(i >>  8);
        bytes[from + 1] = (byte)(i      );
    }

    public static final long getUnsignedIntBigEndian(byte[] bytes, int from) {
        if (bytes == null) {
            throw new NullPointerException("bytes is null");
        }
        if (bytes.length - from < 4) {
            throw new IllegalArgumentException("there is no enough bytes to decode unsigned int value");
        }
        return (((((long) bytes[from]) & 0xff) << 24) |
                ((bytes[from + 1] & 0xff) << 16) |
                ((bytes[from + 2] & 0xff) << 8) |
                ((bytes[from + 3] & 0xff)));
    }

    public static final byte[] getUnsignedBytesBigEndian(int i) {
        long l = i & 0xffffffff;
        byte[] bytes = new byte[4];
        bytes[0] = (byte)(i >> 24);
        bytes[1] = (byte)(i >> 16);
        bytes[2] = (byte)(i >>  8);
        bytes[3] = (byte)(i      );
        return bytes;
    }

    public static final void getUnsignedBytesBigEndian(int i, byte[] bytes, int from) {
        if (bytes == null) {
            throw new NullPointerException("bytes is null");
        }
        if (bytes.length - from < 4) {
            throw new IllegalArgumentException("there is no enough bytes to encode unsigned int value");
        }
        long l = i & 0xffffffff;
        bytes[from    ] = (byte)(i >> 24);
        bytes[from + 1] = (byte)(i >> 16);
        bytes[from + 2] = (byte)(i >>  8);
        bytes[from + 3] = (byte)(i      );
    }

    public static final byte[] getBytesBigEndian(short s) {
        byte[] bytes = new byte[2];
        bytes[0] = (byte)(s >>  8);
        bytes[1] = (byte)(s      );
        return bytes;
    }

    public static final void getBytesBigEndian(short s, byte[] bytes, int from) {
        if (bytes == null) {
            throw new NullPointerException("bytes is null");
        }
        if (bytes.length - from < 2) {
            throw new IllegalArgumentException("there is no enough bytes to encode short value");
        }
        bytes[from    ] = (byte)(s >>  8);
        bytes[from + 1] = (byte)(s      );
    }

    public static final short getShortBigEndian(byte[] bytes, int from) {
        if (bytes == null) {
            throw new NullPointerException("bytes is null");
        }
        if (bytes.length - from < 2) {
            throw new IllegalArgumentException("there is no enough bytes to decode short value");
        }
        return (short) (((bytes[from]) << 8) |
                ((bytes[from + 1] & 0xff)));
    }

    public static final byte[] getBytesBigEndian(int i) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte)(i >> 24);
        bytes[1] = (byte)(i >> 16);
        bytes[2] = (byte)(i >>  8);
        bytes[3] = (byte)(i      );
        return bytes;
    }

    public static final void getBytesBigEndian(int i, byte[] bytes, int from) {
        if (bytes == null) {
            throw new NullPointerException("bytes is null");
        }
        if (bytes.length - from < 4) {
            throw new IllegalArgumentException("there is no enough bytes to encode int value");
        }
        bytes[from    ] = (byte)(i >> 24);
        bytes[from + 1] = (byte)(i >> 16);
        bytes[from + 2] = (byte)(i >>  8);
        bytes[from + 3] = (byte)(i      );
    }

    public static final int getIntBigEndian(byte[] bytes, int from) {
        if (bytes == null) {
            throw new NullPointerException("bytes is null");
        }
        if (bytes.length - from < 4) {
            throw new IllegalArgumentException("there is no enough bytes to decode int value");
        }
        return (((bytes[from    ]       ) << 24) |
                ((bytes[from + 1] & 0xff) << 16) |
                ((bytes[from + 2] & 0xff) <<  8) |
                ((bytes[from + 3] & 0xff)      ));
    }

    public static final byte[] getBytesBigEndian(long l) {
        byte[] bytes = new byte[8];
        bytes[0] = (byte)(l >> 56);
        bytes[1] = (byte)(l >> 48);
        bytes[2] = (byte)(l >> 40);
        bytes[3] = (byte)(l >> 32);
        bytes[4] = (byte)(l >> 24);
        bytes[5] = (byte)(l >> 16);
        bytes[6] = (byte)(l >>  8);
        bytes[7] = (byte)(l      );
        return bytes;
    }

    public static final void getBytesBigEndian(long l, byte[] bytes, int from) {
        if (bytes == null) {
            throw new NullPointerException("bytes is null");
        }
        if (bytes.length - from < 8) {
            throw new IllegalArgumentException("there is no enough bytes to encode long value");
        }
        bytes[from    ] = (byte)(l >> 56);
        bytes[from + 1] = (byte)(l >> 48);
        bytes[from + 2] = (byte)(l >> 40);
        bytes[from + 3] = (byte)(l >> 32);
        bytes[from + 4] = (byte)(l >> 24);
        bytes[from + 5] = (byte)(l >> 16);
        bytes[from + 6] = (byte)(l >>  8);
        bytes[from + 7] = (byte)(l      );
    }

    public static final long getLongBigEndian(byte[] bytes, int from) {
        if (bytes == null) {
            throw new NullPointerException("bytes is null");
        }
        if (bytes.length - from < 8) {
            throw new IllegalArgumentException("there is no enough bytes to decode long value");
        }
        return ((((long) (bytes[from    ]       )) << 56) |
                (((long) (bytes[from + 1] & 0xff)) << 48) |
                (((long) (bytes[from + 2] & 0xff)) << 40) |
                (((long) (bytes[from + 3] & 0xff)) << 32) |
                (((long) (bytes[from + 4] & 0xff)) << 24) |
                (((long) (bytes[from + 5] & 0xff)) << 16) |
                (((long) (bytes[from + 6] & 0xff)) <<  8) |
                (((long) (bytes[from + 7] & 0xff))      ));
    }
}
