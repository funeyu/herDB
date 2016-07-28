package index;

/**
 * 文件缓冲块的大小，用来加快读写文件，类似于BufferByte
 *
 * @author funer
 */
public class BufferedBlock {
    // 缓冲大小
    protected final int capacity;
    // 缓冲池操作指针指示的位置
    protected int position;
    // 缓冲的具体二进制内容
    protected final byte[] container;
    // 缓冲区域有效区的截止边
    protected int limit;

    // 用来记录文件块在文件中的具体的偏移
    protected long offset;

    protected BufferedBlock(int capacity, int position) {

        this.capacity = capacity;
        this.position = position;
        this.container = new byte[capacity];
    }

    protected byte[] getBlock() {

        return container;
    }

    // 只在此更改offset,将position与offset加 skip
    protected void advance(int skip) {

        position += skip;
        offset += skip;
    }

    protected int getPosition() {

        return this.position;
    }

    protected int setLimit(int limit) {

        this.limit = limit;
        return this.limit;
    }

    // 获取该读取文件块的有效大小
    protected int getLimit() {

        return limit;
    }

    // block剩余的空间
    protected int left() {

        return limit - position;
    }

    protected long getOffset() {

        return offset;
    }

    // 将文件指针置于块头
    protected BufferedBlock placeHeader() {

        position = 0;
        return this;
    }
}
