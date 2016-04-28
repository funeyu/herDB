package store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import herdb.UnsupportedException;

/**
 * 为了加快文件随机读的操作，将文件映射到内存中 
 * 注意： 不能在又读又写的情况下开启映射文件，为了性能映射内存只映射一次， 不能根据文件的追加而实时改变内存；
 * 
 * @author funeyu
 *
 */
public class MMapInputStream extends InputOutData {

    private MappedByteBuffer randomFile;

    public MMapInputStream(String pathName) {
        File file = new File(pathName);
        try {
            randomFile = new RandomAccessFile(file, "r").getChannel().map(FileChannel.MapMode.READ_ONLY, 0,
                    file.length());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 不需要将磁盘文件全部读到内存中
    @Override public byte[] readFully() throws IOException {

        throw new UnsupportedException("readFully", this);
    }

    @Override public byte[] seek(long offset, int size) throws IOException {
        
        randomFile.position((int) offset);
        return null;
    }

    @Override public InputOutData append(byte[] data) throws IOException {

        throw new UnsupportedException("append", this);
    }

    @Override public void flush(byte[] data) {

        throw new UnsupportedException("flush", this);
    }

    @Override public byte[] readSequentially(int size) throws IOException {

        return null;
    }

    @Override public long maxOffSet() {

        return randomFile.capacity();
    }

    @Override public InputOutData position(long offset) throws IOException {
        
        randomFile.position((int)offset);
        return this;
    }

    @Override public InputOutData deleteFile() {
        
        throw new UnsupportedException("deleteFile", this);
    }

    @Override public InputOutData createNewFile() throws IOException {
        
        throw new UnsupportedException("createNewFile", this);
    }

    @Override public int readBlock(byte[] block) {
        
        return randomFile.get(block).capacity();
    }

    @Override public boolean reName(String newName) {
        
        throw new UnsupportedException("reName", this);
    }

    @Override public void jumpHeader() throws IOException {
        
        randomFile.position(0);
    }

}
