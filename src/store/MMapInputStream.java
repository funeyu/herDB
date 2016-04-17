package store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 为了加快文件随机读的操作，将文件映射到内存中 注意： 不能在又读又写的情况下开启映射文件，为了性能映射内存只映射一次， 不能根据文件的追加而实时改变内存；
 * 
 * @author funer
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

    public byte[] readFully() throws IOException {

        return null;
    }

    public byte[] seek(long offset, int size) throws IOException {

        return null;
    }

    public long append(byte[] data) throws IOException {

        return 0;
    }

    public void flush(byte[] data) {

    }

    @Override public byte[] readSequentially(int size) throws IOException {

        return null;
    }

    @Override public long maxOffSet() {

        return 0;
    }

    @Override public InputOutData position(long offset) throws IOException {

        return null;
    }

    @Override public InputOutData deleteFile() {
        
        return null;
    }

    @Override
    public InputOutData createNewFile() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

}
