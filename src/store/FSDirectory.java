package store;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;

/**
 * @author funer
 *
 */
public class FSDirectory {
    private File directory;
    private Lock lock;
    private InputStream input;
    private OutputStream output;
    private final static String LOCKFILENAME = "herDB.lock";
    private final static String DATAFILENAME = "herDB.dat";

    /**
     * @param directoryPath：为目录的文件夹文件
     */
    private FSDirectory(File directory) {
        this.directory = directory;
    }

    /**
     * 利用创建herDB.lock空文件的锁定策略
     * 
     * @return 如果上锁失败返回null， 成功就返回该FSDirectory实例
     */
    private FSDirectory lockDir() {

        if (lock == null)
            lock = new Lock() {
                protected boolean acquire() {
                    File lockFile = new File(directory.getPath() + File.separator + LOCKFILENAME);
                    try {
                        if (lockFile.createNewFile())
                            return true;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return false;
                };

                public void release() {
                    File f = new File(directory.getPath(), LOCKFILENAME);
                    if (f.exists())
                        f.delete();
                };
            };
        return lock.lock(10) ? this : null;
    }

    private String getPath() {

        return directory.getPath();
    }

    private InputStream getInput() {

        return null;
    }

    public boolean isExsit(String pathName) {

        File f = new File(directory.getPath(), pathName);
        return f.exists() ? true : false;
    }

    /**
     * 文件不存在的时候，创建一个文件名为<code>fileName</code>的文件
     * 
     * @param pathName
     * @return
     */
    public File touchFile(String fileName) {

        File newedFile = new File(directory.getPath() + File.separator + fileName);
        try {
            if (newedFile.createNewFile())
                return newedFile;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public byte[] readIndexFully(String name) {
        try {
            RandomAccessFile f = new RandomAccessFile(new File(directory, name), "r");
            int length = (int) f.length();
            byte[] data = new byte[length];
            f.readFully(data);
            return data;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    private byte[] seek(int start, int offset) {
        return null;
    }

    /**
     * 创建FSDirectory的 最后要将该目录锁定，不让有其他的进程操作数据库；
     * 
     * @param directoryPath
     *            指定的目录路径
     * @param delete
     *            目录存在删除为true，不删除为false
     * @return 创建成功就返回FSDirectory实例
     * @throws IOException
     */
    public static FSDirectory create(String directoryPath, boolean delete) throws IOException {

        File file = new File(directoryPath);
        if (!file.exists()) {
            if (!file.mkdirs())
                throw new IOException("不能创建初始的文件夹：" + directoryPath);
        } else {
            // 这里有两个出错异常点：
            // 1，文件夹已经存在但是 delete为false的情况下，自定义的异常
            // 2，删除文件夹里所有的文件出现的系统IOException异常
            if (delete ? !deleteFiles(file) : true)
                throw new IOException("文件夹存在但是又不能删除或者删除出错：" + directoryPath);
        }
        return new FSDirectory(file).lockDir();
    }

    /**
     * 打开一个目录, 此目录必须是herDB的目录，如果有锁文件就throw exception 然后再进行枷锁的操作
     * 
     * @param directoryPath
     *            要打开的文件夹
     * @param onlyRead
     *            true:只读不写 false:读写
     * @return
     */
    public static FSDirectory open(String directoryPath) {

        File file = new File(directoryPath);
        if (!file.exists())
            return null;
        return new FSDirectory(file).lockDir();
    }

    /**
     * 目录下只有文件，没有二级的文件夹，删除目录里所有的文件 留着一个空的目录
     * 
     * @param dir
     *            将要删除文件的目录
     * @return 只要有一个删除不成功就返回false 全部删除完成返回true
     */
    public static boolean deleteFiles(File dir) {

        for (File f : dir.listFiles()) {
            if (!f.delete())
                return false;
        }
        return true;
    }

    /**
     * 通过FSDirectory创建FSDataStream，负责目录下所有的数据读写操作 包括index文件的读取，与rawData数据文件的读取
     * 
     * @param name
     *            该目录下的文件名
     * @return
     * @throws Exception
     */
    public FSDataStream createDataStream(String name) throws Exception {

        return new FSDataStream(this.directory, name);
    }

    class FSDataStream extends InputOutData {

        private long length;
        private long maxOffSet;
        private RandomAccessFile raf;

        public FSDataStream(File dir, String name) throws Exception {

            File f = new File(dir, name);

            raf = new RandomAccessFile(f, "rw");
            length = raf.length();
        }

        public byte[] readFully() throws IOException {

            byte[] data = new byte[(int) length];
            raf.read(data);
            return data;
        }

        public void flush(byte[] datas) {

            try {
                raf.write(datas, 0, datas.length);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        @Override
        public byte[] seek(long offset, int size) throws IOException {

            raf.seek(offset);

            byte[] res = new byte[size];
            if (raf.read(res) == -1)
                throw new EOFException();

            return res;
        }

        @Override
        public long append(byte[] data) throws IOException {

            long fileLength = raf.length();
            raf.seek(fileLength);
            raf.write(data);
            maxOffSet += data.length;
            return fileLength + data.length;
        }

        @Override
        public byte[] readSequentially(int size) throws IOException {

            byte[] bytes = new byte[size];
            raf.read(bytes);
            return bytes;
        }

        @Override
        public long maxOffSet() {

            return maxOffSet;
        }
    }
}
