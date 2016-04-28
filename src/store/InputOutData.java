package store;

import java.io.IOException;

/**
 * 文件读写涉及到的类
 * 
 * @author fuheu
 *
 */
/**
 * @author funeyu
 *
 */
public abstract class InputOutData {

    /**
     * 一次将文件所有的内容读入内存中
     * 
     * @return 文件内容的字节数组
     */
    public abstract byte[] readFully() throws IOException;

    /**
     * 从文件offset开始处，随机读取length长度的字节数组
     * 
     * @param offset
     *            开始读取的位置
     * @param length
     *            读取文件的长度
     * @return 读取到的字节数组
     */
    public abstract byte[] seek(long offset, int size) throws IOException;

    /**
     * 将内存中的flush到磁盘中
     * 
     * @param data
     *            内存中的字节数组
     */
    public abstract void flush(byte[] data);

    /**
     * 从文件的末尾追加数据
     * 
     * @param data
     * @return 追加完数据后
     */
    public abstract InputOutData append(byte[] data) throws IOException;

    /**
     * 顺序读取文件
     * 
     * @param size
     * @return
     */
    public abstract byte[] readSequentially(int size) throws IOException;

    /**
     * 获取文件的offset
     * 
     * @return
     */
    public abstract long maxOffSet();
    
    /**
     * 将文件的指针置于相应的位置
     * @param offset
     * @return
     */
    public abstract InputOutData position(long offset) throws IOException;
    
    /**
     * 清除该文件磁盘的所有内容
     */
    public abstract InputOutData deleteFile();
    
    /**
     * 新建文件
     * @return
     */
    public abstract InputOutData createNewFile() throws IOException;
    
    /**
     * 将该文件读写指针置于开头
     * @return
     */
    public abstract void jumpHeader()throws IOException;
    
    public abstract int readBlock(byte[] block) throws IOException;
    
    /**
     * 修改文件的名称为 newName
     * @param newName
     * @return
     */
    public abstract boolean reName(String newName);
}
