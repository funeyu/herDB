package store;

import java.io.IOException;

/**
 * 文件读写涉及到的类
 * @author fuheu
 *
 */
public abstract class InputOutData {
	
	/**
	 * 一次将文件所有的内容读入内存中
	 * @return 文件内容的字节数组
	 */
	public abstract byte[] readFully() throws IOException;
	
	/**
	 * 从文件随机读取length长度的字节数组
	 * @param offset 开始读取的位置
	 * @param length 读取文件的长度
	 * @return 读取到的字节数组
	 */
	public abstract byte[] seek(long offset, int size) throws IOException;
	
	/**
	 * 将内存中的flush到磁盘中
	 * @param data 内存中的字节数组
	 */
	public abstract void flush(byte[] data);
	
	/**
	 * 从文件的末尾追加数据
	 * @param data
	 * @return 追加完数据后 
	 */
	public abstract long append(byte[]data) throws IOException; 
	
	/**
	 * 顺序读取文件
	 * @param size
	 * @return
	 */
	public abstract byte[] readSequentially(int size)throws IOException;
	
	/**
	 * 获取文件的offset
	 * @return
	 */
	public abstract long maxOffSet();
	
	
}
