package index;

/**
 * 内存索引的溢出异常，
 * @author funeyu
 *
 */
@SuppressWarnings("serial")
public class IndexOutofRangeException extends Exception{
    
    public IndexOutofRangeException(){
        super("the IndexMemory is full and can't find any empty slot in attachedSlots");
    }
}
