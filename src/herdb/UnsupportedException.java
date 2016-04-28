package herdb;

/**
 * 不支持的操作异常
 * @author funer
 *
 */
@SuppressWarnings("serial")
public class UnsupportedException extends RuntimeException {
    
    public UnsupportedException(String method, Object obj){
        
        super("the method：　" + method + "of class:" + obj.getClass() + "isn't supported");
    }
}
