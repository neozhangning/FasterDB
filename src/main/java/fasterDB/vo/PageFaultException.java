package fasterDB.vo;

/**
 * Created by zn on 15/5/3.
 */
public class PageFaultException extends Exception {
    private static final long serialVersionUID = 6534959927285194570L;

    public PageFaultException(String message) {
        super(message);
    }
}
