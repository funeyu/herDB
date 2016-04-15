package store;

public abstract class Lock {
    protected final static int INTERVAL = 1000;

    /**
     * 获取锁的方法，这里是一次尝试获取，若要多次尝试需写while循环
     * 
     * @return
     */
    protected abstract boolean acquire();

    /**
     * 释放锁的方法，这里是一次释放就可以
     * 
     * @return
     */
    public abstract void release();

    /**
     * 尝试多次获取锁，若经过仍没能获取成功就直接返回false
     * 
     * @return
     * @throws Exception
     */
    public boolean lock(int lockWaitTimes) {

        int times = 0;
        while (++times < lockWaitTimes) {
            if (acquire())
                return true;
            try {
                Thread.sleep(INTERVAL);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        }
        return false;
    }
}