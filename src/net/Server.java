package net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by funeyu on 16/12/31.
 */
public class Server {

    private int port;
    private static Server singleServer;
    private AtomicBoolean listenning = new AtomicBoolean(true);

    public static Server initServer(int port) {
        if(singleServer == null) {
            singleServer = new Server();
        }

        singleServer.port = port;
        return singleServer;
    }

    private class Listenner extends Thread {
        private ServerSocketChannel acceptChannel = null;
        private Selector selector = null;

        public void run() {
            try {
                acceptChannel = ServerSocketChannel.open();
                acceptChannel.configureBlocking(false);
                selector = Selector.open();
                acceptChannel.socket().bind(new InetSocketAddress(port));
                acceptChannel.register(selector, SelectionKey.OP_ACCEPT);

                while(listenning.get()) {
                    int n = selector.select();
                    if(n == 0) { //没有指定的I/O事件发生
                        continue;
                    }
                    Iterator<SelectionKey> selectionKeys = selector.selectedKeys().iterator();
                    while(selectionKeys.hasNext()) {
                        SelectionKey key = selectionKeys.next();
                        if(key.isAcceptable()) {
                            ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
                            SocketChannel sc = ssc.accept();
                            sc.configureBlocking(false);
                            sc.register(selector, SelectionKey.OP_READ);
                        }

                        if(key.isReadable() && key.isValid()) {
                            doRead(key);
                        }

                        selectionKeys.remove();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void doRead(SelectionKey key) throws IOException {

        }
    }
}
