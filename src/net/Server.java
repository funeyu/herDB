package net;

import herdb.HerDB;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by funeyu on 16/12/31.
 */
public class Server {

    private int port;
    private static Server singleServer;
    private AtomicBoolean listenning = new AtomicBoolean(true);
    // 记录clients的认证信息
    private static ConcurrentHashMap<String, Boolean> authedClients = new ConcurrentHashMap<>();

    private static HerDB dbStore;

    public static Server initServer(int port) {
        try {
            dbStore = HerDB.bootStrap(true, 8192);
        } catch (Exception e) {
            System.out.println("初始Server出错了");
            e.printStackTrace();
        }

        if(singleServer == null) {
            singleServer = new Server();
        }

        singleServer.port = port;
        return singleServer;
    }

    public void start() {
        new Listenner().start();
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

        /**
         * 读取Client 端的command信息
         * @param key
         * @throws IOException
         */
        private void doRead(SelectionKey key) throws IOException {
            SocketChannel sc = (SocketChannel) key.channel();

            try {
                ByteBuffer uuidBuffer = ByteBuffer.allocate(32);
                sc.read(uuidBuffer);
                String uuid = uuidBuffer.toString();

                ByteBuffer lenBuffer = ByteBuffer.allocate(4);
                sc.read(lenBuffer);
                lenBuffer.flip();
                int dataLen = lenBuffer.getInt();

                ByteBuffer content = ByteBuffer.allocate(dataLen);
                sc.read(content);

                handleCommand(sc, content.array(), uuid);
            } catch (Exception e) {
                key.cancel();
                sc.socket().close();
                e.printStackTrace();
            }
        }

        /**
         * 处理Client端的command请求,如果已经验证过了,就进行,否则直接关闭socket
         * commands 信息格式为:
         *                   'commandType(get or set or auth),commandParam'
         * @param channel
         * @param commands
         * @param uuid
         */
        private void handleCommand(SocketChannel channel, byte[] commands, String uuid) throws IOException{
            String[] commandsInfo = new String(commands).split(",");
            // 命令行数据格式不对
            if(commandsInfo.length == 0 || commandsInfo.length > 2) {
                byte[] error = "wrong command info".getBytes();
                channel.write(ByteBuffer.wrap(error));
                channel.socket().close();
                return ;
            }

            if (!authedClients.get(uuid)) {
                if (commandsInfo[0].equals("auth")) {
                    if(commandsInfo[1].equals("funer")) {
                        authedClients.put(uuid, Boolean.TRUE);
                    }
                }
                else {
                    channel.socket().close();
                }
            }

            // 重复认证
            if(commandsInfo[0].equals("auth")) {
                return ;
            }
            // get请求处理
            if(commandsInfo[0].equals("get")) {
                byte[] result = Commands.get(commandsInfo[1], dbStore);
                channel.write(ByteBuffer.wrap(result));
            }
            // put请求的处理
            if(commandsInfo[0].equals("put")) {
                Commands.put(commandsInfo[1], commandsInfo[2], dbStore);
                channel.write(ByteBuffer.wrap("success".getBytes()));
            }
        }
    }

    public static void main(String[]args) {
        Server server = Server.initServer(3000);
        server.start();
    }
}
