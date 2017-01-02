package net;

import herdb.HerDB;
import utils.Bytes;
import utils.NumberPacker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
                    System.out.println("Size:" + selector.selectedKeys().size());
                    Iterator<SelectionKey> selectionKeys = selector.selectedKeys().iterator();
                    while(selectionKeys.hasNext()) {
                        SelectionKey key = selectionKeys.next();
                        selectionKeys.remove();

                        if(key.isAcceptable()) {
                            ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
                            SocketChannel sc = ssc.accept();
                            sc.configureBlocking(false);
                            sc.register(selector, SelectionKey.OP_READ);
                        }

                        if(key.isReadable() && key.isValid()) {
                            System.out.println("readable");
                            doRead(key);
                        }

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
                ByteBuffer uuidBuffer = ByteBuffer.allocate(36);
                int readed = sc.read(uuidBuffer);
                if(readed == -1) {  // 客户端关闭socket
                    sc.socket().close();
                    return ;
                }

                if(readed != 36) {
                    return ;
                }

                String uuid = new String(uuidBuffer.array());

                ByteBuffer lenBuffer = ByteBuffer.allocate(4);
                sc.read(lenBuffer);
                lenBuffer.flip();
                lenBuffer.order(ByteOrder.BIG_ENDIAN);
                int dataLen = lenBuffer.getInt();

                if(dataLen > 64 || dataLen < 0) {
                    sc.write(wrapOutData("comand payload too large"));
                    return ;
                }
                ByteBuffer content = ByteBuffer.allocate(dataLen);
                sc.read(content);

                handleCommand(key, content.array(), uuid);
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
         * @param key
         * @param commands
         * @param uuid
         */
        private void handleCommand(SelectionKey key, byte[] commands, String uuid) throws IOException{
            System.out.println(new String(commands));
            SocketChannel sc = (SocketChannel)key.channel();
            String[] commandsInfo = new String(commands).split(",");
            // 命令行数据格式不对
            if(commandsInfo.length == 0 || commandsInfo.length > 3) {
                sc.write(wrapOutData("wrong command info"));
                sc.socket().close();
                return ;
            }

            if (authedClients.get(uuid) == null) {
                if (commandsInfo[0].equals("auth")) {
                    if(commandsInfo[1].equals("funer")) {
                        System.out.println("funnnnnnnnnn");
                        authedClients.put(uuid, Boolean.TRUE);
                        sc.write(wrapOutData("success: in auth"));
                        key.interestOps(SelectionKey.OP_READ);
                    }
                }
                else {
                    sc.write(wrapOutData("error: in auth"));
                    key.interestOps(SelectionKey.OP_READ);
                    sc.socket().close();
                }

                return ;
            }

            // get请求处理
            if(commandsInfo[0].equals("get")) {
                String result = Commands.get(commandsInfo[1], dbStore);
                if(result == null) {
                    sc.write(wrapOutData("null"));
                    key.interestOps(SelectionKey.OP_READ);
                    return ;
                }
                System.out.println("get Result:" + result);
                sc.write(wrapOutData(result));
                key.interestOps(SelectionKey.OP_READ);
            }
            // put请求的处理
            if(commandsInfo[0].equals("put")) {
                System.out.println(commandsInfo.length);
                Commands.put(commandsInfo[1], commandsInfo[2], dbStore);
                sc.write(wrapOutData("success"));
                key.interestOps(SelectionKey.OP_READ);
            }
        }

        // 坑啊, 这里讲数据返回给client的时候加上size
        private ByteBuffer wrapOutData(String data) {
            byte[] rawBytes = data.getBytes();
            // 记录数据的长度
            byte[] dataLen = NumberPacker.packInt(rawBytes.length);
            byte[] wrapedBytes = Bytes.join(dataLen, rawBytes);
            ByteBuffer buffer = ByteBuffer.wrap(wrapedBytes);
            return buffer;
        }
    }

    public static void main(String[]args) {
        Server server = Server.initServer(8888);
        server.start();
    }
}
