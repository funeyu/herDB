package net;

import utils.Bytes;
import utils.NumberPacker;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.UUID;

/**
 * Created by funeyu on 17/1/1.
 */
public class Client {

    // 生成一个uuid标识该client;每次都将该id传送给server,用来校验等操作
    private String uuid ;
    private Socket socket;
    private InputStream in;
    private OutputStream os;

    private Client(Socket socket, String uuid, InputStream in, OutputStream os) {
        this.socket = socket;
        this.uuid = uuid;
        this.in = in;
        this.os = os;
    }

    public static Client initClient(int port, String host) throws IOException{
        Socket socket = new Socket(host, port);
        String uuid = UUID.randomUUID().toString();
        InputStream in = socket.getInputStream();
        OutputStream os = socket.getOutputStream();
        Client client = new Client(socket, uuid, in, os);
        return client;
    }

    public String get(String key) throws IOException{
        sendCommand(("get,"+ key).getBytes());
        return read();
    }

    public void put(String key, String value) {
        sendCommand(("put,"+ key + "," + value).getBytes());
    }

    public void auth(String token) throws IOException{
        sendCommand(("auth," + token).getBytes());
        String re = read();
        System.out.println(re);
    }

    public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendCommand(byte[] commandInfo) {

        byte[] callInfo = Bytes.join(uuid.getBytes(), Bytes.join(NumberPacker.packInt(commandInfo.length), commandInfo));

        try {
            os.write(callInfo);
            os.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String read() throws IOException{
        try {
            byte[] lenBytes = new byte[4];
            in.read(lenBytes);
            int bytesLen = NumberPacker.unpackInt(lenBytes);
            byte[] rawContent = new byte[bytesLen];
            String result = new String(rawContent);
            return result;

        } catch (IOException e) {
            e.printStackTrace();

            return "error: " + e.toString();
        }
    }

    public static void main(String[]args) {
        System.out.println(UUID.randomUUID().toString());
        try {
            Client client = Client.initClient(8888, "127.0.0.1");
            client.auth("funer");
//            client.put("fuheyu", "java c art");
            System.out.println(client.get("fuheyu"));

            client.close();
//            client.put("funer", "java, eclipse");
//            System.out.println(client.get("funer"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
