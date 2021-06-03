package au.ooi.streams;

import lombok.Getter;
import org.zeromq.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

public class DataServiceReader implements Runnable {

    private final ZMQ.Socket locatorSocket;
    private final ZMQ.Socket dataSocket;
    // Used to release blocking recv() calls so that connection changes can take place
    private final ZMQ.Socket controlSocket;
    public static final String THE_IGNORE_ME_MESSAGE = "###IgnoreMe###";

    private final String serviceName;
    @Getter
    private final Runnable runnable;
    private Set<String> locations = new HashSet<>();

    private final ArrayBlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(10000);

    public DataServiceReader(String serviceName, ZContext ctx, String dataServiceLocatorUrl) {
        this.serviceName = serviceName;
        locatorSocket = ctx.createSocket(SocketType.DEALER);
        // For now we don't really care what the socket is called as long as its something reasonably unique.
        // This should probably be replaced with information derived from the running application.
        String identity = UUID.randomUUID().toString();
        locatorSocket.setIdentity(identity.getBytes(StandardCharsets.UTF_8));
        locatorSocket.setSendTimeOut(5);
        System.out.println("Created reader with identity " + identity);
        locatorSocket.connect(dataServiceLocatorUrl);

        controlSocket = ctx.createSocket(SocketType.PUSH);
        String controlUrl = String.format("inproc://%s", UUID.randomUUID());
        controlSocket.bind(controlUrl);
        dataSocket = ctx.createSocket(SocketType.PULL);
        dataSocket.connect(controlUrl);

        this.runnable = () -> {
            ZMQ.Poller poller = ctx.createPoller(1);
            poller.register(locatorSocket, ZMQ.Poller.POLLIN);
            while (true) {
                try {
                    boolean send = new ZMsg()
                            .addLast("query")
                            .addLast(this.serviceName)
                            .send(locatorSocket);
                    if (!send) {
                        continue;
                    }
                    int poll = poller.poll(10000);
                    if (poll == -1) {
                        return;
                    }
                    if (poller.pollin(0)) {
                        ZMsg msg = ZMsg.recvMsg(locatorSocket);
                        ZFrame frame = msg.poll();
                        Set<String> locations = new HashSet<>();
                        while (frame != null) {
                            String location = frame.getString(ZMQ.CHARSET);
                            if (!location.isBlank()) {
                                locations.add(location);
                            }
                            frame = msg.poll();
                        }
                        this.doConnects(locations);
                    }
                } catch (ZMQException e) {
                    return;
                }
            }
        };
    }

    // Check if we need to adjust our connections
    private void doConnects(Set<String> incoming) {
        List<String> toConnect = new ArrayList<>();
        for (String location : incoming) {
            if (!locations.contains(location)) {
                toConnect.add(location);
            }
        }
        List<String> toDisconnect = new ArrayList<>();
        for (String location : locations) {
            if (!incoming.contains(location)) {
                toDisconnect.add(location);
            }
        }

        // Testing shows that no locking needs to be placed around the DataSocket. Connections can be altered even if
        // the data socket is in a blocking receive but they will not take effect until the next time the blocking
        // recv() is released, hence the need for a separate socket to pass a dummy message through.
        if (!toDisconnect.isEmpty() || !toConnect.isEmpty()) {
            for (String location : toDisconnect) {
                dataSocket.disconnect(location);
                System.out.printf("[Reader] Disconnecting from old provider %s%n", location);
            }
            for (String location : toConnect) {
                dataSocket.connect(location);
                System.out.printf("[Reader] Connecting to new provider %s%n", location);
            }
            // necessary to resolve any blocking .recv() calls so that the changes take hold even when there is not data
            controlSocket.send(THE_IGNORE_ME_MESSAGE);
        }
        locations = incoming;
    }

    @Override
    public void run() {
        while (true) {
            try {
                process();
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    void process() throws InterruptedException {
        byte[] recv = dataSocket.recv();
        // I'm sure this could be optimised to simply send through a unique set of bytes to compare instead of
        // constructing a string. It would probably require all outgoing data to be prefixed with a byte to denote
        // whether it is a control or data payload.
        String s = new String(recv);
        if (s.equals(THE_IGNORE_ME_MESSAGE)) {
            return;
        }
        queue.put(recv);
    }

    public boolean hasData() {
        return !queue.isEmpty();
    }

    public byte[] take() throws InterruptedException {
        return queue.take();
    }

}
