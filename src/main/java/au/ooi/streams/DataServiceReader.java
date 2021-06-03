package au.ooi.streams;

import lombok.Getter;
import org.zeromq.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

public class DataServiceReader implements Runnable {

    public static final String THE_IGNORE_ME_MESSAGE = "###IgnoreMe###";
    private final ZMQ.Socket serviceSocket;
    private final ZMQ.Socket dataSocket;
    private final ZMQ.Socket controlSocket;

    private final String serviceName;
    @Getter
    private final Runnable runnable;
    private Set<String> locations = new HashSet<>();

    private final ArrayBlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(10000);

    public DataServiceReader(String serviceName, ZContext ctx, String dataServiceLocatorUrl) {
        this.serviceName = serviceName;
        serviceSocket = ctx.createSocket(SocketType.DEALER);
        String identity = UUID.randomUUID().toString();
        serviceSocket.setIdentity(identity.getBytes(StandardCharsets.UTF_8));
        serviceSocket.setSendTimeOut(5);
        System.out.println("Created reader with identity " + identity);
        serviceSocket.connect(dataServiceLocatorUrl);

        controlSocket = ctx.createSocket(SocketType.PUSH);
        String controlUrl = String.format("inproc://%s", UUID.randomUUID());
        controlSocket.bind(controlUrl);
        dataSocket = ctx.createSocket(SocketType.PULL);
        dataSocket.connect(controlUrl);

        // Probably yoink this out somehow.
        this.runnable = () -> {
            ZMQ.Poller poller = ctx.createPoller(1);
            poller.register(serviceSocket, ZMQ.Poller.POLLIN);
            ZMsg queryMsg = new ZMsg();
            queryMsg.add("query");
            queryMsg.add(this.serviceName);

            while (true) {
                try {
                    boolean send = queryMsg.send(serviceSocket);
                    if (!send) {
                        continue;
                    }
                    int poll = poller.poll(10000);
                    if (poll == -1) {
                        return;
                    }
                    if (poller.pollin(0)) {
                        ZMsg msg = ZMsg.recvMsg(serviceSocket);
                        ZFrame frame = msg.poll();
                        Set<String> locations = new HashSet<>();
                        while (frame != null) {
                            String location = frame.getString(ZMQ.CHARSET);
                            if (!location.isBlank()) {
                                locations.add(location);
                            }
                            frame = msg.poll();
                        }
                        doConnects(locations);
                    }
                } catch (ZMQException e) {
                    return;
                }
            }
        };
    }

    // Check if we need to adjust what we're connected to
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
