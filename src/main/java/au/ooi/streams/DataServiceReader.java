package au.ooi.streams;

import org.zeromq.*;
import zmq.ZError;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;

public class DataServiceReader implements Runnable {

    public static final String THE_IGNORE_ME_MESSAGE = "###IgnoreMe###";
    private final ZMQ.Socket serviceSocket;
    private final ZMQ.Socket dataSocket;
    private final ZMQ.Socket controlSocket;

    private final String serviceName;
    private Set<String> locations = new HashSet<>();

    private final ArrayBlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(10000);

    public DataServiceReader(String serviceName, ZContext ctx, String dataServiceLocatorUrl, ExecutorService executorService) {
        this.serviceName = serviceName;
        this.serviceSocket = ctx.createSocket(SocketType.DEALER);
        String identity = UUID.randomUUID().toString();
        this.serviceSocket.setIdentity(identity.getBytes(StandardCharsets.UTF_8));
        this.serviceSocket.setSendTimeOut(5);
        System.out.println("Created reader with identity " + identity);
        this.serviceSocket.connect(dataServiceLocatorUrl);

        this.controlSocket = ctx.createSocket(SocketType.PUSH);
        String controlUrl = String.format("inproc://%s", UUID.randomUUID());
        this.controlSocket.bind(controlUrl);
        this.dataSocket = ctx.createSocket(SocketType.PULL);
        this.dataSocket.connect(controlUrl);

        // Probably yoink this out somehow.
        executorService.submit(() -> {
            ZMQ.Poller poller = ctx.createPoller(1);
            poller.register(this.serviceSocket, ZMQ.Poller.POLLIN);
            while (true) {
                ZMsg queryMsg = new ZMsg();
                queryMsg.add("query");
                queryMsg.add(this.serviceName);
                try {
                    boolean send = queryMsg.send(this.serviceSocket);
                    if (!send) {
                        continue;
                    }
                    int poll = poller.poll(10000);
                    if(poll == -1) {
                        return;
                    }
                    if(poller.pollin(0)) {
                        ZMsg msg = ZMsg.recvMsg(this.serviceSocket);
                        ZFrame frame = msg.poll();
                        Set<String> locations = new HashSet<>();
                        while (frame != null) {
                            String location = frame.getString(ZMQ.CHARSET);
                            locations.add(location);
                            frame = msg.poll();
                        }
                        doConnects(locations);
                    }
                    else {
                        continue;
                    }
                } catch (ZMQException e) {
                    return;
                }
            }
        });
    }

    // Check if we need to adjust what we're connected to
    private void doConnects(Set<String> incoming) {
        List<String> toConnect = new ArrayList<>();
        for (String location : incoming) {
            if (!this.locations.contains(location)) {
                toConnect.add(location);
            }
        }
        List<String> toDisconnect = new ArrayList<>();
        for (String location : this.locations) {
            if (!incoming.contains(location)) {
                toDisconnect.add(location);
            }
        }

        if (!toDisconnect.isEmpty() || !toConnect.isEmpty()) {
            for (String location : toDisconnect) {
                this.dataSocket.disconnect(location);
                System.out.printf("[Reader] Disconnecting from old provider %s%n", location);
            }
            for (String location : toConnect) {
                this.dataSocket.connect(location);
                System.out.printf("[Reader] Connecting to new provider %s%n", location);
            }
            // necessary to resolve any blocking .recv() calls so that the changes take hold even when there is not data
            this.controlSocket.send(THE_IGNORE_ME_MESSAGE);
        }
        this.locations = incoming;
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
        return !this.queue.isEmpty();
    }

    public byte[] take() throws InterruptedException {
        return this.queue.take();
    }

}
