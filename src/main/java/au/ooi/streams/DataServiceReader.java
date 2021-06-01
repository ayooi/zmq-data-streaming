package au.ooi.streams;

import org.zeromq.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DataServiceReader implements Runnable {

    private final ZMQ.Socket serviceSocket;
    private final ZMQ.Socket dataSocket;

    private final String serviceName;
    private final int readerCount;
    private Set<String> locations = new HashSet<>();

    public DataServiceReader(String serviceName, ZContext ctx, int readerCount, String dataServiceLocatorUrl) {
        this.serviceName = serviceName;
        this.readerCount = readerCount;
        this.serviceSocket = ctx.createSocket(SocketType.REQ);
        this.serviceSocket.connect(dataServiceLocatorUrl);
        this.dataSocket = ctx.createSocket(SocketType.PULL);

        // Probably yoink this out somehow.
        new Thread(() -> {
            serviceSocket.sendMore("query");
            serviceSocket.send(this.serviceName);
            ZMsg msg = ZMsg.recvMsg(serviceSocket);
            ZFrame frame = msg.poll();
            Set<String> locations = new HashSet<>();
            while (frame != null) {
                String location = frame.getString(ZMQ.CHARSET);
                locations.add(location);
                frame = msg.poll();
            }
            doConnects(locations);
            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                // ignored
            }
        }).start();
    }

    // Check if we need to adjust what we're connected to
    private void doConnects(Set<String> incoming) {
        List<String> toConnect = new ArrayList<>();
        for (String location : this.locations) {
            if (this.locations.contains(location)) {
                toConnect.add(location);
            }
        }
        List<String> toDisconnect = new ArrayList<>();
        for (String location : this.locations) {
            if (!incoming.contains(location)) {
                toDisconnect.add(location);
            }
        }

        // may have to lock around this
        if (!toDisconnect.isEmpty() || !toConnect.isEmpty()) {
            for (String location : toDisconnect) {
                this.dataSocket.disconnect(location);
                System.out.printf("[Writer] Disconnecting from old provider %s%n", location);
            }
            for (String location : toConnect) {
                this.dataSocket.connect(location);
                System.out.printf("[Writer] Connecting to new provider %s%n", location);
            }
        }
        this.locations = incoming;
    }

    @Override
    public void run() {
        while (true) {
            System.out.printf("[Reader-%d] %s%n", this.readerCount, dataSocket.recvStr());
        }
    }

}
