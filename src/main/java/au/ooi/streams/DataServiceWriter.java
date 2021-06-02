package au.ooi.streams;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;
import java.util.UUID;


public class DataServiceWriter implements Runnable {
    private final String serviceName;
    private ZMQ.Socket serviceSocket;
    private ZMQ.Socket dataSocket;
    private final ZContext ctx;
    private final String dataServiceUrl;
    private final String dataServiceLocatorUrl;

    public DataServiceWriter(String serviceName, String dataServiceUrl, ZContext ctx, String dataServiceLocatorUrl) {
        this.serviceName = serviceName;
        this.dataServiceUrl = dataServiceUrl;
        this.ctx = ctx;
        this.dataServiceLocatorUrl = dataServiceLocatorUrl;
    }

    public void startup() {
        this.dataSocket = ctx.createSocket(SocketType.PUSH);
        this.dataSocket.setHWM(1000);
        this.dataSocket.bind(this.dataServiceUrl);
        System.out.printf("[Writer] bound to %s%n", this.dataServiceUrl);
        this.serviceSocket = ctx.createSocket(SocketType.DEALER);
        this.serviceSocket.setIdentity(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        this.serviceSocket.connect(dataServiceLocatorUrl);
    }

    public void put(byte[] type) {
        this.dataSocket.send(type);
    }

    public void process() {
        serviceSocket.sendMore("register");
        serviceSocket.sendMore(this.serviceName);
        serviceSocket.send(this.dataServiceUrl);
    }

    @Override
    public void run() {
        while (true) {
            process();
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // ignored
            }
        }
    }
}
