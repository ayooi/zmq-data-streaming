package au.ooi.streams;

import au.ooi.data.WriteConnectionDetail;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;
import java.util.UUID;


public class DataServiceWriter implements Runnable {
    private final String serviceName;
    private final WriteConnectionDetail detail;
    private ZMQ.Socket serviceSocket;
    private ZMQ.Socket dataSocket;
    private final ZContext ctx;
    private final String dataServiceLocatorUrl;

    public DataServiceWriter(String serviceName, WriteConnectionDetail detail, ZContext ctx, String dataServiceLocatorUrl) {
        this.serviceName = serviceName;
        this.detail = detail;
        this.ctx = ctx;
        this.dataServiceLocatorUrl = dataServiceLocatorUrl;
    }

    public void startup() {
        this.dataSocket = ctx.createSocket(SocketType.PUSH);
        this.dataSocket.setHWM(1000);
        this.dataSocket.bind(WriteConnectionDetail.listenUrl(this.detail));
        System.out.printf("[Writer] bound to %s%n", WriteConnectionDetail.listenUrl(this.detail));
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
        serviceSocket.send(WriteConnectionDetail.announcementUrl(this.detail));
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
