package au.ooi.streams;

import au.ooi.data.ExpiredServiceDetails;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * An annoying but effective companion class to the ServiceStoreInterface and DataServiceLocator which purely
 * handles sending deregister commands to the DataServiceLocator when the ServiceStoreInterface spits out expired events
 * based on timeout. This keeps the number of threads in the DataServiceLocator to just 1 but the reality is that we
 * wouldn't need this class visible outside of the DataServiceLocator with the @OnStartup annotation that we have
 * available to us in the real framework.
 */
public class DataServiceLocatorExpiryNotifier implements Runnable {
    private final ServiceStoreInterface serviceStoreInterface;
    private final ZMQ.Socket socket;

    public DataServiceLocatorExpiryNotifier(String url, ZContext ctx, ServiceStoreInterface serviceStoreInterface) {
        this.serviceStoreInterface = serviceStoreInterface;
        this.socket = ctx.createSocket(SocketType.DEALER);
        this.socket.setIdentity(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        this.socket.connect(url);
    }

    void process() throws InterruptedException {
        ExpiredServiceDetails take = this.serviceStoreInterface.take();
        new ZMsg().addLast("deregister")
                .addLast(take.getServiceName())
                .addLast(take.getLocation())
                .send(this.socket);
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
}
