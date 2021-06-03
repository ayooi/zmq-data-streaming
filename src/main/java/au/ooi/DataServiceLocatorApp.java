package au.ooi;

import au.ooi.externals.RealTimeProvider;
import au.ooi.streams.DataServiceLocator;
import au.ooi.streams.DataServiceLocatorExpiryNotifier;
import au.ooi.streams.ServiceStore;
import org.zeromq.ZContext;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataServiceLocatorApp {
    public static void main(String[] args) throws InterruptedException {
        RealTimeProvider timeProvider = new RealTimeProvider();
        ServiceStore serviceStore = new ServiceStore(30, timeProvider);
        ZContext ctx = new ZContext();
        ExecutorService executorService = Executors.newCachedThreadPool();

        DataServiceLocator dataServiceLocator = new DataServiceLocator(ctx, "tcp://localhost:19999", timeProvider, serviceStore);
        executorService.submit(dataServiceLocator);
        executorService.submit(serviceStore);
        DataServiceLocatorExpiryNotifier notifier = new DataServiceLocatorExpiryNotifier("tcp://localhost:19999", ctx, serviceStore);
        executorService.submit(notifier);

        while (true) {
            Thread.sleep(5000);
            // just keep looping
        }
    }
}
