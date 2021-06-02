package au.ooi.example;

import au.ooi.streams.DataServiceLocator;
import au.ooi.streams.DataServiceReader;
import au.ooi.streams.DataServiceWriter;
import au.ooi.streams.RealTimeProvider;
import org.zeromq.ZContext;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimpleReaderWriter {

    public static final String SERVICE_LOCATOR_URL = "inproc://service-locator-url";
    public static final String SERVICE_NAME = "service-name";
    private static int count1 = 0;
    private static int count2 = 0;

    // Doesn't shutdown properly because I'm lazy :(
    public static void main(String[] args) throws InterruptedException {
        ZContext ctx = new ZContext();
        DataServiceLocator dataServiceLocator = new DataServiceLocator(ctx, SERVICE_LOCATOR_URL, 10, new RealTimeProvider());
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.submit(dataServiceLocator);

        DataServiceWriter writer = new DataServiceWriter(SERVICE_NAME, "inproc://data-url", ctx, SERVICE_LOCATOR_URL);
        writer.startup();
        executorService.submit(writer);

        DataServiceReader reader1 = new DataServiceReader(SimpleReaderWriter.SERVICE_NAME, ctx, SERVICE_LOCATOR_URL);
        executorService.submit(reader1);

        DataServiceReader reader2 = new DataServiceReader(SimpleReaderWriter.SERVICE_NAME, ctx, SERVICE_LOCATOR_URL);
        executorService.submit(reader2);

        Thread.sleep(1000);

        Instant start = Instant.now();
        new Thread(() -> {
            for (int i = 0; i < 10000000; i++) {
                writer.put("Payload".getBytes(StandardCharsets.UTF_8));
            }
        }).start();

        executorService.submit(() -> {
            try {
                do {
                    reader2.take();
                    count2++;
                } while (count2 + count1 < 10000000);
            } catch (InterruptedException e) {
                // ignored
            }
        });

        executorService.submit(() -> {
            try {
                do {
                    reader1.take();
                    count1++;
                } while (count2 + count1 < 10000000);
            } catch (InterruptedException e) {
                // ignored
            }
        });

        while ((count2 + count1) < 10000000) {
            // just spin
            Thread.sleep(50);
        }

        Instant end = Instant.now();
        float timeTakenMs = Math.abs(Duration.between(end, start).toMillis());
        System.out.printf("Took %02f milliseconds (%02f/pps) [count1 = %d, count2 = %d]%n",
                timeTakenMs,
                (count2 + count1) / (timeTakenMs / 1000),
                count1,
                count2);
        executorService.shutdownNow();
        ctx.close();
    }
}
