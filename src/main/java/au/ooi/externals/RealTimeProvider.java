package au.ooi.externals;

import java.time.Instant;

public class RealTimeProvider implements TimeProvider {
    @Override
    public Instant now() {
        return Instant.now();
    }

    @Override
    public void sleep(long milliseconds) throws InterruptedException {
        Thread.sleep(milliseconds);
    }
}
