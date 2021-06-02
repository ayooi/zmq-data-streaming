package au.ooi.externals;

import java.time.Instant;

public interface TimeProvider {
    Instant now();
    void sleep(long milliseconds) throws InterruptedException;
}
