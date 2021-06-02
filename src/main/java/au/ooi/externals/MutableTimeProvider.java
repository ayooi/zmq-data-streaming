package au.ooi.externals;

import java.time.Instant;

public class MutableTimeProvider implements TimeProvider {
    private Instant instant;

    public MutableTimeProvider(Instant time) {
        this.instant = time;
    }

    public void set(Instant instant) {
        this.instant = instant;
    }

    public void addSeconds(int seconds) {
        this.instant = this.instant.plusSeconds(seconds);
    }

    @Override
    public Instant now() {
        return this.instant;
    }

    @Override
    public void sleep(long milliseconds) throws InterruptedException{
        // dont do anything
    }
}
