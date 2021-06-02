package au.ooi.streams;

import java.time.Instant;

public class RealTimeProvider implements TimeProvider {
    @Override
    public Instant now() {
        return Instant.now();
    }
}
