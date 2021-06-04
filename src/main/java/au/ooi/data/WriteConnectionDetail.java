package au.ooi.data;

import lombok.NonNull;
import lombok.Value;

@Value
public class WriteConnectionDetail {
    WriteConnectionType type;
    String address;
    long listenPort;

    public static String listenUrl(WriteConnectionDetail detail) {
        switch (detail.getType()) {
            case TCP -> {
                return String.format("tcp://*:%s", detail.getListenPort());
            }
            case INPROC -> {
                return String.format("inproc://%s", detail.getAddress());
            }
            case IPC -> {
                return String.format("ipc://%s", detail.getAddress());
            }
        }
        // Can't get here.
        throw new IllegalStateException();
    }

    public static String announcementUrl(WriteConnectionDetail detail) {
        switch (detail.getType()) {
            case TCP -> {
                return String.format("tcp://%s:%s", detail.getAddress(), detail.getListenPort());
            }
            case INPROC -> {
                return String.format("inproc://%s", detail.getAddress());
            }
            case IPC -> {
                return String.format("ipc://%s", detail.getAddress());
            }
        }
        // Can't get here.
        throw new IllegalStateException();
    }

    public static WriteConnectionDetail parse(@NonNull String connectionUrl) {
        int i = connectionUrl.indexOf("://");
        if (i == -1) {
            throw new IllegalArgumentException("Not a valid URL");
        }

        WriteConnectionType protocolType = WriteConnectionType.valueOf(connectionUrl.substring(0, i).toUpperCase());
        switch (protocolType) {
            case TCP -> {
                String addressComponent = connectionUrl.substring(i + 3);
                int colonIndex = addressComponent.lastIndexOf(":");
                if (colonIndex == -1) {
                    throw new IllegalArgumentException("No listen port");
                }
                String address = addressComponent.substring(0, colonIndex);
                long port = Long.parseLong(addressComponent.substring(colonIndex + 1));
                return new WriteConnectionDetail(WriteConnectionType.TCP, address, port);
            }
            case INPROC -> {
                String addressComponent = connectionUrl.substring(i + 3);
                return new WriteConnectionDetail(WriteConnectionType.INPROC, addressComponent, 0);
            }
            case IPC -> {
                String addressComponent = connectionUrl.substring(i + 3);
                return new WriteConnectionDetail(WriteConnectionType.IPC, addressComponent, 0);
            }
        }
        // can't get here
        throw new IllegalStateException();
    }
}
