package pl.touk.flink.ignite;

import java.io.IOException;
import java.net.ServerSocket;

public class PortFinder {

    public static int getAvailablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
