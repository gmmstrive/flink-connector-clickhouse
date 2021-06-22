package pl.touk.flink.ignite.precondition;

public class Preconditions {

    public IgniteJdbcClient igniteClient(int port) {
        return new IgniteJdbcClient(port);
    }
}
