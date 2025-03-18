package Configuration;


import org.apache.flink.connector.jdbc.JdbcConnectionOptions;


public class DbConfiguration {

    private static JdbcConnectionOptions connectionOptions;
    public static JdbcConnectionOptions getConnectionOptions() {
        if (connectionOptions == null) {
            synchronized (DbConfiguration.class) {
                if (connectionOptions == null) {
                    connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .withUrl("jdbc:postgresql://localhost:5432/HIE_db")
                            .withDriverName("org.postgresql.Driver")
                            .withUsername("postgres")
                            .withPassword("root")
                            .build();
                }
            }
        }
        return connectionOptions;
    }
}
