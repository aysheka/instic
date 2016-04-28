package com.fnklabs.instic;

import com.google.common.base.Verify;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.jetbrains.annotations.Nullable;

public class Application {

    /**
     * @param args
     */
    public static void main(String[] args) {
        if (args.length > 0) {
            String operationType = args[0];

            Operation operation = getOperation(operationType);

            Verify.verifyNotNull(operation);

            operation.perform();
        } else {
            System.err.println("Specify operation type");
        }

        System.exit(0);
    }

    @Nullable
    private static Operation getOperation(String operationType) {
        Config config = ConfigFactory.load();

        if (operationType.equals("import")) {
            return new ImportOperation(
                config.getString("cassandra.host"),
                config.getInt("cassandra.port"),
                config.getString("cassandra.username"),
                config.getString("cassandra.password"),
                config.getString("cassandra.keyspace"),
                config.getString("working_dir"),
                config.getBoolean("import.dml"),
                config.getBoolean("import.ddl")
            );
        } else if (operationType.equals("export")) {
            return new ExportOperation(
                config.getString("cassandra.host"),
                config.getInt("cassandra.port"),
                config.getString("cassandra.username"),
                config.getString("cassandra.password"),
                config.getString("cassandra.keyspace"),
                config.getString("working_dir"),
                config.getBoolean("export.dml"),
                config.getBoolean("export.ddl")
            );
        }

        return null;
    }
}
