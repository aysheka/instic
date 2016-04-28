package com.fnklabs.instic;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.esotericsoftware.kryo.io.Output;
import com.fnklabs.draenei.CassandraClient;
import com.google.common.base.Verify;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

class ExportOperation extends Operation {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExportOperation.class);

    ExportOperation(String host, int port, String username, String password, String keyspace, String workingDirectory, boolean createDml, boolean createDdl) {
        super(host, port, username, password, keyspace, workingDirectory, createDml, createDdl);
    }


    @Override
    public void perform() {
        CassandraClient cassandraClient = createCassandraClient();

        KeyspaceMetadata keyspaceMetadata = cassandraClient.getKeyspaceMetadata(keyspace);

        if (useDdl) {
            exportDdl(keyspaceMetadata);
        }

        if (useDml) {
            exportData(keyspaceMetadata, cassandraClient);
        }
    }

    private void exportDdl(KeyspaceMetadata keyspaceMetadata) {
        File file = getDdlFile();

        if (file.exists()) {
            LOGGER.debug("File is exists {}. Try to remove it", file.getAbsolutePath());
            Verify.verify(file.delete());

            try {
                Verify.verify(file.createNewFile());
            } catch (IOException e) {
                LOGGER.error("Can't create ddl file", e);
            }
        }

        String ddlQuery = keyspaceMetadata.exportAsString();

        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            fileOutputStream.write(ddlQuery.getBytes());
            LOGGER.debug("DDL query was successfully exported");
        } catch (IOException e) {
            LOGGER.error("Can't export schema", e);
        }
    }

    private int exportData(KeyspaceMetadata keyspaceMetadata, CassandraClient cassandraClient) {
        File dmlFile = getDmlFile();

        if (dmlFile.exists()) {
            LOGGER.debug("File is exists {}. Try to remove it", dmlFile.getAbsolutePath());
            Verify.verify(dmlFile.delete());
        }

        try {
            Verify.verify(dmlFile.createNewFile());
        } catch (IOException e) {
            LOGGER.error("Can't create dml file", e);
        }


        int totalExportedRows = 0;

        Collection<TableMetadata> tables = keyspaceMetadata.getTables();

        try (Output output = new Output(new BufferedOutputStream(new FileOutputStream(dmlFile), 64 * 1024 * 1024))) {
            for (TableMetadata tableMetadata : tables) {
                int tableExportedRows = exportTableData(tableMetadata, cassandraClient, output);

                totalExportedRows += tableExportedRows;

                LOGGER.debug("Exporting rows {}/{} from {} was successfully completed", tableExportedRows, totalExportedRows, tableMetadata.getName());
            }
        } catch (IOException e) {
            LOGGER.error("Cant' export data", e);
        }

        return totalExportedRows;
    }

    private int exportTableData(TableMetadata tableMetadata, CassandraClient cassandraClient, Output output) throws IOException {
        int tableExportedRows = 0;

        LOGGER.debug("Exporting data from {}", tableMetadata.getName());

        Statement stmt = QueryBuilder.select()
                                     .from(tableMetadata.getKeyspace().getName(), tableMetadata.getName())
                                     .setConsistencyLevel(ConsistencyLevel.ONE);

        ResultSet execute = cassandraClient.execute(tableMetadata.getKeyspace().getName(), stmt);

        for (Row row : execute) {
            tableExportedRows++;

            RowData rowData = new RowData(tableMetadata.getName());

            for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                ByteBuffer bytesUnsafe = row.getBytesUnsafe(columnMetadata.getName());

                rowData.addValue(columnMetadata.getName(), bytesUnsafe != null ? bytesUnsafe.array() : null);
            }

            KRYO.writeObject(output, rowData);

            if (tableExportedRows % 10000 == 0) {
                output.flush();
                LOGGER.debug("Export {} rows from {}", tableExportedRows, tableMetadata.getName());
            }
        }


        return tableExportedRows;
    }
}
