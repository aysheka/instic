package com.fnklabs.instic;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.esotericsoftware.kryo.io.Input;
import com.fnklabs.draenei.CassandraClient;
import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;

class ImportOperation extends Operation {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExportOperation.class);
    private static final Splitter SPLITTER = Splitter.on(";");

    ImportOperation(String host, int port, String username, String password, String keyspace, String workingDirectory, boolean createDml, boolean createDdl) {
        super(host, port, username, password, keyspace, workingDirectory, createDml, createDdl);
    }


    @Override
    public void perform() {
        if (useDdl) {
            importDdl();
        }

        if (useDml) {
            importData();
        }
    }


    private void importDdl() {
        CassandraClient cassandraClient = new CassandraClient(username, password, "system", host, port);

        try {
            byte[] bytes = Files.readAllBytes(getDdlFile().toPath());

            String ddl = new String(bytes, "UTF-8");

            LOGGER.debug("DDL: {}", ddl);

            List<String> queries = SPLITTER.splitToList(ddl);

            queries.forEach(query -> {
                LOGGER.debug("Executing query: `{}`", query.trim());

                if (!StringUtils.isEmpty(query.trim())) {
                    cassandraClient.execute(query);
                }
            });

        } catch (IOException e) {
            LOGGER.error("Can't import DDL", e);
        }
    }

    private void importData() {
        CassandraClient cassandraClient = createCassandraClient();

        int exportedRows = 0;

        try (Input input = new Input(new FileInputStream(getDmlFile()))) {

            while (!input.eof()) {
                RowData rowData = KRYO.readObject(input, RowData.class);

                TableMetadata tableMetadata = cassandraClient.getTableMetadata(keyspace, rowData.getTable());

                Insert insert = createInsertStmt(rowData, tableMetadata);

                PreparedStatement preparedStatement = cassandraClient.prepare(keyspace, insert.getQueryString());

                BoundStatement boundStatement = preparedStatement.bind();

                for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                    byte[] value = rowData.getValue(columnMetadata.getName());
                    boundStatement.setBytesUnsafe(columnMetadata.getName(), value == null ? null : ByteBuffer.wrap(value));
                }

                try {
                    cassandraClient.execute(boundStatement);
                } catch (Exception e) {
                    LOGGER.warn("Cant execute stmt", e);
                }

                exportedRows++;

                if (exportedRows % 1000 == 0) {
                    LOGGER.debug("Executing query: {}", insert.getQueryString());
                    LOGGER.debug("Rows was exported: {}", exportedRows);
                }
            }


        } catch (IOException e) {
            LOGGER.warn("Can't import data", e);
        }
    }

    @NotNull
    private Insert createInsertStmt(RowData rowData, TableMetadata tableMetadata) {
        Insert insert = QueryBuilder.insertInto(rowData.getTable());

        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            insert.value(columnMetadata.getName(), QueryBuilder.bindMarker());
        }
        return insert;
    }

}
