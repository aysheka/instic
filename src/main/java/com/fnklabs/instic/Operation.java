package com.fnklabs.instic;

import com.esotericsoftware.kryo.Kryo;
import com.fnklabs.draenei.CassandraClient;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.nio.file.Paths;

abstract class Operation {
    static final Kryo KRYO = new Kryo();

    final String keyspace;
    final boolean useDml;
    final boolean useDdl;

    final String host;
    final int port;
    final String username;
    final String password;

    private final String workingDirectory;

    Operation(String host, int port, String username, String password, String keyspace, String workingDirectory, boolean useDml, boolean useDdl) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.keyspace = keyspace;
        this.workingDirectory = workingDirectory;
        this.useDml = useDml;
        this.useDdl = useDdl;
    }

    @NotNull
    private String getDdlFilename() {
        return String.format("%s/%s.ddl", workingDirectory, keyspace);
    }

    @NotNull
    private String getDmlFilename() {
        return String.format("%s/%s.dml", workingDirectory, keyspace);
    }

    CassandraClient createCassandraClient() {
        return new CassandraClient(username, password, keyspace, host, port);
    }

    File getDdlFile() {
        return Paths.get(getDdlFilename()).toFile();
    }

    File getDmlFile() {
        return Paths.get(getDmlFilename()).toFile();
    }

    public abstract void perform();
}
