package com.example.springbatchintegrationsample;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

class Main {

    @Test
    void test() {

        final URI uri = URI.create("files:///D:/Workspaces/IdeaProjects/spring-batch-integration-sample/files");

        final Path path = Paths.get("D:\\Workspaces\\IdeaProjects\\spring-batch-integration-sample\\files\\fuck");

        System.out.println(path);

    }
}