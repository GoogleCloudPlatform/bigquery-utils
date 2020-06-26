package com.google.bigquery;

import org.apache.commons.cli.CommandLine;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class MainTest {

    final String resourcesPath = "src/test/resources/";
    final String queryOne = resourcesPath + "query1.sql";

    @Test
    public void testGetContentsOfFile() {
        String contents = new Main().getContentsOfFile(queryOne);
        contents = contents.trim();
        assertEquals(contents, "SELECT 1 + 1;");
    }

    @Test
    public void testGetContentsOfInvalidFile() {
        String contents = new Main().getContentsOfFile(resourcesPath + "query0.sql");
        assertNull(contents);
    }

    @Test
    public void testNoArguments() {
        CommandLine command = new Main().buildCommand(new String[0]);
        assertNull(command);
    }

    @Test
    public void testInvalidArgument() {
        CommandLine command = new Main().buildCommand(new String[]{"-a"});
        assertNull(command);
    }

    @Test
    public void testOneQueryArgument() {
        CommandLine command = new Main().buildCommand(String.format("-q %s", queryOne).split(" "));
        assertArrayEquals(command.getOptionValues('q'), new String[]{queryOne});
    }

    @Test
    public void testTwoQueryArguments() {
        CommandLine command = new Main().buildCommand(String.format("-q %s %s", queryOne, queryOne).split(" "));
        assertArrayEquals(command.getOptionValues('q'), new String[]{queryOne, queryOne});
    }
}