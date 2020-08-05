package com.google.bigquery;

import org.apache.commons.cli.CommandLine;
import org.junit.Test;

import static org.junit.Assert.*;

public class MainTest {

    final String resourcesPath = "src/test/resources/";
    final String queryOne = resourcesPath + "query1.sql";

    @Test
    public void testGetContentsOfFile() {
        String contents = Main.getContentsOfFile(queryOne);
        contents = contents.trim();
        assertEquals(contents, "SELECT 1 + 1");
    }

    @Test
    public void testGetContentsOfInvalidFile() {
        String contents = Main.getContentsOfFile(resourcesPath + "query0.sql");
        assertNull(contents);
    }

    @Test
    public void testNoArguments() {
        CommandLine command = Main.buildCommand(new String[0]);
        assertNull(command);
    }

    @Test
    public void testInvalidArgument() {
        CommandLine command = Main.buildCommand(new String[]{"-a"});
        assertNull(command);
    }

    @Test
    public void testOneQueryArgument() {
        CommandLine command = Main.buildCommand(String.format("-q %s", queryOne).split(" "));
        assertArrayEquals(command.getOptionValues('q'), new String[]{queryOne});
    }

    @Test
    public void testTwoQueryArguments() {
        CommandLine command = Main.buildCommand(String.format("-q %s %s", queryOne, queryOne).split(" "));
        assertArrayEquals(command.getOptionValues('q'), new String[]{queryOne, queryOne});
    }
}