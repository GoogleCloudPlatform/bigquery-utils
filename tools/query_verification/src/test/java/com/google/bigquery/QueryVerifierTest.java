package com.google.bigquery;

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class QueryVerifierTest {

    @Test
    public void testCompareResultsWithNoDifferences() {
        // First results list
        Set<List<Object>> resultSet = new HashSet<List<Object>>();
        resultSet.add(Arrays.asList(1L, 1.25, "value"));
        resultSet.add(Arrays.asList(2L, 2.25, "value"));
        List<QueryJobResults> firstResults = quickGenerateResults(resultSet);

        // Seconds results list
        resultSet = new HashSet<List<Object>>();
        resultSet.add(Arrays.asList(1L, 1.25, "value"));
        resultSet.add(Arrays.asList(2L, 2.25, "value"));
        List<QueryJobResults> secondResults = quickGenerateResults(resultSet);

        ResultDifferences differences = QueryVerifier.compareResults(firstResults, secondResults);

        assertTrue(differences.extraResults().isEmpty());
        assertTrue(differences.missingResults().isEmpty());
    }

    @Test
    public void testCompareResultsWithDifferences() {
        // First results list
        Set<List<Object>> resultSet = new HashSet<List<Object>>();
        resultSet.add(Arrays.asList(1L, 1.25, "value"));
        resultSet.add(Arrays.asList(2L, 2.25, "value"));
        List<QueryJobResults> firstResults = quickGenerateResults(resultSet);

        // Seconds results list
        resultSet = new HashSet<List<Object>>();
        resultSet.add(Arrays.asList(1L, 1.25, "value"));
        resultSet.add(Arrays.asList(3L, 3.25, "string"));
        List<QueryJobResults> secondResults = quickGenerateResults(resultSet);

        ResultDifferences differences = QueryVerifier.compareResults(firstResults, secondResults);

        assertEquals(differences.extraResults(), Arrays.asList(Arrays.asList(2L, 2.25, "value")));
        assertEquals(differences.missingResults(), Arrays.asList(Arrays.asList(3L, 3.25, "string")));
    }

    @Test
    public void testCompareResultsWithErrorResult() {
        Set<List<Object>> resultSet = new HashSet<List<Object>>();
        resultSet.add(null);

        QueryJobResults queryJobResults = QueryJobResults.create("", null, "Syntax Error", resultSet);

        List<QueryJobResults> results = new ArrayList<QueryJobResults>();
        results.add(queryJobResults);

        ResultDifferences differences = QueryVerifier.compareResults(results, results);

        assertTrue(differences.extraResults().isEmpty());
        assertTrue(differences.missingResults().isEmpty());
    }

    @Test
    public void testCompareResultsWithUnknownObjects() {
        Set<List<Object>> resultSet = new HashSet<List<Object>>();
        resultSet.add(Arrays.asList(1L, 1, new ArrayList<>(), null));
        List<QueryJobResults> results = quickGenerateResults(resultSet);

        ResultDifferences differences = QueryVerifier.compareResults(results, results);

        assertTrue(differences.extraResults().isEmpty());
        assertTrue(differences.missingResults().isEmpty());
    }

    private List<QueryJobResults> quickGenerateResults(Set<List<Object>> resultSet) {
        QueryJobResults queryJobResults = QueryJobResults.create("", null, null, resultSet);

        List<QueryJobResults> results = new ArrayList<QueryJobResults>();
        results.add(queryJobResults);

        return results;
    }

}
