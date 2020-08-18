package com.google.bigquery;

import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class QueryVerifierTest {

    @Test
    public void testCompareResultsWithNoDifferences() {
        // First results list
        List<QueryJobResults> firstResults = new ArrayList<QueryJobResults>();

        Set<List<Object>> resultSet = new HashSet<List<Object>>();
        resultSet.add(Arrays.asList(1L, 1.25, "value"));
        resultSet.add(Arrays.asList(2L, 2.25, "value"));
        resultSet.add(Arrays.asList(3L, 3.25, "value"));
        firstResults.add(QueryJobResults.create("", null, null, resultSet, null));

        resultSet = new HashSet<List<Object>>();
        resultSet.add(Arrays.asList(0L));
        resultSet.add(Arrays.asList(100L));
        firstResults.add(QueryJobResults.create("", null, null, resultSet, null));

        // Seconds results list
        List<QueryJobResults> secondResults = new ArrayList<QueryJobResults>();

        resultSet = new HashSet<List<Object>>();
        resultSet.add(Arrays.asList(1L, 1.25, "value"));
        resultSet.add(Arrays.asList(2L, 2.25, "value"));
        resultSet.add(Arrays.asList(3L, 3.25, "value"));
        secondResults.add(QueryJobResults.create("", null, null, resultSet, null));

        resultSet = new HashSet<List<Object>>();
        resultSet.add(Arrays.asList(0L));
        resultSet.add(Arrays.asList(100L));
        secondResults.add(QueryJobResults.create("", null, null, resultSet, null));

        List<ResultDifferences> differences = QueryVerifier.compareResults(firstResults, secondResults);

        assertEquals(differences.size(), 2);
        assertTrue(differences.get(0).missingResults().isEmpty());
        assertTrue(differences.get(0).extraResults().isEmpty());
        assertTrue(differences.get(1).missingResults().isEmpty());
        assertTrue(differences.get(1).extraResults().isEmpty());
    }

    @Test
    public void testCompareResultsWithDifferences() {
        // First results list
        List<QueryJobResults> firstResults = new ArrayList<QueryJobResults>();

        Set<List<Object>> resultSet = new HashSet<List<Object>>();
        resultSet.add(Arrays.asList(1L, 1.25, "value"));
        resultSet.add(Arrays.asList(2L, 2.25, "value"));
        resultSet.add(Arrays.asList(3L, 2.25, "value"));
        firstResults.add(QueryJobResults.create("", null, null, resultSet, null));

        resultSet = new HashSet<List<Object>>();
        resultSet.add(Arrays.asList(0L));
        resultSet.add(Arrays.asList(100L));
        firstResults.add(QueryJobResults.create("", null, null, resultSet, null));

        // Seconds results list
        List<QueryJobResults> secondResults = new ArrayList<QueryJobResults>();

        resultSet = new HashSet<List<Object>>();
        resultSet.add(Arrays.asList(1L, 1.25, 0L));
        resultSet.add(Arrays.asList(2L, 2.251, "value"));
        resultSet.add(Arrays.asList(3L, 3.25, "value"));
        secondResults.add(QueryJobResults.create("", null, null, resultSet, null));

        resultSet = new HashSet<List<Object>>();
        resultSet.add(Arrays.asList(100L));
        resultSet.add(Arrays.asList(200L));
        secondResults.add(QueryJobResults.create("", null, null, resultSet, null));

        List<ResultDifferences> differences = QueryVerifier.compareResults(firstResults, secondResults);

        assertEquals(differences.size(), 2);
        assertEquals(differences.get(0).missingResults().size(), 3);
        assertEquals(differences.get(0).extraResults().size(), 3);
        assertEquals(differences.get(1).missingResults().size(), 1);
        assertEquals(differences.get(1).extraResults().size(), 1);
    }

    @Test
    public void testCompareResultsWithErrorResult() {
        Set<List<Object>> resultSet = new HashSet<List<Object>>();
        resultSet.add(null);

        QueryJobResults queryJobResults = QueryJobResults.create("", null, "Syntax Error", resultSet, null);

        List<QueryJobResults> results = new ArrayList<QueryJobResults>();
        results.add(queryJobResults);

        List<ResultDifferences> differences = QueryVerifier.compareResults(results, results);

        assertEquals(differences.size(), 1);
        assertTrue(differences.get(0).extraResults().isEmpty());
        assertTrue(differences.get(0).missingResults().isEmpty());
    }

    @Test
    public void testCompareResultsWithUnknownObjects() {
        Set<List<Object>> resultSet = new HashSet<List<Object>>();
        resultSet.add(Arrays.asList(1L, 1, new ArrayList<>(), null));
        List<QueryJobResults> results = quickGenerateResults(resultSet);

        List<ResultDifferences> differences = QueryVerifier.compareResults(results, results);

        assertEquals(differences.size(), 1);
        assertTrue(differences.get(0).extraResults().isEmpty());
        assertTrue(differences.get(0).missingResults().isEmpty());
    }

    private List<QueryJobResults> quickGenerateResults(Set<List<Object>> resultSet) {
        QueryJobResults queryJobResults = QueryJobResults.create("", null, null, resultSet, null);

        List<QueryJobResults> results = new ArrayList<QueryJobResults>();
        results.add(queryJobResults);

        return results;
    }

}
