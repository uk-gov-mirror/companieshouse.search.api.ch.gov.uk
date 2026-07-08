package uk.gov.companieshouse.search.api.opensearch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.opensearch.client.opensearch._types.query_dsl.Query;
import static org.junit.jupiter.api.Assertions.*;

class AlphabeticalOpenSearchQueriesTest {

    private AlphabeticalOpenSearchQueries queries;

    @BeforeEach
    void setUp() {
        queries = new AlphabeticalOpenSearchQueries();
    }

    // Positive Tests
    @Test
    @DisplayName("Should create MatchQuery when valid ordered alpha key is provided")
    void testCreateOrderedAlphaKeySearchQuery_Success() {
        String orderedAlphaKey = "ABC123";
        Query query = queries.createOrderedAlphaKeySearchQuery(orderedAlphaKey);

        assertNotNull(query, "Query should not be null");
        assertTrue(query.isMatch(), "Query should be a MatchQuery");
    }

    @Test
    @DisplayName("Should create PrefixQuery when valid ordered alpha key is provided")
    void testCreateOrderedAlphaKeyKeywordQuery_Success() {
        String orderedAlphaKey = "XYZ789";
        Query query = queries.createOrderedAlphaKeyKeywordQuery(orderedAlphaKey);

        assertNotNull(query, "Query should not be null");
        assertTrue(query.isPrefix(), "Query should be a PrefixQuery");
    }

    @Test
    @DisplayName("Should create MatchPhrasePrefixQuery for corporate name starts with search")
    void testCreateStartsWithQuery_Success() {
        String corporateName = "Acme Corporation";
        Query query = queries.createStartsWithQuery(corporateName);

        assertNotNull(query, "Query should not be null");
        assertTrue(query.isMatchPhrasePrefix(), "Query should be a MatchPhrasePrefixQuery");
    }


    @Test
    @DisplayName("Should create MatchAllQuery for match all searches")
    void testCreateMatchAllQuery_Success() {
        Query query = queries.createMatchAllQuery();

        assertNotNull(query, "Query should not be null");
        assertTrue(query.isMatchAll(), "Query should be a MatchAllQuery");
    }

    // Negative Tests
    @Test
    @DisplayName("Should throw MissingRequiredPropertyException when null ordered alpha key is provided")
    void testCreateOrderedAlphaKeySearchQuery_WithNull() {
        assertThrows(org.opensearch.client.util.MissingRequiredPropertyException.class, () -> {
            queries.createOrderedAlphaKeySearchQuery(null);
        }, "Should throw MissingRequiredPropertyException for null input");
    }

    @Test
    @DisplayName("Should handle empty ordered alpha key gracefully")
    void testCreateOrderedAlphaKeyKeywordQuery_WithEmpty() {
        String emptyKey = "";
        Query query = queries.createOrderedAlphaKeyKeywordQuery(emptyKey);

        assertNotNull(query, "Query should handle empty strings");
    }

    @Test
    @DisplayName("Should throw MissingRequiredPropertyException when null corporate name is provided")
    void testCreateStartsWithQuery_WithNull() {
        assertThrows(org.opensearch.client.util.MissingRequiredPropertyException.class, () -> {
            queries.createStartsWithQuery(null);
        }, "Should throw MissingRequiredPropertyException for null corporate name");
    }

}
