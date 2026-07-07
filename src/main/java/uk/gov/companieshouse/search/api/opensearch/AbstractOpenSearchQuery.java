package uk.gov.companieshouse.search.api.opensearch;

import org.opensearch.client.opensearch._types.query_dsl.MatchAllQuery;
import org.opensearch.client.opensearch._types.query_dsl.Query;

public abstract class AbstractOpenSearchQuery {

    abstract Query createOrderedAlphaKeySearchQuery(String orderedAlphaKey);

    abstract Query createOrderedAlphaKeyKeywordQuery(String orderedAlphaKey);

    abstract Query createStartsWithQuery(String corporateName);

    public Query createMatchAllQuery() {
        return MatchAllQuery.of(m -> m).toQuery();
    }

}
