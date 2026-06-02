package uk.gov.companieshouse.search.api.elasticsearch;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;


import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.query_dsl.MatchQuery;
import org.opensearch.client.opensearch._types.query_dsl.Query;

import org.springframework.stereotype.Component;

@Component
public class AlphabeticalSearchQueries extends AbstractSearchQuery {

    public QueryBuilder createOrderedAlphaKeySearchQuery(String orderedAlphaKey) {

        QueryBuilder query = QueryBuilders.matchQuery("items.ordered_alpha_key", orderedAlphaKey);

        System.err.println("*** Query is ***");

        System.err.println(query);

        return query;
    }


    public Query createOrderedAlphaKeyOpenSearchQuery(String orderedAlphaKey) {

        Query query = MatchQuery.of(m -> m
                .field("items.ordered_alpha_key")
                .query(FieldValue.of(orderedAlphaKey))
        ).toQuery();


        System.err.println("*** Query is ***");
        System.err.println(query);

        return query;
    }


    public QueryBuilder createOrderedAlphaKeyKeywordQuery(String orderedAlphaKey) {

        QueryBuilder query = QueryBuilders.prefixQuery("items.ordered_alpha_key.keyword", orderedAlphaKey);

        System.err.println("*** Query is ***");

        System.err.println(query);

        return query;
    }

    public Query createOrderedAlphaKeyKeywordOpenSearchQuery(String orderedAlphaKey) {

        Query query = MatchQuery.of(m -> m
                .field("items.ordered_alpha_key.keyword")
                .query(FieldValue.of(orderedAlphaKey))
        ).toQuery();

        System.err.println("*** Query is ***");

        System.err.println(query);

        return query;
    }

    public QueryBuilder createStartsWithQuery(String corporateName) {

        QueryBuilder query = QueryBuilders.matchPhrasePrefixQuery("items.corporate_name.startswith", corporateName);

        System.err.println("*** Query is ***");

        System.err.println(query);

        return query;
    }

    public Query createStartsWithOpenSearchQuery(String corporateName) {

        Query query = MatchQuery.of(m -> m
                .field("items.corporate_name.startswith")
                .query(FieldValue.of(corporateName))
        ).toQuery();

        System.err.println("*** Query is ***");

        System.err.println(query);

        return query;
    }
}
