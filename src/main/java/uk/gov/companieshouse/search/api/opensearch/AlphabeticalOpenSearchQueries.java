package uk.gov.companieshouse.search.api.opensearch;

import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.query_dsl.MatchQuery;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.springframework.stereotype.Component;

@Component
public class AlphabeticalOpenSearchQueries extends AbstractOpenSearchQuery {

    public Query createOrderedAlphaKeySearchQuery(String orderedAlphaKey) {

        Query query = MatchQuery.of(m -> m
                .field("items.ordered_alpha_key")
                .query(FieldValue.of(orderedAlphaKey))
        ).toQuery();


        System.err.println("*** Query is ***");
        System.err.println(query);

        return query;
    }

    public Query createOrderedAlphaKeyKeywordQuery(String orderedAlphaKey) {

        Query query = MatchQuery.of(m -> m
                .field("items.ordered_alpha_key.keyword")
                .query(FieldValue.of(orderedAlphaKey))
        ).toQuery();

        System.err.println("*** Query is ***");

        System.err.println(query);

        return query;
    }

    public Query createStartsWithQuery(String corporateName) {

        Query query = MatchQuery.of(m -> m
                .field("items.corporate_name.startswith")
                .query(FieldValue.of(corporateName))
        ).toQuery();

        System.err.println("*** Query is ***");

        System.err.println(query);

        return query;
    }
}
