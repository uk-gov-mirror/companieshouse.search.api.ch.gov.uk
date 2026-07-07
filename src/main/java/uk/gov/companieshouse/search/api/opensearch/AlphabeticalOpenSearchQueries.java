package uk.gov.companieshouse.search.api.opensearch;

import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.query_dsl.MatchPhrasePrefixQuery;
import org.opensearch.client.opensearch._types.query_dsl.MatchQuery;
import org.opensearch.client.opensearch._types.query_dsl.PrefixQuery;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.search.api.logging.LoggingUtils;

@Component
public class AlphabeticalOpenSearchQueries extends AbstractOpenSearchQuery {

    public Query createOrderedAlphaKeySearchQuery(String orderedAlphaKey) {

        LoggingUtils.getLogger().info("Creating Ordered Alpha Key Search Query for OpenSearch");

        Query query = MatchQuery.of(m -> m
                .field("items.ordered_alpha_key")
                .query(FieldValue.of(orderedAlphaKey))
        ).toQuery();


        System.err.println("*** Query is match***");
        System.err.println(query);

        return query;
    }

    public Query createOrderedAlphaKeyKeywordQuery(String orderedAlphaKey) {

        LoggingUtils.getLogger().info("Creating Ordered Alpha Key Keyword Query for OpenSearch");


        Query query = PrefixQuery.of(p -> p
                .field("items.ordered_alpha_key.keyword")
                .value(orderedAlphaKey)
        ).toQuery();

        System.err.println("*** Query is prefix***");

        System.err.println(query);

        return query;
    }

    public Query createStartsWithQuery(String corporateName) {

        LoggingUtils.getLogger().info("Creating Starts With Query for OpenSearch");

        Query query = MatchPhrasePrefixQuery.of(mpp -> mpp
                        .field("items.corporate_name.startswith")
                        .query(corporateName)
                ).toQuery();

        System.err.println("*** Query is matchPhrasePrefix***");
        System.err.println(query);

        return query;
    }
}
