package uk.gov.companieshouse.search.api.service.search.impl.alphabetical;

import com.sun.org.apache.bcel.internal.generic.MULTIANEWARRAY;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import uk.gov.companieshouse.environment.EnvironmentReader;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.search.api.model.alphakeyService.AlphaKeyResponse;
import uk.gov.companieshouse.search.api.service.search.AlphaKeyService;
import uk.gov.companieshouse.search.api.service.search.SearchRequestService;

import java.util.Arrays;

import static uk.gov.companieshouse.search.api.SearchApiApplication.APPLICATION_NAME_SPACE;

@Service
public class AlphabeticalSearchRequestService implements SearchRequestService {

    @Autowired
    private EnvironmentReader environmentReader;
    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private AlphaKeyService alphaKeyService;

    private static final String INDEX = "ALPHABETICAL_SEARCH_INDEX";

    private static final String RESULTS_SIZE = "ALPHABETICAL_SEARCH_RESULT_MAX";
    private static final int AGGS_HIGHEST_MATCH_SIZE = 1;
    private static final String HIGHEST_MATCH = "highest_match";

    private static final String ALPHABETICAL_SEARCH = "Alphabetical Search: ";

    private static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAME_SPACE);

    private static final String SPACE_CHARACTER = " ";
    // spaces and pipes around company endings help in finding exact matches
    private static final String COMPANY_NAME_ENDINGS = " AEIE | ANGHYFYNGEDIG | C.B.C | C.B.C. | C.C.C | C.C.C. " +
            "| C.I.C. | CBC | CBCN | CBP | CCC | CCG CYF | CCG CYFYNGEDIG | CIC | COMMUNITY INTEREST COMPANY " +
            "| COMMUNITY INTEREST P.L.C. | COMMUNITY INTEREST PLC | COMMUNITY INTEREST PUBLIC LIMITED COMPANY " +
            "| CWMNI BUDDIANT CYMUNEDOL | CWMNI BUDDIANT CYMUNEDOL C.C.C | CWMNI BUDDIANT CYMUNEDOL CCC " +
            "| CWMNI BUDDIANT CYMUNEDOL CYHOEDDUS CYFYNGEDIG | CWMNI BUDDSODDIA CHYFALAF NEWIDIOL " +
            "| CWMNI BUDDSODDIANT PENAGORED | CWMNI CELL GWARCHODEDIG | CWMNI CYFYNGEDIG CYHOEDDUS | CYF | CYF. " +
            "| CYFYNGEDIG | EEIG | EESV | EOFG | EOOS | EUROPEAN ECONOMIC INTEREST GROUPING | GEIE | GELE | ICVC " +
            "| INVESTMENT COMPANY WITH VARIABLE CAPITAL | L.P. | LIMITED | LIMITED LIABILITY PARTNERSHIP " +
            "| LIMITED PARTNERSHIP | LLP | LP | LTD | LTD. | OEIC | OPEN-ENDED INVESTMENT COMPANY | P.C. | P.L.C " +
            "| P.L.C. | PAC | PARTNERIAETH ATEBOLRWYDD CYFYNGEDIG | PARTNERIAETH CYFYNGEDIG | PC | PCC LIMITED " +
            "| PCC LTD | PLC | PROTECTED CELL COMPANY | PUBLIC LIMITED COMPANY | SE | UNLIMITED | UNLTD | UNLTD. ";

   /**
     * {@inheritDoc}
     */
    @Override
    public SearchRequest createSearchRequest(String corporateName, String requestId) {

        LOG.info(ALPHABETICAL_SEARCH + "Creating search request for: " + corporateName
                + " for user with Id: " + requestId);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(environmentReader.getMandatoryString(INDEX));
        searchRequest.preference(requestId);
        searchRequest.source(createSource(corporateName));

        return searchRequest;
    }

    private SearchSourceBuilder createSource(String corporateName) {

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(Integer.parseInt(environmentReader.getMandatoryString(RESULTS_SIZE)));
        sourceBuilder.query(createAlphabeticalSearchQuery(corporateName));
        sourceBuilder.aggregation(createAggregation(HIGHEST_MATCH, AGGS_HIGHEST_MATCH_SIZE, corporateName));

        return sourceBuilder;
    }

    private AggregationBuilder createAggregation(String aggregationName, int size, String corporateName) {

        LOG.info(ALPHABETICAL_SEARCH + "Adding top hit aggregation for: " + corporateName);

        return AggregationBuilders
            .topHits(aggregationName)
            .size(size);
    }

    private String stripCorporateEnding(String corporateName){
        if (corporateName.contains(SPACE_CHARACTER)){
            int corpNameEndingStart = corporateName.lastIndexOf(SPACE_CHARACTER);
            String corpNameEnding = corporateName.substring(corpNameEndingStart).trim().toUpperCase();
            if (COMPANY_NAME_ENDINGS.contains(" " + corpNameEnding.toUpperCase() + " ")) {
                String corpNameSansEnding = corporateName.trim().substring(0, corporateName.lastIndexOf(SPACE_CHARACTER)).trim();
                return corpNameSansEnding;
            }
        }
        return corporateName;
    }

    private char previousCharacter(char c){
        int cint = (int)c;
        if ((cint > 48 && cint < 57) || (cint > 65 && cint < 90) || (cint > 97 && cint < 122)) {
            return (char) (cint - 1);
        }
        else if (cint == 65){
            return 'Z';
        }
        else if (cint == 97){
            return 'z';
        }
        else {
            return c;
        }
    }

    private QueryBuilder createAlphabeticalSearchQuery(String corporateName) {
        LOG.info(ALPHABETICAL_SEARCH + "Building query for: " + corporateName);

        // get alpha key value for corporate name
        AlphaKeyResponse alphaKeyResponse = alphaKeyService.getAlphaKeyForCorporateName(corporateName);
        String corporateNameAlphaKey = alphaKeyResponse.getSameAsAlphaKey();

        // Strip company name ending. Corporate name search against corporate_name_start works best in lower case.
        String corporateNameSansEnding = stripCorporateEnding(corporateName).toLowerCase();

        String corporateNameFirstPart = "", corporateNamePrevious, corporateNameFirstPartPrevious = "", corporateNamePrefix,
                corporateNameFirstPartPrefix, corporateNameAlphaPrefix, corporateNameFirstThreeLetters = "",
                corporateNameFirstThreeLettersPrevious = "";

        final int CORP_NAME_SANS_ENDING_LENGTH = corporateNameSansEnding.length();

        // Calculate and apply a trailing previous character
        // so matches above the best match are alphabetically descending
        if (corporateNameSansEnding.length() > 1) {
            corporateNamePrefix = corporateNameSansEnding.substring(0, CORP_NAME_SANS_ENDING_LENGTH-1);
            char prevChar = previousCharacter(corporateNameSansEnding.charAt(CORP_NAME_SANS_ENDING_LENGTH-1));
            corporateNamePrevious = corporateNamePrefix + prevChar;

            if (corporateNameSansEnding.length() > 3) {
                corporateNameFirstThreeLetters = corporateNameSansEnding.substring(0, 3);
                char prevCharFourthLetter = previousCharacter(corporateNameFirstThreeLetters.charAt(2));
                corporateNameFirstThreeLettersPrevious = corporateNameFirstThreeLetters.substring(0, 2) + prevCharFourthLetter;
            }
            if (corporateNameSansEnding.contains(SPACE_CHARACTER)) {
                corporateNameFirstPart = corporateNameSansEnding.substring(0, corporateNameSansEnding.indexOf(SPACE_CHARACTER));
                char prevCharFirstPart = previousCharacter(corporateNameFirstPart.charAt(corporateNameFirstPart.length()-1));
                corporateNameFirstPartPrefix = corporateNameFirstPart.substring(0, corporateNameFirstPart.length()-1);
                corporateNameFirstPartPrevious = corporateNameFirstPartPrefix + prevCharFirstPart;
            }
            else {
                corporateNameFirstPart = corporateNameSansEnding;
                corporateNameFirstPartPrevious = corporateNamePrevious;
            }
        }
        else {
            corporateNamePrefix = corporateNameSansEnding;
            char prevChar = previousCharacter(corporateNameSansEnding.charAt(0));
            corporateNamePrevious = "" + prevChar;
        }

        final int CORP_NAME_FIRST_PART_LENGTH = corporateNameFirstPart.length();
        final int CORP_NAME_FIRST_THREE_LETTERS_LENGTH = corporateNameFirstThreeLetters.length();
        final int CORP_NAME_ALPHA_KEY_LENGTH = corporateNameAlphaKey.length();
        final int CORP_NAME_PREFIX_LENGTH = corporateNamePrefix.length();

        if (CORP_NAME_ALPHA_KEY_LENGTH > 1) {
            corporateNameAlphaPrefix = corporateNameAlphaKey.substring(0, corporateNameAlphaKey.length()-1);
        }
        else {
            corporateNameAlphaPrefix = corporateNameAlphaKey;
        }

        BoolQueryBuilder query = QueryBuilders.boolQuery();

        if (CORP_NAME_SANS_ENDING_LENGTH == 1 && !corporateNameSansEnding.matches("([\\s?&?$?0-9a-zA-Z])+")) {
            if (corporateNamePrefix.equals("*")){
                corporateNamePrefix = "\\*";
            }
            query.should(QueryBuilders.wildcardQuery("items.corporate_name_start", corporateNamePrefix + "*"));
        }
        else {
            /// alpha key
            query.should(QueryBuilders.matchQuery("items.alpha_key", corporateNameAlphaKey).boost(5));
            query.should(QueryBuilders.prefixQuery("items.alpha_key", corporateNameAlphaKey).boost(5));
            query.should(QueryBuilders.wildcardQuery("items.alpha_key", corporateNameAlphaPrefix + "*"));
        }

        /// company name start
        query.should(QueryBuilders.regexpQuery("items.corporate_name_start", "^(" + corporateNamePrefix + ")([0-1])*([a-cA-C])*.*"));
        query.should(QueryBuilders.wildcardQuery("items.corporate_name_start.edge_ngram", corporateNameSansEnding + "*"));
        query.should(QueryBuilders.wildcardQuery("items.corporate_name_start.edge_ngram", corporateNamePrevious + "*"));
        query.should(QueryBuilders.wildcardQuery("items.corporate_name_start.edge_ngram", corporateNameSansEnding + "a*"));

        if (CORP_NAME_FIRST_THREE_LETTERS_LENGTH > 0) {
            query.should(QueryBuilders.wildcardQuery("items.corporate_name_start.edge_ngram", corporateNameFirstThreeLetters + "*"));
            query.should(QueryBuilders.wildcardQuery("items.corporate_name_start.edge_ngram", corporateNameFirstThreeLettersPrevious + "*"));
        }
        if (CORP_NAME_FIRST_PART_LENGTH > 1) {
            if (CORP_NAME_FIRST_PART_LENGTH < CORP_NAME_SANS_ENDING_LENGTH) {
                query.should(QueryBuilders.wildcardQuery("items.corporate_name_start.edge_ngram", corporateNameFirstPartPrevious + "*"));
                query.should(QueryBuilders.wildcardQuery("items.corporate_name_start.edge_ngram", corporateNameFirstPart + "*"));
            }
            query.should(QueryBuilders.regexpQuery("items.corporate_name_start", "^(" + corporateNameSansEnding + ")([0-1])*([a-cA-C])*.*"));
            query.filter(QueryBuilders.regexpQuery("items.corporate_name_start", "~([.*])" + corporateNamePrefix.substring(0, 2) + "([^ ]).*").boost(5));
        }
        System.out.println(query.toString());

        return query;
    }
}
