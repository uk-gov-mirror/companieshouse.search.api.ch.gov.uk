package uk.gov.companieshouse.search.api.opensearch;

import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.model.company.CompanyProfileApi;

@Component
public class AlphabeticalOpenSearchUpsertRequest {

    private static final String ID = "ID";
    private static final String COMPANY_TYPE = "company_type";
    private static final String ITEMS = "items";
    private static final String COMPANY_NUMBER = "company_number";
    private static final String COMPANY_STATUS = "company_status";
    private static final String CORPORATE_NAME = "corporate_name";
    private static final String RECORD_TYPE = "record_type";
    private static final String RECORD_TYPE_VALUE = "companies";
    private static final String LINKS = "links";
    private static final String SELF = "self";
    private static final String ORDERED_ALPHA_KEY = "ordered_alpha_key";
    private static final String ORDERED_ALPHA_KEY_WITH_ID = "ordered_alpha_key_with_id";

    public Map<String, Object> buildRequest(
            CompanyProfileApi company,
            String orderedAlphaKey,
            String orderedAlphaKeyWithID) {

        Map<String, Object> root = new HashMap<>();

        root.put(ID, company.getCompanyNumber());
        root.put(ORDERED_ALPHA_KEY_WITH_ID, orderedAlphaKeyWithID);
        root.put(COMPANY_TYPE, company.getType());

        // items object
        Map<String, Object> items = new HashMap<>();
        items.put(COMPANY_NUMBER, company.getCompanyNumber());
        items.put(ORDERED_ALPHA_KEY, orderedAlphaKey);
        items.put(COMPANY_STATUS, company.getCompanyStatus());
        items.put(CORPORATE_NAME, company.getCompanyName());
        items.put(RECORD_TYPE, RECORD_TYPE_VALUE);

        root.put(ITEMS, items);

        // links object
        Map<String, Object> links = new HashMap<>();
        links.put(SELF, company.getLinks().get(SELF));

        root.put(LINKS, links);

        return root;
    }
}
