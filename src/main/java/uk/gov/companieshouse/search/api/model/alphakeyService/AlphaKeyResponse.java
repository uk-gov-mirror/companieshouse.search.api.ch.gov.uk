package uk.gov.companieshouse.search.api.model.alphakeyService;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Simple object which forms response to getAlphaKey REST call
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AlphaKeyResponse {

    String error;
    String sameAsAlphaKey;
    String orderedAlphaKey;
    String upperCaseName;

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getSameAsAlphaKey() {
        return sameAsAlphaKey;
    }

    public void setSameAsAlphaKey(String sameAsAlphaKey) {
        this.sameAsAlphaKey = sameAsAlphaKey;
    }

    public String getOrderedAlphaKey() {
        return orderedAlphaKey;
    }

    public void setOrderedAlphaKey(String orderedAlphaKey) {
        this.orderedAlphaKey = orderedAlphaKey;
    }

    public String getUpperCaseName() {
        return upperCaseName;
    }

    public void setUpperCaseName(String upperCaseName) {
        this.upperCaseName = upperCaseName;
    }
}
