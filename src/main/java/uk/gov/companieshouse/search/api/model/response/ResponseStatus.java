package uk.gov.companieshouse.search.api.model.response;

public enum ResponseStatus {
    SEARCH_FOUND,
    SEARCH_NOT_FOUND,
    SEARCH_ERROR,
    REQUEST_PARAMETER_ERROR,
    UPSERT_ERROR,
    UPDATE_REQUEST_ERROR,
    DOCUMENT_UPSERTED
}
