package uk.gov.companieshouse.search.api.service.upsert;

import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.api.model.company.CompanyProfileApi;
import uk.gov.companieshouse.search.api.model.esdatamodel.Company;
import uk.gov.companieshouse.search.api.model.response.ResponseObject;

public interface UpsertCompanyService {


    /**
     * Upserts a new document to the alphabetical search index.
     * If a document does not exist it is added.
     * If the document does exist it is updated.
     *
     * @param company - Company sent over in REST call to be added/updated
     * @return {@link ResponseObject}
     */
    ResponseObject<String> upsert(CompanyProfileApi company);

    /**
     * Upserts a new document to advanced search index.
     * If a document does not exist it is added.
     * If the document does exist it is updated.
     *
     * @param company - Company sent over in REST call to be added/updated
     * @return {@link ResponseObject}
     */
    ResponseObject<String> upsertAdvanced(CompanyProfileApi company);

    ResponseObject<Company> upsertCompany(String companyNumber, Data profileData);
}
