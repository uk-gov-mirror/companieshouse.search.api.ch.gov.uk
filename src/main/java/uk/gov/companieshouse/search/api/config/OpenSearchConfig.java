package uk.gov.companieshouse.search.api.config;

import org.apache.http.HttpHost;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.gov.companieshouse.environment.EnvironmentReader;
import uk.gov.companieshouse.search.api.exception.EndpointException;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

@Configuration
public class OpenSearchConfig {

    private final EnvironmentReader environmentReader;

    public OpenSearchConfig(EnvironmentReader environmentReader) {
        this.environmentReader = environmentReader;
    }

    // These are currently pointing at the existing ES instance, will need to be updated in the configs for both
    private static final String ALPHABETICAL_SEARCH_URL = "ELASTIC_SEARCH_URL";

    @Bean
    public OpenSearchClient alphabeticalOpenSearchRestClient() {
        return createOpenSearchClient(ALPHABETICAL_SEARCH_URL);
    }

    public OpenSearchClient createOpenSearchClient(String url) {
        URL endpoint;

        try {
            String rawUrl = environmentReader.getMandatoryString(url);
            URI uri = new URI(rawUrl);
            endpoint = uri.toURL();
        } catch (URISyntaxException | MalformedURLException e) {
            throw new EndpointException(
                    url + " environment variable is malformed; expected format is <protocol>://<host>[:port]"
            );
        }

        org.opensearch.client.RestClient restClient = org.opensearch.client.RestClient.builder(
                new HttpHost(endpoint.getHost(), endpoint.getPort(), endpoint.getProtocol())
        ).build();

        OpenSearchTransport transport = new RestClientTransport(
                restClient,
                new JacksonJsonpMapper()
        );

        return new OpenSearchClient(transport);
    }
}
