package com.example.kafka.srbackup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.DefaultUriBuilderFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Thin wrapper over the Schema Registry REST API.
 *
 * Responses are returned as raw JsonNode on purpose: we never map a schema into a
 * Java class, so fields we don't know about (metadata, ruleSet, future fields)
 * survive the backup untouched.
 */
@Component
public class SrClient {

    private final RestClient rest;
    private final ObjectMapper mapper;
    private final String baseUrl;

    public SrClient(@Value("${sr.url:}") String baseUrl,
                    @Value("${sr.username:}") String username,
                    @Value("${sr.password:}") String password,
                    ObjectMapper mapper) {
        this.baseUrl = baseUrl;
        this.mapper = mapper;

        // VALUES_ONLY = encode every {variable} strictly, so subject names
        // containing '/', ':' or spaces are sent correctly.
        DefaultUriBuilderFactory uris = new DefaultUriBuilderFactory(baseUrl);
        uris.setEncodingMode(DefaultUriBuilderFactory.EncodingMode.VALUES_ONLY);

        // without timeouts a hung registry hangs the CLI (and a scheduled backup) forever
        HttpComponentsClientHttpRequestFactory http = new HttpComponentsClientHttpRequestFactory();
        http.setConnectTimeout(Duration.ofSeconds(10));
        http.setReadTimeout(Duration.ofSeconds(60));

        RestClient.Builder b = RestClient.builder()
                .requestFactory(http)
                .uriBuilderFactory(uris)
                .defaultHeader(HttpHeaders.ACCEPT, "application/vnd.schemaregistry.v1+json, application/json");
        if (!username.isBlank()) {
            b.defaultHeaders(h -> h.setBasicAuth(username, password));
        }
        this.rest = b.build();
    }

    public String baseUrl() {
        return baseUrl;
    }

    /**
     * The registry's identity: its Kafka cluster id plus its schema-registry group id.
     * Unlike the URL, this is the same whichever hostname or load balancer you reach it through.
     * Null if the endpoint does not exist.
     */
    public JsonNode registryId() {
        JsonNode md = get("/v1/metadata/id");
        return md == null ? null : md.path("scope").path("clusters");
    }

    /** GET. Returns null when Schema Registry answers 404 (e.g. "no subject-level config"). */
    public JsonNode get(String uri, Object... vars) {
        return call(HttpMethod.GET, uri, null, true, vars);
    }

    public JsonNode post(String uri, Object body, Object... vars) {
        return call(HttpMethod.POST, uri, body, false, vars);
    }

    public JsonNode put(String uri, Object body, Object... vars) {
        return call(HttpMethod.PUT, uri, body, false, vars);
    }

    public JsonNode delete(String uri, Object... vars) {
        return call(HttpMethod.DELETE, uri, null, false, vars);
    }

    private JsonNode call(HttpMethod method, String uri, Object body, boolean nullOn404, Object... vars) {
        RestClient.RequestBodySpec spec = rest.method(method).uri(uri, vars);
        if (body != null) {
            spec = spec.contentType(MediaType.APPLICATION_JSON).body(body);
        }
        return spec.exchange((request, response) -> {
            int status = response.getStatusCode().value();
            byte[] bytes = response.getBody().readAllBytes();
            if (status == 404 && nullOn404) {
                return null;
            }
            if (status >= 400) {
                throw new IllegalStateException(method + " " + request.getURI() + " -> HTTP " + status
                        + " " + new String(bytes, StandardCharsets.UTF_8));
            }
            return bytes.length == 0 ? mapper.nullNode() : mapper.readTree(bytes);
        });
    }
}
