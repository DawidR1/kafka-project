package pl.dawid.producer;


import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class UserGeneratorClient {
    public static final String RAND_USER_URL = "https://randomuser.me/api/";
    private final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .build();
    private final HttpRequest request = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(RAND_USER_URL))
            .build();

    public String getRandomUser() {
        try {
            return httpClient.send(request, HttpResponse.BodyHandlers.ofString()).body();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Error when sending request to " + RAND_USER_URL, e);
        }
    }
}
