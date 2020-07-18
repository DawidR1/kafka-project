
package model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.*;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({
    "results",
})
public class Result {

    @JsonProperty("results")
    private List<User> results = null;

    @JsonProperty("results")
    public List<User> getResults() {
        return results;
    }

    @JsonProperty("results")
    public void setResults(List<User> results) {
        this.results = results;
    }
}
