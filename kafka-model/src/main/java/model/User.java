
package model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({
        "gender",
        "name",
        "location",
        "login",
        "id"
})
public class User {

    @JsonProperty("gender")
    private String gender;
    @JsonProperty("name")
    private Name name;
    @JsonProperty("location")
    private Location location;
    @JsonProperty("login")
    private Login login;
    @JsonProperty("id")
    private Id id;
    @JsonProperty("dob")
    private Dob dob;

    @JsonProperty("gender")
    public String getGender() {
        return gender;
    }

    @JsonProperty("gender")
    public void setGender(String gender) {
        this.gender = gender;
    }

    @JsonProperty("name")
    public Name getName() {
        return name;
    }

    @JsonProperty("name")
    public void setName(Name name) {
        this.name = name;
    }

    @JsonProperty("location")
    public Location getLocation() {
        return location;
    }

    @JsonProperty("location")
    public void setLocation(Location location) {
        this.location = location;
    }

    @JsonProperty("login")
    public Login getLogin() {
        return login;
    }

    @JsonProperty("login")
    public void setLogin(Login login) {
        this.login = login;
    }

    @JsonProperty("id")
    public Id getId() {
        return id;
    }

    @JsonProperty("id")
    public void setId(Id id) {
        this.id = id;
    }

    @JsonProperty("dob")
    public Dob getDob() {
        return dob;
    }

    @JsonProperty("dob")
    public void setDob(Dob dob) {
        this.dob = dob;
    }

    @Override
    public String toString() {
        return "User{" +
                "gender='" + gender + '\'' +
                ", name=" + name +
                ", location=" + location +
                ", login=" + login +
                ", id=" + id +
                '}';
    }
}
