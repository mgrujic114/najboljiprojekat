package io.conductor;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Course {
    @JsonProperty("id")
    private String id;

    @JsonProperty("title")
    private String title;

    @JsonProperty("url")
    private String url;

    @JsonProperty("is_paid")
    private String paid;  // Treat as String, can convert later to boolean if necessary

    @JsonProperty("instructor_names")
    private String instructorNames;

    @JsonProperty("category")
    private String category;

    @JsonProperty("headline")
    private String headline;

    @JsonProperty("num_subscribers")
    private int numSubscribers;

    @JsonProperty("rating")
    private double rating;

    @JsonProperty("num_reviews")
    private int numReviews;

    @JsonProperty("instructional_level")
    private String instructionalLevel;

    @JsonProperty("objectives")
    private String objectives;

    @JsonProperty("curriculum")
    private String curriculum;

    public Course() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getPaid() {
        return paid;
    }

    public void setPaid(String paid) {
        this.paid = paid;
    }

    public boolean isPaid() {
        return "True".equalsIgnoreCase(paid);  // Convert "True"/"False" to boolean if necessary
    }

    public String getInstructorNames() {
        return instructorNames;
    }

    public void setInstructorNames(String instructorNames) {
        this.instructorNames = instructorNames;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getHeadline() {
        return headline;
    }

    public void setHeadline(String headline) {
        this.headline = headline;
    }

    public int getNumSubscribers() {
        return numSubscribers;
    }

    public void setNumSubscribers(int numSubscribers) {
        this.numSubscribers = numSubscribers;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    public int getNumReviews() {
        return numReviews;
    }

    public void setNumReviews(int numReviews) {
        this.numReviews = numReviews;
    }

    public String getInstructionalLevel() {
        return instructionalLevel;
    }

    public void setInstructionalLevel(String instructionalLevel) {
        this.instructionalLevel = instructionalLevel;
    }

    public String getObjectives() {
        return objectives;
    }

    public void setObjectives(String objectives) {
        this.objectives = objectives;
    }

    public String getCurriculum() {
        return curriculum;
    }

    public void setCurriculum(String curriculum) {
        this.curriculum = curriculum;
    }

    @Override
    public String toString() {
        return "Course{" +
                "id='" + id + '\'' +
                ", title='" + title + '\'' +
                '}';
    }
}
