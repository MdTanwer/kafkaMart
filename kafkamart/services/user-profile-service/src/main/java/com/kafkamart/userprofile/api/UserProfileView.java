package com.kafkamart.userprofile.api;

import com.kafkamart.avro.UserProfile;

public record UserProfileView(String userId, String name, String email) {
    public static UserProfileView from(UserProfile profile) {
        return new UserProfileView(
                String.valueOf(profile.getUserId()),
                String.valueOf(profile.getName()),
                String.valueOf(profile.getEmail()));
    }
}
