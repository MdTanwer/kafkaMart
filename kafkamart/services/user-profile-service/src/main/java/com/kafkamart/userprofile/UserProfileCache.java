package com.kafkamart.userprofile;

import com.kafkamart.avro.UserProfile;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** In-memory projection of compacted {@code users} — last record per {@code user_id} wins. */
@ApplicationScoped
public class UserProfileCache {
    private final ConcurrentHashMap<String, UserProfile> byId = new ConcurrentHashMap<>();

    public void put(UserProfile profile) {
        byId.put(String.valueOf(profile.getUserId()), profile);
    }

    public Optional<UserProfile> get(String userId) {
        return Optional.ofNullable(byId.get(userId));
    }

    public Map<String, UserProfile> snapshot() {
        return Map.copyOf(byId);
    }
}
