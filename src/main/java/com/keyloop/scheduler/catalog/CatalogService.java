package com.keyloop.scheduler.catalog;

import com.keyloop.scheduler.booking.BookingRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;

@Service
public class CatalogService {
    private static final Logger log = LoggerFactory.getLogger(CatalogService.class);
    private static final Duration TTL = Duration.ofHours(1);
    private final BookingRepository repository;
    private final StringRedisTemplate redis;
    private final JsonMapper mapper;
    private final boolean enabled;

    public CatalogService(BookingRepository repository, StringRedisTemplate redis, JsonMapper mapper,
                          @Value("${scheduler.cache.enabled:true}") boolean enabled) {
        this.repository = repository;
        this.redis = redis;
        this.mapper = mapper;
        this.enabled = enabled;
    }

    public Optional<Dealership> findDealership(UUID id) {
        return cached("scheduler:catalog:dealership:" + id, Dealership.class,
                value -> id.equals(value.id()) && value.name() != null && value.timezone() != null
                        && value.openTime() != null && value.closeTime() != null
                        && value.openTime().isBefore(value.closeTime()),
                () -> repository.findDealership(id));
    }

    public Optional<ServiceType> findServiceType(UUID id) {
        return cached("scheduler:catalog:service-type:" + id, ServiceType.class,
                value -> id.equals(value.id()) && value.code() != null && value.name() != null
                        && value.requiredSkill() != null && value.durationMinutes() > 0,
                () -> repository.findServiceType(id));
    }

    private <T> Optional<T> cached(String key, Class<T> type, Predicate<T> valid, Supplier<Optional<T>> loader) {
        if (!enabled) {
            return loader.get();
        }
        try {
            String serialized = redis.opsForValue().get(key);
            if (serialized != null) {
                T value = mapper.readValue(serialized, type);
                if (value != null && valid.test(value)) {
                    return Optional.of(value);
                }
                log.warn("catalog_cache_invalid key={}", key);
            }
        } catch (RuntimeException cacheFailure) {
            log.warn("catalog_cache_read_bypassed key={} failureType={}", key,
                    cacheFailure.getClass().getSimpleName());
        }
        // Database errors intentionally propagate: PostgreSQL is the authority.
        Optional<T> loaded = loader.get();
        loaded.ifPresent(value -> {
            try {
                redis.opsForValue().set(key, mapper.writeValueAsString(value), TTL);
            } catch (RuntimeException cacheFailure) {
                log.warn("catalog_cache_write_bypassed key={} failureType={}", key,
                        cacheFailure.getClass().getSimpleName());
            }
        });
        return loaded;
    }
}
