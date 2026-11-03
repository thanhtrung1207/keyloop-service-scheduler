package com.keyloop.scheduler.catalog;

import java.util.UUID;

public record ServiceType(UUID id, String code, String name, int durationMinutes, String requiredSkill) { }
