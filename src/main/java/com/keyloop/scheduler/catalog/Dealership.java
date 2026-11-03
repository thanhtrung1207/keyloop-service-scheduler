package com.keyloop.scheduler.catalog;

import java.time.LocalTime;
import java.time.ZoneId;
import java.util.UUID;

public record Dealership(UUID id, String name, ZoneId timezone, LocalTime openTime, LocalTime closeTime) { }
