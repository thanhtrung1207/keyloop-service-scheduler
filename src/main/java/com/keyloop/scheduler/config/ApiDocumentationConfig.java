package com.keyloop.scheduler.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class ApiDocumentationConfig {
    @Bean
    OpenAPI schedulerApi() {
        return new OpenAPI().info(new Info().title("Unified Service Scheduler").version("1.0")
                .description("Vehicle service bookings with automatic bay and qualified technician allocation. "
                        + "Supply UTC instants; the full interval must fit dealership local opening hours."));
    }
}
