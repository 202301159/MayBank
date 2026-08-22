package com.banking.apigateway.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

@Configuration
public class RateLimiterConfig {

    @Bean
    public keyResolver keyResolver() {

        return exchange -> Mono.just(
                exchange.getRequest()
                        .getRemoteAddress()
                        .getAddress()
                        .getHostAddress()
        );
    }
}
