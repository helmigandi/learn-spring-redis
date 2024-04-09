package com.helmigandi.learnspringredis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

import java.time.Duration;

@EnableCaching
@SpringBootApplication
@EnableRedisRepositories
public class LearnSpringRedisApplication {

    public static void main(String[] args) {
        SpringApplication.run(LearnSpringRedisApplication.class, args);
    }

    @Bean(destroyMethod = "stop", initMethod = "start")
    public StreamMessageListenerContainer<String, ObjectRecord<String, Order>> orderContainer(RedisConnectionFactory connectionFactory) {
        var options = StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                .builder()
                .pollTimeout(Duration.ofSeconds(5))
                .targetType(Order.class)
                .build();

        return StreamMessageListenerContainer.create(connectionFactory, options);
    }
}
