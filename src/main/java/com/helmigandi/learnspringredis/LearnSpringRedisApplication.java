package com.helmigandi.learnspringredis;

import com.helmigandi.learnspringredis.product.Product;
import com.helmigandi.learnspringredis.product.ProductRepository;
import com.helmigandi.learnspringredis.product.ProductService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.RedisKeyValueAdapter;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

import java.math.BigDecimal;
import java.time.Duration;

@Slf4j
@EnableCaching
@SpringBootApplication
@EnableRedisRepositories(enableKeyspaceEvents = RedisKeyValueAdapter.EnableKeyspaceEvents.ON_STARTUP)
public class LearnSpringRedisApplication implements CommandLineRunner {

    @Autowired
    private ProductRepository productRepository;

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

    @Override
    public void run(String... args) throws Exception {
        Product mieAyam = Product.builder()
                .id("3")
                .name("Mie Ayam Bakso")
                .price(new BigDecimal("25000"))
                .ttl(15L)
                .build();

        productRepository.save(mieAyam);

        log.info("Product saved!");
    }
}
