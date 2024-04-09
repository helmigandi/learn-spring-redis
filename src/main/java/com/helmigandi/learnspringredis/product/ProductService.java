package com.helmigandi.learnspringredis.product;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Slf4j
@Service
public class ProductService {

    @Cacheable(value = "products", key = "#id")
    public Product getProduct(String id) {
        log.info("Get Product: {}", id);
        return Product.builder()
                .id(id)
                .name("Example Product 1")
                .price(new BigDecimal("10000"))
                .build();
    }

    @CachePut(value = "products", key = "#product.id")
    public Product save(Product product) {
        log.info("Save Product: {}", product);
        return product;
    }

    @CacheEvict(value = "products", key = "#id")
    public void remove(String id) {
        log.info("Remove Product: {}", id);
    }
}
