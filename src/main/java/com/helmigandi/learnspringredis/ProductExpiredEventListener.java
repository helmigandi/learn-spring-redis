package com.helmigandi.learnspringredis;

import com.helmigandi.learnspringredis.product.Product;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.RedisKeyExpiredEvent;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ProductExpiredEventListener {

    @EventListener
    public void handleRedisKeyExpiredEvent(RedisKeyExpiredEvent<Product> event) {
        Product expiredSession = (Product) event.getValue();
        log.info("Product with key={} has expired", expiredSession.getId());
    }
}
