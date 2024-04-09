package com.helmigandi.learnspringredis.product;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.keyvalue.annotation.KeySpace;
import org.springframework.data.redis.core.TimeToLive;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@KeySpace("products")
public class Product implements Serializable {

    @Id
    private String id;

    private String name;

    private BigDecimal price;

    @TimeToLive(unit = TimeUnit.SECONDS)
    private Long ttl = -1L;
}
