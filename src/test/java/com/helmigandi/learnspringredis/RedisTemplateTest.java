package com.helmigandi.learnspringredis;

import com.helmigandi.learnspringredis.product.Product;
import com.helmigandi.learnspringredis.product.ProductRepository;
import com.helmigandi.learnspringredis.product.ProductService;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.*;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.support.collections.*;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@SpringBootTest
class RedisTemplateTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    private ProductService productService;

    @Test
    void notNullRedis() {
        Assertions.assertNotNull(redisTemplate);
    }

    @Test
    void successStringOperations() throws InterruptedException {
        ValueOperations<String, String> operations = redisTemplate.opsForValue();

        operations.set("name", "Helmi", Duration.ofSeconds(2));
        Assertions.assertEquals("Helmi", operations.get("name"));

        Thread.sleep(Duration.ofSeconds(3));
        Assertions.assertNull(operations.get("name"));
    }

    @Test
    void successListOperations() {
        ListOperations<String, String> operations = redisTemplate.opsForList();

        operations.rightPush("names", "Abdul");
        operations.rightPush("names", "Randy");
        operations.rightPush("names", "Anissa");

        Assertions.assertEquals("Abdul", operations.leftPop("names"));
        Assertions.assertEquals("Randy", operations.leftPop("names"));
        Assertions.assertEquals("Anissa", operations.leftPop("names"));
        Assertions.assertEquals(0, operations.size("names"));
    }

    @Test
    void successSetOperations() {
        SetOperations<String, String> operations = redisTemplate.opsForSet();

        operations.add("students", "Rozak");
        operations.add("students", "Rozak");
        operations.add("students", "Bambang");
        operations.add("students", "Ayu");
        operations.add("students", "Ayu");

        Set<String> students = operations.members("students");
        Assertions.assertEquals(3, students.size());

        assertThat(students, Matchers.hasItems("Rozak", "Bambang", "Ayu"));
    }

    @Test
    void successZSetOperations() {
        ZSetOperations<String, String> operations = redisTemplate.opsForZSet();

        operations.add("score", "Ronal", 100);
        operations.add("score", "Zizi", 79);
        operations.add("score", "Budi", 81);

        // Get max score
        Assertions.assertEquals("Ronal", operations.popMax("score").getValue());
        Assertions.assertEquals("Budi", operations.popMax("score").getValue());
        Assertions.assertEquals("Zizi", operations.popMax("score").getValue());
    }

    @Test
    void successHashOperations() {
        HashOperations<String, Object, Object> operations = redisTemplate.opsForHash();

//        operations.put("user:1", "id", "1");
//        operations.put("user:1", "username", "rozali21");
//        operations.put("user:1", "email", "rozali@example.com");

        HashMap<String, String> map = new HashMap<>();
        map.put("id", "1");
        map.put("name", "rozali31");
        map.put("email", "rozali@example.com");
        operations.putAll("user:1", map);

        Assertions.assertEquals("1", operations.get("user:1", "id"));

        redisTemplate.delete("user:1");
    }

    @Test
    void successGeoOperations() {
        GeoOperations<String, String> operations = redisTemplate.opsForGeo();

        operations.add("places", new Point(106.9648821, -6.2145343), "smp13");
        operations.add("places", new Point(106.9498617, -6.2190263), "st-cakung");
        operations.add("places", new Point(106.9435911, -6.2130897), "tm-pulo-gebang");
        operations.add("places", new Point(106.9810826, -6.2278248), "grand-mall");

        Distance distance = operations.distance("places", "smp13", "st-cakung", Metrics.KILOMETERS);
        Assertions.assertEquals(1.7343, distance.getValue());

        GeoResults<RedisGeoCommands.GeoLocation<String>> places =
                operations.search("places", new Circle(
                        new Point(106.966387, -6.215988),
                        new Distance(2, Metrics.KILOMETERS)
                ));

        Assertions.assertEquals(2, places.getContent().size());
        Assertions.assertEquals("smp13", places.getContent().get(0).getContent().getName());
    }

    /**
     * HyperLogLog untuk mencari data unik, tidak butuh media penyimpanan terlalu besar.
     * Akan tetapi kita tidak bisa mengambil datanya.
     */
    @Test
    void successHyperLogLogOperations() {
        HyperLogLogOperations<String, String> operations = redisTemplate.opsForHyperLogLog();

        operations.add("traffics", "abdul", "anissa", "rozak");
        operations.add("traffics", "bambang", "ayu", "shinta");
        operations.add("traffics", "anissa", "bambang", "rozak");

        Assertions.assertEquals(6, operations.size("traffics"));
    }

    @Test
    void successTransactionOperations() {
        redisTemplate.execute(new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.multi();
                operations.opsForValue().set("test1", "Helmi", Duration.ofSeconds(2));
                operations.opsForValue().set("test2", "Nurul", Duration.ofSeconds(2));
                operations.exec(); // commit
                return null;
            }
        });

        Assertions.assertEquals("Helmi", redisTemplate.opsForValue().get("test1"));
        Assertions.assertEquals("Nurul", redisTemplate.opsForValue().get("test2"));
    }

    /**
     * Mengirim beberapa perintah secara langsung tanpa harus menunggu balasan
     * satu per satu dari Redis.
     */
    @Test
    void successPipelineOperations() {
        List<Object> names = redisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.opsForValue().set("test1", "Rozak", Duration.ofSeconds(2));
                operations.opsForValue().set("test2", "Abdul", Duration.ofSeconds(2));
                operations.opsForValue().set("test3", "Ayu", Duration.ofSeconds(2));
                operations.opsForValue().set("test4", "Anton", Duration.ofSeconds(2));
                return null;
            }
        });

        assertThat(names, hasSize(4));
        assertThat(names, hasItems(true));
        assertThat(names, not(hasItems(false)));
    }

    @Test
    void successList() {
        RedisList<String> names = RedisList.create("names", redisTemplate);
        names.add("abdul");
        names.add("djadjang");
        names.add("anissa");

        List<String> list = redisTemplate.opsForList().range("names", 0, -1);

        assertThat(list, hasItems("abdul", "djadjang", "anissa"));
    }

    @Test
    void successSet() {
        RedisSet<String> traffics = RedisSet.create("traffics", redisTemplate);
        traffics.addAll(Set.of("Rahman", "Dani", "Anton", "Lisa"));

        Set<String> trafficsSaved = redisTemplate.opsForSet().members("traffics");
        assertThat(trafficsSaved, hasItems("Rahman", "Dani", "Anton", "Lisa"));
    }

    @Test
    void successZSet() {
        RedisZSet<String> winners = RedisZSet.create("winners", redisTemplate);
        winners.add("Budi", 100);
        winners.add("Joko", 78);
        winners.add("Nisa", 80);
        winners.add("Luka", 56);

        Set<String> savedWinners = redisTemplate.opsForZSet().range("winners", 0, -1);

        assertThat(savedWinners, hasItems("Budi", "Joko", "Nisa", "Luka"));

        // Get Highest score
        Assertions.assertEquals("Budi", winners.popLast());
    }

    @Test
    void successMap() {
        DefaultRedisMap<String, String> userMap = new DefaultRedisMap<>("user:1", redisTemplate);
        userMap.put("name", "suryana malik");
        userMap.put("address", "Jakarta");
        userMap.put("age", "21");

        Map<Object, Object> entries = redisTemplate.opsForHash().entries("user:1");
        assertThat(entries, hasEntry("name", "suryana malik"));
        assertThat(entries, hasEntry("address", "Jakarta"));
    }

    @Test
    void successSave() {
        Product mieAyam = Product.builder()
                .id("1")
                .name("Mie Ayam")
                .price(new BigDecimal("20000"))
                .build();

        productRepository.save(mieAyam);

        Product savedProduct = productRepository.findById("1").get();
        Assertions.assertEquals(mieAyam, savedProduct);

        // Manual
        Map<Object, Object> map = redisTemplate.opsForHash().entries("products:1");
        Assertions.assertEquals(mieAyam.getId(), map.get("id"));
        Assertions.assertEquals(mieAyam.getName(), map.get("name"));
        Assertions.assertEquals(mieAyam.getPrice().toString(), map.get("price"));
    }

    @Test
    void successTTL() throws InterruptedException {
        Product mieAyam = Product.builder()
                .id("2")
                .name("Mie Ayam")
                .price(new BigDecimal("20000"))
                .ttl(3L)
                .build();

        productRepository.save(mieAyam);

        Assertions.assertTrue(productRepository.findById("2").isPresent());

        Thread.sleep(Duration.ofSeconds(5));

        Assertions.assertFalse(productRepository.findById("2").isPresent());
    }

    @Test
    void successCacheManager() {
        Cache mathScores = cacheManager.getCache("math_scores");
        mathScores.put("santoso", 49);
        mathScores.put("anton", 81);

        Assertions.assertEquals(49, mathScores.get("santoso", Integer.class));
        Assertions.assertEquals(81, mathScores.get("anton", Integer.class));

        mathScores.evict("santoso");
        Assertions.assertNull(mathScores.get("santoso"));
    }

    @Test
    void successCacheable() {
        Product product1 = productService.getProduct("EX-001");
        Assertions.assertEquals("EX-001", product1.getId());

        Product product2 = productService.getProduct("EX-001");
        Assertions.assertEquals(product2, product1);
    }

    @Test
    void successCachePut() {
        Product product2 = Product.builder()
                .id("EX-002")
                .name("Example Product 2")
                .price(new BigDecimal("2000"))
                .build();

        productService.save(product2);

        Product product = productService.getProduct(product2.getId());

        Assertions.assertEquals(product2, product);
    }

    @Test
    void successCacheEvict() {
        Product product3a = productService.getProduct("EX-003");

        productService.remove("EX-003");

        Product product3b = productService.getProduct("EX-003");

        Assertions.assertEquals(product3a, product3b);
    }
}
