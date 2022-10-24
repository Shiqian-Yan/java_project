package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <T, ID> T queryWithPassThrough(String keyPrefix, ID id, Class<T> type,
                                          Function<ID, T> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        //1.从redis查询商品缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            //3.存在直接返回
            return JSONUtil.toBean(json, type);
        }
        //判断命中的是否是空值
        if (json != null) {
            //返回错误信息
            return null;
        }
        //查数据库
        T r = dbFallback.apply(id);
        //4.不存在
        if (r == null) {
            //5.不存在返回错误
            //将空值写入redis(缓存穿透)
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //6.存在，将数据写到redis
        this.set(key, r,time,unit);
        //7.返回
        return r;
    }
    //利用互斥锁解决缓存击穿问题
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key,"1",10,TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    //这个方法需要将key提前加入缓存，不需要检查缓存穿透问题，如果redis查不到key说明，不在活动范围内
    //直接返回空就可以，不用查数据库
    public <T, ID> T queryWithLogicalExpire(String keyPrefix,ID id,Class<T> type,
                                            Function<ID,T> dbFallback,Long time, TimeUnit unit){
        String key = keyPrefix + id;
        //1.从redis查询商品缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if(StrUtil.isBlank(json)){
            //3.不存在直接返回
            return null;
        }
        //命中需要判断过期时间，反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        T r = JSONUtil.toBean((JSONObject) redisData.getData(),type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            //未过期直接返回
            return r;
        }
        //已经过期缓存重建
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        //获取mutex
        boolean tryLock = tryLock(lockKey);
        //判断是否获取成功
        if(tryLock) {
            //获取成功 再次检查缓存
            //1.从redis查询商品缓存
            json = stringRedisTemplate.opsForValue().get(key);
            //2.判断是否存在
            if(StrUtil.isBlank(json)){
                //3.不存在直接返回
                return null;
            }
            //命中需要判断过期时间，反序列化为对象
            redisData = JSONUtil.toBean(json, RedisData.class);
            r = JSONUtil.toBean((JSONObject) redisData.getData(),type);
            expireTime = redisData.getExpireTime();
            //判断是否过期
            if(expireTime.isAfter(LocalDateTime.now())) {
                //未过期直接返回
                return r;
            }
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    T t = dbFallback.apply(id);
                    this.setWithLogicalExpire(key,t,time,unit);
                } finally {
                    unlock(lockKey);
                }
            });
        }
        //返回过期商铺信息
        return r;
    }
}
