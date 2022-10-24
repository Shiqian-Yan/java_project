package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    //新建的工具类
    @Resource
    private CacheClient client;
    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = queryWithPassThrough(id);
        //互斥锁解决缓存击穿(包含缓存穿透)
        //Shop shop = queryWithMutex(id);
        //Shop shop = queryWithLogicalExpire(id);
        //id2->getById(id2) == this::getById
        Shop shop = client.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY,id,
                Shop.class, this::getById,RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES);
        if(shop == null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }
    //利用互斥锁解决缓存击穿问题
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key,"1",10,TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
    public void saveShop2Redis(Long id,Long expire){
        //1.查询店铺数据
        Shop shop = getById(id);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expire));
        //3.写入redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(redisData));
    }
    public Shop queryWithPassThrough(Long id){
        String key = "cache:shop:" + id;
        //1.从redis查询商品缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            //3.存在直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断命中的是否是空值
        if(shopJson != null){
            //返回错误信息
            return null;
        }
        Shop shop = getById(id);
        //4.不存在
        if(shop == null){
            //5.不存在返回错误
            //将空值写入redis(缓存穿透)
            stringRedisTemplate.opsForValue().set(key,"", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //6.存在，将数据写到redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //7.返回
        return shop;
    }
    public Shop queryWithMutex(Long id){
        String key = "cache:shop:" + id;
        //1.从redis查询商品缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            //3.存在直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断命中的是否是空值
        if(shopJson != null){
            //返回错误信息
            return null;
        }
        //实现缓存重建
        //获取互斥锁
        String lockKey = "lock:shop:" + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //判断是否获取成功
            if(!isLock){
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //获取锁成功再次检查缓存，没有则查数据库
            //1.从redis查询商品缓存
            shopJson = stringRedisTemplate.opsForValue().get(key);
            //2.判断是否存在
            if(StrUtil.isNotBlank(shopJson)){
                //3.存在直接返回
                shop = JSONUtil.toBean(shopJson, Shop.class);
                return shop;
            }
            //判断命中的是否是空值
            if(shopJson != null){
                //返回错误信息
                return null;
            }
            shop = getById(id);
            //4.不存在
            if(shop == null){
                //5.不存在返回错误
                //将空值写入redis(缓存穿透)
                stringRedisTemplate.opsForValue().set(key,"", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //6.存在，将数据写到redis
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            //释放锁
            unlock(lockKey);
        }
        //7.返回
        return shop;
    }
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    //这个方法需要将key提前加入缓存，不需要检查缓存穿透问题，如果redis查不到key说明，不在活动范围内
    //直接返回空就可以，不用查数据库
    public Shop queryWithLogicalExpire(Long id){
        String key = "cache:shop:" + id;
        //1.从redis查询商品缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if(StrUtil.isBlank(shopJson)){
            //3.不存在直接返回
            return null;
        }
        //命中需要判断过期时间，反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(),Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            //未过期直接返回
            return shop;
        }
        //已经过期缓存重建
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        //获取mutex
        boolean tryLock = tryLock(lockKey);
        //判断是否获取成功
        if(tryLock) {
            //获取成功 再次检查缓存
            //1.从redis查询商品缓存
            shopJson = stringRedisTemplate.opsForValue().get(key);
            //2.判断是否存在
            if(StrUtil.isBlank(shopJson)){
                //3.不存在直接返回
                return null;
            }
            //命中需要判断过期时间，反序列化为对象
            redisData = JSONUtil.toBean(shopJson, RedisData.class);
            shop = JSONUtil.toBean((JSONObject) redisData.getData(),Shop.class);
            expireTime = redisData.getExpireTime();
            //判断是否过期
            if(expireTime.isAfter(LocalDateTime.now())) {
                //未过期直接返回
                return shop;
            }
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    saveShop2Redis(id,30L);
                } finally {
                    unlock(lockKey);
                }
            });
        }
        //返回过期商铺信息
        return shop;
    }
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("fail");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete("cache:shop:" + id);
        return Result.ok();
    }
}
