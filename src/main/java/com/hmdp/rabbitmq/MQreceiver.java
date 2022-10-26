package com.hmdp.rabbitmq;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Slf4j
@Service
public class MQreceiver {
    @Resource
    private VoucherOrderServiceImpl voucherOrderService;
    @RabbitListener(queues = "seckillQueue")
    public void receive(String message){
        log.info("收到消息" + message);
        VoucherOrder voucherOrder = JSONUtil.toBean(message, VoucherOrder.class);
        voucherOrderService.handleVoucherOrder(voucherOrder);
    }
}
