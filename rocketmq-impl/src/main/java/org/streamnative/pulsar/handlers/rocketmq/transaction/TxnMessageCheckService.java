package org.streamnative.pulsar.handlers.rocketmq.transaction;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.ServiceThread;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.TransactionalMessageCheckService;

@Slf4j
public class TxnMessageCheckService extends ServiceThread {

    private RocketMQBrokerController brokerController;

    public TxnMessageCheckService(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public String getServiceName() {
        return TransactionalMessageCheckService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("Start transaction check service thread!");
        long checkInterval = brokerController.getServerConfig().getTransactionCheckInterval();
        while (!this.isStopped()) {
            this.waitForRunning(checkInterval);
        }
        log.info("End transaction check service thread!");
    }

    @Override
    protected void onWaitEnd() {
        long timeout = brokerController.getServerConfig().getTransactionTimeOut();
        int checkMax = brokerController.getServerConfig().getTransactionCheckMax();
        long begin = System.currentTimeMillis();
        log.info("Begin to check prepare message, begin time:{}", begin);
        this.brokerController.getTransactionalMessageService()
                .check(timeout, checkMax, this.brokerController.getTransactionalMessageCheckListener());
        log.info("End to check prepare message, consumed time:{}", System.currentTimeMillis() - begin);
    }
}
