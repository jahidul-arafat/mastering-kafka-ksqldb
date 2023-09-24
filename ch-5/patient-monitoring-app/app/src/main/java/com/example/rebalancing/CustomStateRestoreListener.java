package com.example.rebalancing;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomStateRestoreListener implements StateRestoreListener {
    private static final Logger log = LoggerFactory.getLogger(CustomStateRestoreListener.class);

    // Rebalancing invoked state-store reinitialization
    /*
    - check 'startingOffset'- if set to 0, full reinitialization is required
                            - if set > 0, then only a partial restore is necessary
     */
    @Override
    public void onRestoreStart
            (TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
        log.info("The following state store is being restored: {}", storeName);

    }

    // invoked when a single batch of records is restored.
    // MAX_BATCH_SIZE = MAX_POLL_RECORDS
    @Override
    public void onBatchRestored
            (TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
        // this is very noisy. don't log anything //better to commenting the below
        log.info(
                "A batch of {} records has been restored in the following state store: {}",
                numRestored,
                storeName);
    }

    // method to invoke when state-store restore ends after Rebalancing
    @Override
    public void onRestoreEnd
            (TopicPartition topicPartition, String storeName, long totalRestored) {
        log.info("Restore complete for the following state store: {}", storeName);
    }
}
