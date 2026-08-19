package com.banking.transactionservice.entity;

/**
 * Transaction Lifecycle Flow
 *
 * PENDING -> PROCESSING -> COMPLETED (clean transaction)
 *                       -> PENDING_VERIFICATION (suspicious detected)
 *                                -> COMPLETED (verified)
 *                                -> FLAGGED (SAGA REFUND)
 *                       -> FAILED
 *                       -> FLAGGED
 */

public enum TransactionStatus {

    PENDING,
    PROCESSING,
    PENDING_VERIFICATION,
    COMPLETED,
    FAILED,
    FLAGGED
}
