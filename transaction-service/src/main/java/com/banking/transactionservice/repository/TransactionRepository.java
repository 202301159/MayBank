package com.banking.transactionservice.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface TransactionRepository extends JpaRepository<Transaction, String> {

    List<Transaction> findBySenderAccountNumberOrderByCreatedAtDesc(String accountNumber);
}
