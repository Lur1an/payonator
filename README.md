# PAYONATOR

## Description
A concurrent payment processing engine.

## Assumptions
- Input stream (CSV file) has chronologically ordered events
- A `Dispute` is only valid when targeting a `Deposit` type `TransactionRecord`. Disputes 
decrease the `Client`'s `total_funds` by increasing the `held_funds` by the `TransactionRecord`'s amount, this does not fit in with `Withdrawal` type transactions.

## Correctness
The engine leverages *SQLite* storage to guarantee integrity of the data in regards to transaction, dispute and client ids being correct and referencing each other. This simplifies verifying disputes, resolutions and chargebacks.

For ease of development the funds of the `Client` are denormalized and integrity of the data is ensured by opening a transaction when inserting new data that will affect these fields.

For faster storage solutions the engine can be refactored around a trait `Storage`, creating a
`PaymentEngine<S>` that can run on different storage backends, this would require some refactoring as the `process_transaction` function currently directly calls SQL methods.

## Safety
The engine is safe despite concurrency by ensuring that for every client it is always the same working executing its transactions in chronological order.

## Negative Balance
Negative total balance can't happen, however negative available balance can happen through a dispute.

## Error handling
Error handling currently is not fine-grained as there is no behaviour to be executed when handling
different types of errors in the context of this toy payment engine. An error in the database layer is not distinguished from a integrity error encountered when inserting invalid data, or a precondition not being met to execute the given transaction as in all cases the engine currently just rejects the transaction and continues processing events. Using `anyhow` cuts down on complexity whilst implementing the bare-bone functionality.

For fine-grained behaviour on different types of error enums can be created and on specific error types the engine can be modified to apply retry logic.

## Multiple Input Streams
The engine can handle multiple input streams (for example multiple TCP streams, or parallel `.csv` files) fused into a single
`impl Stream<Item = Result<TransactionRecord, impl Into<BoxError>>>`, however only if the following invariant is maintained:
> Given a `TransactionRecord` `A` and a `TransactionRecord` `B` where `A.client_id == B.client_id`
if `A` is chronologically before `B` then `A` must be yielded before `B` in the input stream.

This invariant can be implemented the code if the `TransactionRecord` had a way of tracking
the chronological order of the transactions. If we would assumne that the `u32` id of the transaction is linked to its chronological order (or if the record had a timestamp field), we could buffer up to `N` transactions for each `Client` and process them in batches at an interval `I` by sorting the buffered transaction in chronological order first.
Currently the engine trusts that the invariant is maintained, otherwise it can't guarantee that all transactions can be handled correctly.
