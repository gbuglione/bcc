# Bcc

### Assumptions

* dispute, resolve and chargeback all reference transactions by the same client, that is, a client can only dispute their transactions.
* only deposits can be disputed. This is a bit unclear for me, but the actions described in the doc for dispute seemed only appliable for deposits (same for resolve and chargeback).
In addition, IMO it's not clear why reversing a withdrawal should first result in adding freezed funds to an account, since such funds would not be usable. This being said, there's nothing in the solution that prevents supporting withdrawals disputes in the future.
* deposit and withdrawal amounts are non negative, otherwise the distinction between the two would not be very meaningful
* a transaction can only be disputed once
* No forther operations are allowed on a frozen account, including disputes

### Design

Transactions are processed as a stream, so that it's possible to start processing even without buffering them all in memory. At the moment, this is done synchronously,
but it's relatively easy to switch to async so that, for example, we could accept transactions concurrently from multiple tcp streams.

Old transactions are kept so that we can process disputes on those. Since the system might have to handle a significant amount of transactions we adopt a tiered system, where
older transactions are offloaded to disk.

Transactions processing is parallelized where possible, building on the fact that transactions beloging to different clients are independent in this system.

See code comments and doc for more details.

### Testing

Where deemed useful (e.g. locked account), certain invariants are enforced at the typesytem level, so that a program that changes the balance of a frozen account would be an invalid Rust program and rejected by the compiler.
More traditional tests are used for other parts of the system, with property testing being used where possible.


### TODO
* Several implementation choices are not the best in term of performance and are just there to sketch the design. E.g. most datastructures are not ideal but were readily available off the shelf.
* Possibly switch to async
* Increase test coverage
* Over/underflows handling