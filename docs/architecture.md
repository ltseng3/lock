# Organization

- lock_core: protocol identifiers and strict configuration.
- rdma: reliable-connected QPs, registered memory and completion dispatch.
- locks: independent CAS, Mu and handoff state machines.
- bench: common data operation and lock selection.
- experiments: SSH deployment and result archives.

One process per node owns its transport event loop. Workers are logical issuers, not separate QPs. All peers connect before measurement.

Fixed membership and leader in the first implementation. Fail closed on transport or protocol errors. Timeouts never authorize a new holder. Hardware claims require hardware validation.
