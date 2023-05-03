{% docs gno_ez_xdai_transfers_table_doc %}

This table contains all native xDAI transfers, including equivalent USD amounts. The origin addresses correspond to the to and from addresses from the `fact_transactions` table. The `identifier` and `tx_hash` columns relate this table back to `fact_traces`, which contains more details on the transfers. *Column Name Update: `eth_<to/from>_address` has been migrated to `xdai_<to/from>_address`, values remain unchanged*

{% enddocs %}