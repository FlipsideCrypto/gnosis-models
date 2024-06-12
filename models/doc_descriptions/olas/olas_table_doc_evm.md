{% docs olas_dim_registry_metadata_table_doc %}

This table contains dimensional metadata for the registry contracts, including details about the various Agent, Component and Service entities registered in the OLAS ecosystem. The metadata is sourced via contract reads on the tokenURI function, and typically direct to IPFS.

{% enddocs %}

{% docs olas_ez_service_registrations_table_doc %}

This convenience table contains fact-based records of service registrations within the OLAS protocol, capturing essential information about each registered service event and includes a join on the dim_registry_metadata table for additional details pertaining to each service_id, such as name and description.

{% enddocs %}

{% docs olas_fact_service_event_logs_table_doc %}

This fact-based table contains all emitted event logs related to registered services and service multisigs within the OLAS protocol. This is accomplished by joining all events where the transaction's `origin_to_address` = `multisig_address` to showcase the onchain interactions with each service.

{% enddocs %}

{% docs olas_ez_olas_staking_table_doc %}

This fact-based convenience table contains OLAS token staking events, including deposits (Stake), withdrawals (Unstake), and amount USD where available.

{% enddocs %}

{% docs olas_ez_service_staking_table_doc %}

This fact-based convenience table contains Service staking events, including epoch, owner and multisig addresses, the relevant staking program, and an additional join on the dim_registry_metadata table for details pertaining to each service_id, such as name and description.

{% enddocs %}

{% docs olas_fact_mech_activity_table_doc %}

This fact-based table contains all mech requests (prompts) and delivered data, including offchain metadata links to view the requests/responses to/from AI models. For more information, please visit Olas' [Official Mech Documentation](https://docs.autonolas.network/product/mechkit/).

{% enddocs %}

{% docs olas_ez_mech_activity_table_doc %}

This convenience table consolidates mech request and delivery activities within the OLAS protocol, combining request and delivery data (`prompt` and `deliver` links) into a unified set of records, emulating the format of the [AIMechs app](https://aimechs.autonolas.network/mech). 

{% enddocs %}

