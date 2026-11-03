# Fujin Data Simulation

Fujin is an independent local cloud laboratory. It owns deterministic datasets,
virtual S3/Glacier and OCI Object Storage behavior, restore policies, fault
profiles, payload materialization, and the related administrative APIs.

It does not contain Raijin projects, waves, queues, workers, credentials, or
operational databases. Raijin consumes Fujin only through private provider
endpoints and normal AWS/OCI connection settings.

## Migration baseline

This first repository revision preserves the Fujin implementation that was
previously co-located with Raijin. The next commits replace the transitional
Raijin-named runtime variables and compose topology with Fujin-only names and
publish the provider contract independently.
