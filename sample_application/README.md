Tiny Sample KaufmannEx Application

Used Mostly for testing things.

Sample Application includes no application logic and the core components of a Kaufmann service.

All Sample service tests should pass, run a `mix test`.

To Run integration tests, start the parent docker-compose

```
docker-compose run kaufmann
cd sample-application

mix deps.get
mix test --include integration
```
