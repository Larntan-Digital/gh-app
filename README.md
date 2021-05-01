# ghas-acs

FIXME

## Prerequisites

You will need [Leiningen][] 2.0.0 or above installed.

[leiningen]: https://github.com/technomancy/leiningen

## Running

To build the application, run and deploy in tomcat:

    lein uberwar
VM Options
    -Dclojure.core.async.pool-size=32 -Derl.gh.acs=path/to/dev-config.edn -Dlogback.configurationFile=path/to/logback.xml
## License

Copyright © 2020 FIXME
