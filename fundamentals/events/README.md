## Events

Explorations and notes w.r.t. Ably streams, and supplementary data and tools.

<br>

### Ably

* [Ably Hub](https://www.ably.io/hub)
  * https://github.com/ably/ably-java
  * https://www.ably.io/documentation/realtime/channels

* [Ably OpenWeatherMap](https://www.ably.io/hub/ably-openweathermap)
  * [Product](https://www.ably.io/hub/ably-openweathermap/weather)
  
* Transport
  * [National Public Transport Access Node(NapTAN), National Public Transport Gazetteer (NPTG), etc.](https://naptan.app.dft.gov.uk/datarequest/help)
  
<br>

### Development

* [Avro Schema From JSON Generator](https://toolslick.com/generation/metadata/avro-schema-from-json)

<br>

* Maven, wherein SCM denotes Software Configuration Management
  * [Provider Matrix](https://maven.apache.org/scm/matrix.html)
  * [Maven SCM Plugin Usage](https://maven.apache.org/scm/maven-scm-plugin/usage.html)
  * [Git SCM](http://maven.apache.org/scm/git.html)
  * [Git SCM Configuration via SSH](https://github.com/marketplace/actions/maven-release)
  
<br>

### Environment

Running the Apache Spark Scala fat `.jar` via `--master local`

```bash
    spark-submit --class com.grey.events.EventsApp --master local['${numberOfCores}'] 
    target/events-'${buildNumber}'-jar-with-dependencies.jar
```

<br>
<br>
<br>
<br>
