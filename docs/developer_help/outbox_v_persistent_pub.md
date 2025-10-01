<!--
 Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
 for the German Human Genome-Phenome Archive (GHGA)

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->
# Outbox vs Persistent Publisher: Choosing the Right One

!!! note "Work in Progress"
    This documentation section is still a draft that needs to be reviewed.

When using `KafkaEventPublisher`, produced events are generated and sent to the Kafka
broker in an ephemeral manner: there's no ability to generate the same events again.
The Persistent Publisher is a Hexkit provider that solves this problem by storing
a representation of events in MongoDB as they are published. This makes the database
the de facto backup of Kafka, to an extent. The Persistent Publisher class is
called `PersistentEventPublisher`, located in the `MongoKafka` subpackage.

The other `MongoKafka` provider, the Outbox Publisher, solves a different problem.
Microservices sometimes need to share data without sharing access to the same database.
The Outbox Publisher is essentially a MongoDB DAO retrofitted with a Kafka publisher.
It attaches a small amount of metadata to documents as it upserts or deletes data.
Upon each modifying operation, the provider run the given document through a
user-defined function to produce a Kafka payload which it then publishes. If a
document is deleted, the provider merely publishes a tombstone record with the object
ID as the key. When another service wants to access the data in question, it
subscribes to the Kafka topic housing the outbox events. The Outbox Publisher is
called `MongoKafkaDaoPublisher`, also located in the `MongoKafka` subpackage.

Both the `PersistentEventPublisher` and the `MongoKafkaDaoPublisher` save data to
the database, but they are intended for use in different situations, and are not
interchangeable.

### When to use the Persistent Publisher (`PersistentEventPublisher`)
The Persistent Publisher should be employed when:
- The publisher needs to be used like a normal `KafkaEventPublisher`.
- The data published *does not* represent a stateful entity, like a domain object.
- You want to be able to publish the same Kafka event again.
  - Same topic, event type, correlation ID, event ID, payload, etc.
  - Often these are events that initiate a sequence of events, or Kafka flow.

### When to use the Outbox Publisher (`MongoKafkaDaoPublisher`)
The Outbox Publisher should be used when:
- The publisher needs to be used like a normal `MongoDbDao`.
- The data published represents the latest state of a domain object
  - Examples: user data, access requests, order status, etc.
- Your service needs to inform other services about changes to a given MongoDB
  collection.
- You're focused on **storing** data. The Outbox Publisher cannot publish arbitrary
  events like the Persistent Publisher — it behaves like the MongoDB DAO and publishes
  events in the background.


## Further Details

### How the Two Publishers Store Information
The Persistent Publisher stores data according to the Pydantic model defined
[here](../../src/hexkit/providers/mongokafka/provider/persistent_pub.py):

```python
class PersistentKafkaEvent(BaseModel):
    """A model representing a kafka event to be published and stored in the database."""

    id: str = Field(
        ...,
        description="The unique ID of the event. If the topic is set to be compacted,"
        + " the ID is set to the topic and key in the format <topic>:<key>. Otherwise"
        + " the ID is set to a random UUID.",
    )
    topic: Ascii = Field(..., description="The event topic")
    type_: Ascii = Field(..., description="The event type")
    payload: JsonObject = Field(..., description="The event payload")
    key: Ascii = Field(..., description="The event key")
    headers: Mapping[str, str] = Field(
        default_factory=dict,
        description="Non-standard event headers. Correlation ID and event type are"
        + " transmitted as event headers, but added as such within the publisher"
        + " protocol. The headers here are any additional header that need to be sent.",
    )
    correlation_id: str = Field(..., description="The event correlation ID")
    created: datetime = Field(
        ..., description="The timestamp of when the event record was first inserted"
    )
    published: bool = Field(False, description="Whether the event has been published")
```

By contrast, the Outbox Publisher saves the domain object like a DAO normally would,
but adds a `__metadata__` field. If we were saving data from a `User` class with
a `user_id`, `name` and `email` field, the saved data might look like this:

```json
{
  "__metadata__": {
    "correlation_id": <UUID4>,
    "published": true,
    "deleted": false,
  },
  "_id": <UUID4>, // hexkit converts models' ID field names to the MongoDB `_id`
  "name": "John Doe",
  "email": "doe@example.com"
}
```

### Republishing
Both providers expose the methods `publish_pending` and `republish`. The former
only publishes documents where `published` is false, while the latter will republish
all its documents regardless of publish status. We suggest implementing a method
in your service accessible via CLI in order to trigger republishing as a job.
In both cases, the provider will use the stored correlation ID when republishing data.
