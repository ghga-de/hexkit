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
# Persistent Publisher

With the basic Kafka publish provider (`KafkaEventPublisher`), event publication is not repeatable. The outbox publisher (`MongoKafkaDaoPublisher`) allows for a version of event publication, but mandates the use of a DAO in domain logic. There are plausible scenarios where the desired behavior is to publish events and persist them in a database via some transparent mechanism. Hexkit offers a way to do this via the `PersistentKafkaPublisher` ("Persistent Publisher"). The Persistent Publisher replaces existing usage of the `KafkaEventPublisher` and uses the MongoDB features exposed by Hexkit to store a copy of each event as it is published, along with supplementary information such as when the event was published, whether the event was successfully published, the correlation ID associated with the event, and more. Like the Outbox Publisher, the Persistent Publisher also provides methods to publish stored events that haven't been published yet or to publish all events regardless of publish status.

To learn more about the distinction between the Persistent Publisher and Outbox Publisher, see [this writeup](../../developer_help/outbox_v_persistent_pub.md).

## Persistence Model
The Persistent Publisher stores data according to the Pydantic model defined
[here](../../src/hexkit/providers/mongokafka/provider/persistent_pub.py):

```python
class PersistentKafkaEvent(BaseModel):
    """A model representing a kafka event to be published and stored in the database."""

    compaction_key: str = Field(
        ...,
        description="The unique ID of the event. If the topic is set to be compacted,"
        + " the ID is set to the topic and key in the format <topic>:<key>. Otherwise"
        + " the ID is set to the actual event ID.",
    )
    topic: Ascii = Field(..., description="The event topic")
    payload: JsonObject = Field(..., description="The event payload")
    key: Ascii = Field(..., description="The event key")
    type_: Ascii = Field(..., description="The event type")
    event_id: UUID4 | None = Field(default=None, description="The event ID")
    headers: Mapping[str, str] = Field(
        default_factory=dict,
        description="Non-standard event headers. Correlation ID, event_id, and event"
        + " type are transmitted as event headers, but added as such within the publisher"
        + " protocol. The headers here are any additional header that need to be sent.",
    )
    correlation_id: UUID4 = UUID4Field(description="The event correlation ID")
    created: datetime = Field(
        ..., description="The timestamp of when the event record was first inserted"
    )
    published: bool = Field(False, description="Whether the event has been published")
```

### Instantiation/Construction

The Persistent Publisher is meant to be used as an async context manager. The instance is created via the `construct()` method.

There are three parameters unique to the Persistent Publisher: `compacted_topics`, `topics_not_stored`, and `collection_name`.
- `compacted_topics` (set of strings):
  - Reflects remote topic compaction locally
  - Pass a set of topics as `compacted_topics` when constructing the publisher.
  - For those topics, the database uses a deterministic `compaction_key` of `<topic>:<key>` as the document ID, so only the latest event per (topic, key) is stored. This mimics Kafka log compaction on the storage side and makes “republish latest” semantics trivial.
  - For non-compacted topics, the `compaction_key` is the `event_id` (a UUID), so every event is stored independently. This does mean that, for events in non-compacted topics, the event ID is stored as a UUID4 in the `event_id` field and as a string in the `compaction_key` field.

- `topics_not_stored` (set of strings):
  - Provides a way to opt out of persistence for specified Kafka topics
  - Provide a set of topics that should be published but not stored. Those events are delegated to the plain `KafkaEventPublisher` that underpins the Persistent Publisher and will not appear in MongoDB.
  - `topics_not_stored` and `compacted_topics` must be disjoint; the provider will raise a `ValueError` if there is any overlap.

- `collection_name`:
  - Determines the name of the collection used for storing events. All events are stored in the same collection.
  - By default, events are stored in the collection named `{service_name}PersistedEvents` (derived from `MongoKafkaConfig.service_name`).
  - The MongoDB document ID is `compaction_key`; this is unique by definition and makes writes idempotent for compacted topics.

### Stored information and Considerations:
- Payload:
  - The event content, the payload, is stored in the field of the same name.
  - If developing a database migration that affects persisted events, take care to consider whether the payload itself needs to be updated as well.

- Correlation IDs and headers:
  - If a correlation ID is present in the context, it’s captured and stored; otherwise, a new one is generated.
  - On (re)publish, the stored correlation ID is set in context so downstream consumers see a stable trace.
  - Custom headers you pass are persisted and sent on publish; standard headers (correlation ID, event ID, type) are handled by the publisher protocol.

- Event ID and ordering:
  - The `created` field contains the timestamp denoting when the event was first published. It is not updated upon republishing.
  - `publish_pending` loads all documents with `published == false`, sorts by `created` ascending, and publishes them. This ensures that events are published in their original order.
  - `republish` iterates over all stored events; if an event lacks an `event_id`, it assigns one and marks `published = false` so the update is persisted. Then all events are published as above, regardless of whether they have been published already.
  - Events published to topics listed in `topics_not_stored` are never written to MongoDB, thus they won’t be affected by `publish_pending` or `republish`.

- Considerations:
  - DLQ: Avoid using a Persistent Publisher as the DLQ Publisher for an event subscriber instance. Events should only ever be published once to the DLQ.
  - Idempotence: Think about the consumers of events published by the Persistent Publisher and utilize `compacted_topics` and `topics_not_stored` strategically.
  - Republishing is usually performed as a one-off command for a service, rather than somewhere in standard operation. But this is a convention, and the methods can be utilized as best fits a given use-case.

## Usage example in service 'abc'

```python
from hexkit.providers.mongokafka.provider.persistent_pub import PersistentKafkaPublisher
from hexkit.providers.mongodb.provider import MongoDbDaoFactory
from hexkit.providers.mongokafka.provider.config import MongoKafkaConfig

# Normally, topics would be defined in configuration. This is only for conciseness:
COMPACTED_TOPICS = {"users"}
TOPICS_NOT_STORED = {"notifications"}

@asynccontextmanager
async def get_persistent_publisher(
    config: MongoKafkaConfig, dao_factory: MongoDbDaoFactory | None = None
) -> AsyncGenerator[PersistentKafkaPublisher]:
    """Construct and return a PersistentKafkaPublisher."""
    async with (
        MongoDbDaoFactory.construct(config=config) as _dao_factory,
        PersistentKafkaPublisher.construct(
            config=config,
            dao_factory=_dao_factory,
            compacted_topics=COMPACTED_TOPICS,
            topics_not_stored=TOPICS_NOT_STORED
            collection_name="abcPersistedEvents",
        ) as persistent_publisher,
    ):
        yield persistent_publisher
```
