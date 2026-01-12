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
# Outbox Publisher

A common challenge in microservice development is the need to replicate data between services without introducing tight coupling. The solution to this offered by Hexkit is a special form of `MongoDbDao` grafted with a `KafkaEventPublisher`, called a `MongoDbDaoPublisher` ("Outbox Publisher"). Contrasted with the other provider in the `MongoKafka` subpackage, the Outbox Publisher is used in place of the basic `MongoDbDao` provider and event publishing occurs "in the background". In addition, instances are created via the factory class, `MongoDbDaoPublisherFactory`. More on that below. Like the Persistent Publisher, the Persistent Publisher also provides methods to publish stored events that haven't been published yet or to publish all events regardless of publish status. Using this provider requires you to install hexkit with both the `akafka` and `mongodb` extras.

### Instantiation/Construction

Construction via factory:
Use `MongoKafkaDaoPublisherFactory.construct(...).get_dao(...)` with:
- `name`: MongoDB collection name.
- `dto_model`: The class representing the domain object, which should inherit from Pydantic's `BaseModel`.
- `id_field`: Name of the ID field on the DTO (mapped to `_id` in MongoDB). This is used as the event key.
- `dto_to_event(dto) -> JsonObject | None`: Maps the DTO to the event payload. Return `None` to skip publishing for a given change.
- `event_topic`: Kafka topic where outbox events are emitted.
- `autopublish` (default: True): If False, changes are stored first and can be published later using `publish_pending()`.
- `fields_to_index` *not currently implemented.*

### Behavior

Domain object state changes are captured by the Outbox Publisher and categorized as either an *upsertion* or a *deletion*. This binary categorization is reflected in the Kafka event types that the Outbox Publisher uses, which are hardcoded by Hexkit: `CHANGE_EVENT_TYPE` ("changed") and `DELETE_EVENT_TYPE` ("deleted"). But the Outbox Publisher is designed to be able to replay changes, so how are deletions preserved? This is accomplished through additional data that is automatically managed by Hexkit and stored along with the domain data in the database. This data is stored in a field named `__metadata__` and tracks whether the data has been published as an event, what the event ID was for the last time the data was published, the correlation ID for the data, and a boolean indicating whether or not the data was deleted. When data managed by an Outbox Publisher is deleted, it is not entirely removed from the database. Instead, the ID field and metadata are kept (all other fields *are* deleted) and the `__metadata__.deleted` field is set to True. Even though the data still partially exists in the database, the Outbox Publisher will raise errors just as if the data did not exist. `get_by_id()` will raise a `ResourceNotFoundError` in this case.

    Metadata stored on documents:
    - `__metadata__.deleted` (bool): Whether the resource has been deleted.
    - `__metadata__.published` (bool): Whether the most recent change has been published to Kafka.
    - `__metadata__.correlation_id` (UUID): Correlation ID captured from context when the change was made; reused on (re)publish for tracing.
    - `__metadata__.last_event_id` (UUID | None): Event ID of the last emitted outbox event.

When data is either added or modified deleted, the Outbox Publisher feeds the data into the Factory's `dto_to_event()` method to obtain the desired representation for an event. This might match the original data, or it might remove fields, e.g. to avoid transmitting superfluous information. If the result of `dto_to_event()` is `None`, no event is published. When data is deleted, the Outbox Publisher publishes an event with an empty payload but the key set to the ID. Consumers can subscribe to the given outbox topic and perform the suitable action based on whether the event type is "changed" or "deleted". All event publishing depends on `autopublish` being set to True.

Republishing events:
- `publish_pending()`: Publishes all documents where `__metadata__.published == False` using the stored correlation ID and appropriate event type.
- `republish()`: Republishes the current state of all documents (change events for non-deleted, tombstones for deleted), again using each document’s stored correlation ID.

Correlation & Event IDs:
- The current correlation ID is captured and stored on each write.
- During (re)publish, the stored correlation ID is re-applied to the context so downstream consumers see a consistent trace.
- Event IDs are generated every time the data is (re)published, and the ID is stored after the event is published. This is different from the Persistent Publisher in that the latter will reuse the same event ID.


### Usage example
The following shows a contrived example of how to use the Outbox Publisher:

```python
from pydantic import BaseModel
from uuid import UUID

from hexkit.providers.mongokafka import MongoKafkaDaoPublisherFactory, MongoKafkaConfig

# Define a model
class User(BaseModel):
    user_id: UUID
    name: str
    email: str

# Determine how to represent the data as a Kafka event
def user_to_event(user: User) -> dict | None:
    return {
        "user_id": str(user.user_id),
        "name": user.name,
        "email": user.email,
    }

# Use MongoKafkaConfig to supply the Factory with the necessary information
config = MongoKafkaConfig()

# Construct the Factory as an async context manager
async with MongoKafkaDaoPublisherFactory.construct(config=config) as factory:
    user_dao = await factory.get_dao(
        name="users",
        dto_model=User,
        id_field="user_id",
        dto_to_event=user_to_event,
        event_topic="users.outbox",
        autopublish=True,
    )

    # Create or update data -> publishes a CHANGE event
    await user_dao.upsert(
        User(
            user_id=UUID("2e3975db-2c80-49f1-9f6b-cbb0174ca8f3"),
            name="Jane",
            email="jane@example.com",
        )
    )

    # Delete data -> publishes a DELETE tombstone
    await user_dao.delete(UUID("2e3975db-2c80-49f1-9f6b-cbb0174ca8f3"))

    # Operational controls
    await user_dao.publish_pending()  # Drain any unpublished changes
    await user_dao.republish()        # Replay full current state
```
