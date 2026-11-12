"""Measurement contract shared by every platform collector."""

import uuid
from datetime import UTC, datetime
from typing import Annotated, ClassVar, Literal, Self

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)

EVENT_ID_NAMESPACE: uuid.UUID = uuid.uuid5(uuid.NAMESPACE_URL, "gamedev-trends:event")


def _to_utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


type Name = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]*$", min_length=1, max_length=32)]
type EntityId = Annotated[str, StringConstraints(min_length=1, max_length=1024)]
type UtcDatetime = Annotated[AwareDatetime, AfterValidator(_to_utc)]


def make_event_id(*parts: str) -> uuid.UUID:
    """uuid5(EVENT_ID_NAMESPACE, "|".join(parts))."""
    return uuid.uuid5(EVENT_ID_NAMESPACE, "|".join(parts))


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


class Observation(BaseModel):
    """A number observed for an entity on a platform: a snapshot or a sum over a window.

    Convention for collectors: for an instant value (window_start is None) set observed_at to the
    planned poll slot time (for example exactly 10:00:00 UTC), not the time the platform answered.
    Otherwise a repeated poll gets a different event_id and deduplication fails. The contract
    neither rounds nor checks this.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")

    kind: Literal["observation"] = "observation"
    event_id: uuid.UUID
    source: Name
    entity_type: Name
    entity_id: EntityId
    metric: Name
    value: float = Field(allow_inf_nan=False)
    observed_at: UtcDatetime
    window_start: UtcDatetime | None = None

    @model_validator(mode="after")
    def _check_window(self) -> Self:
        if self.window_start is not None and self.window_start >= self.observed_at:
            raise ValueError("window_start must be earlier than observed_at")
        return self

    @classmethod
    def create(
        cls,
        *,
        source: str,
        entity_type: str,
        entity_id: str,
        metric: str,
        value: float,
        observed_at: datetime,
        window_start: datetime | None = None,
    ) -> Observation:
        event_id: uuid.UUID = make_event_id(
            "observation",
            source,
            entity_type,
            entity_id,
            metric,
            "" if window_start is None else _iso(window_start),
            _iso(observed_at),
        )
        return cls(
            event_id=event_id,
            source=source,
            entity_type=entity_type,
            entity_id=entity_id,
            metric=metric,
            value=value,
            observed_at=observed_at,
            window_start=window_start,
        )


class Erasure(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")

    kind: Literal["erasure"] = "erasure"
    event_id: uuid.UUID
    source: Name
    entity_type: Name
    entity_id: EntityId
    requested_at: UtcDatetime

    @classmethod
    def create(cls, *, source: str, entity_type: str, entity_id: str, requested_at: datetime) -> Erasure:
        return cls(
            event_id=make_event_id("erasure", source, entity_type, entity_id),
            source=source,
            entity_type=entity_type,
            entity_id=entity_id,
            requested_at=requested_at,
        )


type Event = Observation | Erasure


def erases(erasure: Erasure, observation: Observation) -> bool:
    """True if source, entity_type and entity_id all match."""
    return (
        erasure.source == observation.source
        and erasure.entity_type == observation.entity_type
        and erasure.entity_id == observation.entity_id
    )
