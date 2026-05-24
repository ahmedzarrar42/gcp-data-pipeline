"""
Google Cloud Pub/Sub publisher for the data pipeline.
Handles message publishing with batching, ordering, and error handling.
"""
import json
import logging
from typing import Any, Optional
from concurrent.futures import TimeoutError

from google.cloud import pubsub_v1
from google.api_core.exceptions import GoogleAPIError
import structlog

logger = structlog.get_logger(__name__)


class PubSubPublisher:
    """
    Pub/Sub message publisher with batching and error handling.

    Publishes scrape results and pipeline events to GCP Pub/Sub topics
    for downstream processing by subscribers.

    Example:
        publisher = PubSubPublisher(project_id="my-project", topic_id="scraper-results")
        publisher.publish({"url": "https://example.com", "data": {...}})
    """

    def __init__(
        self,
        project_id: str,
        topic_id: str,
        batch_max_messages: int = 100,
        batch_max_bytes: int = 1_000_000,  # 1MB
        batch_max_latency: float = 0.01,   # 10ms
    ):
        self.project_id = project_id
        self.topic_id = topic_id
        self.topic_path = f"projects/{project_id}/topics/{topic_id}"

        batch_settings = pubsub_v1.types.BatchSettings(
            max_messages=batch_max_messages,
            max_bytes=batch_max_bytes,
            max_latency=batch_max_latency,
        )
        self.publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings)
        self.logger = structlog.get_logger(self.__class__.__name__)

    def publish(
        self,
        data: dict,
        attributes: Optional[dict[str, str]] = None,
        timeout: float = 10.0,
    ) -> str:
        """
        Publish a single message to Pub/Sub.

        Args:
            data: Message payload as dict (will be JSON-encoded)
            attributes: Optional message attributes for filtering
            timeout: Publish timeout in seconds

        Returns:
            Message ID from Pub/Sub
        """
        message_bytes = json.dumps(data, default=str).encode("utf-8")
        attributes = attributes or {}

        try:
            future = self.publisher.publish(
                self.topic_path,
                data=message_bytes,
                **attributes,
            )
            message_id = future.result(timeout=timeout)
            self.logger.info(
                "message_published",
                topic=self.topic_id,
                message_id=message_id,
            )
            return message_id

        except TimeoutError:
            self.logger.error("publish_timeout", topic=self.topic_id)
            raise
        except GoogleAPIError as e:
            self.logger.error("publish_failed", topic=self.topic_id, error=str(e))
            raise

    def publish_batch(
        self,
        messages: list[dict],
        attributes: Optional[dict[str, str]] = None,
    ) -> list[str]:
        """
        Publish a batch of messages efficiently using Pub/Sub batching.

        Args:
            messages: List of message payloads
            attributes: Shared attributes applied to all messages

        Returns:
            List of message IDs
        """
        futures = []
        attributes = attributes or {}

        for message in messages:
            message_bytes = json.dumps(message, default=str).encode("utf-8")
            future = self.publisher.publish(
                self.topic_path,
                data=message_bytes,
                **attributes,
            )
            futures.append(future)

        message_ids = []
        for i, future in enumerate(futures):
            try:
                message_id = future.result(timeout=10.0)
                message_ids.append(message_id)
            except Exception as e:
                self.logger.error(
                    "batch_message_failed",
                    index=i,
                    error=str(e),
                )

        self.logger.info(
            "batch_published",
            topic=self.topic_id,
            total=len(messages),
            successful=len(message_ids),
            failed=len(messages) - len(message_ids),
        )
        return message_ids

    def ensure_topic_exists(self) -> None:
        """Create the topic if it does not already exist."""
        try:
            self.publisher.create_topic(request={"name": self.topic_path})
            self.logger.info("topic_created", topic=self.topic_path)
        except Exception as e:
            if "already exists" in str(e).lower():
                self.logger.debug("topic_exists", topic=self.topic_path)
            else:
                raise
