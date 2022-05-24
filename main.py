import json
import os

from retry import retry
import bagit
import pulsar
from viaa.configuration import ConfigParser
from viaa.observability import logging
from logging import Logger

from cloudevents.events import (
    CEMessageMode,
    Event,
    EventOutcome,
    EventAttributes,
    PulsarBinding,
)

configParser = ConfigParser()
log = logging.get_logger(__name__, config=configParser)

APP_NAME = "sip-validator"
PULSAR_HOST = os.getenv("PULSAR_HOST", "localhost")
PULSAR_PORT = os.getenv("PULSAR_PORT", "6650")
CONSUMER_TOPIC = "be.meemoo.sipin.bag.unzip"
PRODUCER_BAG_TOPIC = "be.meemoo.sipin.bag.validate"
PRODUCER_SIP_TOPIC = "be.meemoo.sipin.sip.validate"

client = pulsar.Client(f"pulsar://{PULSAR_HOST}:{PULSAR_PORT}")


@retry(pulsar.ConnectError, tries=10, delay=1, backoff=2)
def create_producer(topic):
    return client.create_producer(topic)


@retry(pulsar.ConnectError, tries=10, delay=1, backoff=2)
def subscribe():
    return client.subscribe(CONSUMER_TOPIC, APP_NAME)


producer_bag = create_producer(PRODUCER_BAG_TOPIC)
producer_sip = create_producer(PRODUCER_SIP_TOPIC)
consumer = subscribe()


def create_event(
    event_type: str,
    attr_outcome: EventOutcome = EventOutcome.SUCCESS,
    subject: str = "",
    correlation_id: str = "",
    data_outcome: EventOutcome = EventOutcome.SUCCESS,
    **data_kwargs,
) -> Event:
    data = {"outcome": data_outcome.to_str(), **data_kwargs}
    attributes = EventAttributes(
        type=event_type,
        source=APP_NAME,
        subject=subject,
        outcome=attr_outcome,
        correlation_id=correlation_id,
    )
    return Event(attributes, data)


def send_event(producer, event: Event):
    msg = PulsarBinding.to_protocol(event, CEMessageMode.STRUCTURED.value)
    producer.send(
        msg.data,
        properties=msg.attributes,
        event_timestamp=event.get_event_time_as_int(),
    )


def validate_bag(path: str, correlation_id: str):
    try:
        bag = bagit.Bag(path)
    except bagit.BagError as e:
        event = create_event(
            PRODUCER_BAG_TOPIC,
            data_outcome=EventOutcome.FAIL,
            subject=path,
            correlation_id=correlation_id,
            message=f"Path '{path}' is not a valid bag: {str(e)}",
        )
        send_event(producer_bag, event)
        return False

    try:
        bag.validate()
    except bagit.BagValidationError as e:
        event = create_event(
            PRODUCER_BAG_TOPIC,
            data_outcome=EventOutcome.FAIL,
            subject=path,
            correlation_id=correlation_id,
            message=f"Path '{path}' is not a valid bag: {str(e.details)}",
        )
        send_event(producer_bag, event)
        return False
    send_event(
        producer_bag,
        create_event(
            PRODUCER_BAG_TOPIC,
            subject=path,
            destination=path,
            correlation_id=correlation_id,
            message=f"Path '{path}' is a valid bag",
        ),
    )
    return True


def validate_aip_creation(path: str):
    return True


def get_path(message: dict) -> str:
    """Given the data of the event, return the path of the bag"""
    return message["destination"]


if __name__ == "__main__":
    while True:
        msg = consumer.receive()
        try:
            event = PulsarBinding.from_protocol(msg)
            # Event with outcome == "success"
            if event.has_successful_outcome():
                data = event.get_data()
                log.info(f"incoming event: {data}")

                # Get the folder path to the bag
                path_directory_bag = get_path(data)
                log.info(f"bag folder: {path_directory_bag}")
                # Check if bag is valid
                if validate_bag(path_directory_bag, event.correlation_id):
                    log.info("is valid bag")
                    # Check if valid for AIP creation
                    if validate_aip_creation(path_directory_bag):
                        send_event(
                            producer_sip,
                            create_event(
                                PRODUCER_SIP_TOPIC,
                                subject=path_directory_bag,
                                correlation_id=event.correlation_id,
                                destination=path_directory_bag,
                                message=f"Path '{path_directory_bag}' is a valid SIP",
                            ),
                        )
                        log.info("Valid event sent")
                    else:
                        send_event(
                            producer_sip,
                            create_event(
                                PRODUCER_SIP_TOPIC,
                                data_outcome=EventOutcome.FAIL,
                                subject=path_directory_bag,
                                correlation_id=event.correlation_id,
                                destination=path_directory_bag,
                                message=f"Path '{path_directory_bag}' is a valid SIP",
                            ),
                        )
                        log.info("Invalid event sent")

            # Acknowledge successful processing of the message
            consumer.acknowledge(msg)
        except Exception as e:
            # Message failed to be processed
            log.error(e)
            consumer.negative_acknowledge(msg)

    client.close()
