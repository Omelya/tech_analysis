import asyncio
import json
from typing import Dict, Any, Optional
import aio_pika
from aio_pika import connect_robust, Message, DeliveryMode
import structlog


class RabbitMQPublisher:
    """RabbitMQ publisher for sending events to Laravel"""

    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.RobustChannel] = None
        self.logger = structlog.get_logger().bind(component="rabbitmq_publisher")

    async def initialize(self):
        """Initialize RabbitMQ connection and setup exchanges"""
        try:
            # Create connection
            self.connection = await connect_robust(self.connection_url)
            self.channel = await self.connection.channel()

            # Declare exchanges
            await self.setup_exchanges()

            self.logger.info("RabbitMQ publisher initialized successfully")

        except Exception as e:
            self.logger.error("Failed to initialize RabbitMQ publisher", error=str(e))
            raise

    async def setup_exchanges(self):
        """Setup RabbitMQ exchanges and queues"""
        # Main events exchange
        crypto_exchange = await self.channel.declare_exchange(
            'crypto_events',
            type=aio_pika.ExchangeType.TOPIC,
            durable=True
        )

        # Laravel queue for processing events
        laravel_queue = await self.channel.declare_queue(
            'laravel_crypto_events',
            durable=True
        )

        # Bind queue to exchange with routing keys
        await laravel_queue.bind(crypto_exchange, 'price.updated')
        await laravel_queue.bind(crypto_exchange, 'orderbook.updated')
        await laravel_queue.bind(crypto_exchange, 'kline.updated')
        await laravel_queue.bind(crypto_exchange, 'historical.updated')

        self.logger.info("RabbitMQ exchanges and queues setup completed")

    async def publish(self, exchange: str, routing_key: str, message: Dict[str, Any]):
        """Publish message to RabbitMQ"""
        try:
            if not self.channel:
                await self.initialize()

            # Create message
            message_body = json.dumps(message, default=str)

            # Publish message
            await self.channel.default_exchange.publish(
                Message(
                    message_body.encode(),
                    delivery_mode=DeliveryMode.PERSISTENT,
                    headers={
                        'content_type': 'application/json',
                        'timestamp': message.get('timestamp'),
                        'event_type': message.get('event')
                    }
                ),
                routing_key=f"{exchange}.{routing_key}"
            )

            self.logger.debug("Message published successfully",
                              exchange=exchange, routing_key=routing_key)

        except Exception as e:
            self.logger.error("Failed to publish message",
                              exchange=exchange, routing_key=routing_key, error=str(e))
            # Try to reconnect
            await self.reconnect()

    async def reconnect(self):
        """Reconnect to RabbitMQ"""
        try:
            if self.connection and not self.connection.is_closed:
                await self.connection.close()

            await self.initialize()
            self.logger.info("RabbitMQ reconnected successfully")

        except Exception as e:
            self.logger.error("Failed to reconnect to RabbitMQ", error=str(e))

    async def close(self):
        """Close RabbitMQ connection"""
        try:
            if self.connection and not self.connection.is_closed:
                await self.connection.close()

            self.logger.info("RabbitMQ connection closed")

        except Exception as e:
            self.logger.error("Error closing RabbitMQ connection", error=str(e))


class RabbitMQConsumer:
    """RabbitMQ consumer for receiving commands from Laravel"""

    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.RobustChannel] = None
        self.logger = structlog.get_logger().bind(component="rabbitmq_consumer")

    async def initialize(self):
        """Initialize RabbitMQ consumer"""
        try:
            self.connection = await connect_robust(self.connection_url)
            self.channel = await self.connection.channel()

            # Setup command queue for receiving commands from Laravel
            await self.setup_command_queue()

            self.logger.info("RabbitMQ consumer initialized successfully")

        except Exception as e:
            self.logger.error("Failed to initialize RabbitMQ consumer", error=str(e))
            raise

    async def setup_command_queue(self):
        """Setup command queue for receiving commands from Laravel"""
        # Command queue for microservice
        command_queue = await self.channel.declare_queue(
            'microservice_commands',
            durable=True
        )

        # Start consuming messages
        await command_queue.consume(self.process_command)

        self.logger.info("Command queue setup completed")

    async def process_command(self, message: aio_pika.IncomingMessage):
        """Process command from Laravel"""
        try:
            # Parse message
            command_data = json.loads(message.body.decode())

            self.logger.info("Received command", command=command_data)

            # Process different command types
            command_type = command_data.get('command')

            if command_type == 'fetch_historical':
                await self.handle_fetch_historical_command(command_data)
            elif command_type == 'subscribe_stream':
                await self.handle_subscribe_stream_command(command_data)
            elif command_type == 'update_trading_pairs':
                await self.handle_update_trading_pairs_command(command_data)
            else:
                self.logger.warning("Unknown command type", command_type=command_type)

            # Acknowledge message
            await message.ack()

        except Exception as e:
            self.logger.error("Failed to process command", error=str(e))
            await message.nack(requeue=False)

    async def handle_fetch_historical_command(self, command_data: Dict[str, Any]):
        """Handle historical data fetch command"""
        # Implementation for fetching historical data on demand
        pass

    async def handle_subscribe_stream_command(self, command_data: Dict[str, Any]):
        """Handle stream subscription command"""
        # Implementation for subscribing to streams on demand
        pass

    async def handle_update_trading_pairs_command(self, command_data: Dict[str, Any]):
        """Handle trading pairs update command"""
        # Implementation for updating active trading pairs
        pass

    async def close(self):
        """Close RabbitMQ consumer connection"""
        try:
            if self.connection and not self.connection.is_closed:
                await self.connection.close()

            self.logger.info("RabbitMQ consumer connection closed")

        except Exception as e:
            self.logger.error("Error closing RabbitMQ consumer connection", error=str(e))
