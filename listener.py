#!/usr/bin/env python3

# interactive_kafka.py

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.abc import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import ssl
import signal
import threading
from concurrent.futures import ThreadPoolExecutor

def load_msk_config(config_dict=None):
    """
    Load MSK configuration from dictionary or environment variables
    
    Args:
        config_dict: Optional dictionary with MSK configuration
        
    Returns:
        Dictionary with MSK configuration
    """
    if config_dict:
        return {
            'bootstrap_servers': config_dict.get('bootstrap_servers', BOOTSTRAP_SERVERS),
            'topic_name': config_dict.get('topic_name', TOPIC_NAME),
            'aws_region': config_dict.get('aws_region', AWS_REGION),
            'consumer_group': config_dict.get('consumer_group', CONSUMER_GROUP),
        }
    
    return {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'topic_name': TOPIC_NAME,
        'aws_region': AWS_REGION,
        'consumer_group': CONSUMER_GROUP,
    }

# Configuration - Load from environment variables with defaults
BOOTSTRAP_SERVERS = os.environ.get('MSK_BOOTSTRAP_SERVERS', 
    'b-3-public.commandhive.aewd11.c4.kafka.ap-south-1.amazonaws.com:9198,b-1-public.commandhive.aewd11.c4.kafka.ap-south-1.amazonaws.com:9198,b-2-public.commandhive.aewd11.c4.kafka.ap-south-1.amazonaws.com:9198').split(',')
TOPIC_NAME = os.environ.get('MSK_TOPIC_NAME', 'mcp_agent_queen')
AWS_REGION = os.environ.get('AWS_REGION', 'ap-south-1')
CONSUMER_GROUP = os.environ.get('MSK_CONSUMER_GROUP', 'interactive-consumer-group')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global variables
shutdown_event = asyncio.Event()
producer = None
consumer = None
message_queue = asyncio.Queue()

def create_ssl_context():
    """
    Create SSL context for secure connection to MSK
    """
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.options |= ssl.OP_NO_SSLv2
    ssl_context.options |= ssl.OP_NO_SSLv3
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    ssl_context.load_default_certs()
    return ssl_context

class AWSTokenProvider(AbstractTokenProvider):
    """
    AWS MSK IAM token provider for authentication
    """
    def __init__(self, region=AWS_REGION):
        self.region = region
    
    async def token(self):
        """
        Generate and return AWS MSK IAM token
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._generate_token)
    
    def _generate_token(self):
        """
        Generate token using MSKAuthTokenProvider
        """
        try:
            token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
            return token
        except Exception as e:
            logger.error(f"Failed to generate auth token: {e}")
            raise

async def create_producer(bootstrap_servers):
    """
    Create and return an async Kafka producer with IAM authentication
    """
    try:
        tp = AWSTokenProvider()
        producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            security_protocol='SASL_SSL',
            ssl_context=create_ssl_context(),
            sasl_mechanism='OAUTHBEARER',
            sasl_oauth_token_provider=tp,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            client_id='interactive_producer',
            api_version="0.11.5"
        )
        
        await producer.start()
        logger.info("Kafka producer initialized successfully!")
        return producer
        
    except Exception as e:
        logger.error(f"Failed to create producer: {str(e)}")
        return None

async def create_consumer(bootstrap_servers, topic_name, consumer_group):
    """
    Create and return an async Kafka consumer with IAM authentication
    """
    try:
        tp = AWSTokenProvider()
        consumer = AIOKafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            group_id=consumer_group,
            security_protocol='SASL_SSL',
            ssl_context=create_ssl_context(),
            sasl_mechanism='OAUTHBEARER',
            sasl_oauth_token_provider=tp,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',  # Start from latest for interactive mode
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            client_id='interactive_consumer',
            api_version="0.11.5",
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        
        await consumer.start()
        logger.info(f"Kafka consumer initialized for topic '{topic_name}'!")
        return consumer
        
    except Exception as e:
        logger.error(f"Failed to create consumer: {str(e)}")
        return None

async def send_message(producer, topic_name, message_text, key=None):
    """
    Send a message to the Kafka topic
    """
    try:
        timestamp = time.time()
        message = {
            'content': message_text,
            'timestamp': timestamp,
            'datetime': datetime.fromtimestamp(timestamp).isoformat(),
            'sender': 'interactive-terminal',
            'type': 'user_message'
        }
        
        if not key:
            key = f"msg_{int(timestamp)}"
        
        record_metadata = await producer.send_and_wait(topic_name, key=key, value=message)
        
        print(f"✅ Message sent to partition {record_metadata.partition}, offset {record_metadata.offset}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to send message: {str(e)}")
        return False

async def consume_messages(consumer):
    """
    Consume messages from the Kafka topic and display them
    """
    try:
        async for msg in consumer:
            if shutdown_event.is_set():
                break
            
            timestamp_str = datetime.fromtimestamp(msg.timestamp / 1000).strftime("%H:%M:%S")
            
            # Format the received message
            if msg.value and isinstance(msg.value, dict):
                if 'text' in msg.value:
                    print(f"\n📨 [{timestamp_str}] {msg.value.get('sender', 'unknown')}: {msg.value['text']}")
                else:
                    print(f"\n📨 [{timestamp_str}] Raw message: {json.dumps(msg.value, indent=2)}")
            else:
                print(f"\n📨 [{timestamp_str}] Raw: {msg.value}")
            
            # Show the prompt again
            print("💬 Enter message (or 'quit' to exit): ", end="", flush=True)
                
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"❌ Error consuming messages: {str(e)}")

async def handle_user_input():
    """
    Handle user input in a separate coroutine
    """
    global producer
    
    executor = ThreadPoolExecutor(max_workers=1)
    
    try:
        while not shutdown_event.is_set():
            try:
                # Get user input asynchronously
                loop = asyncio.get_event_loop()
                user_input = await loop.run_in_executor(executor, input, "💬 Enter message (or 'quit' to exit): ")
                
                if user_input.lower() in ['quit', 'exit', 'q']:
                    print("👋 Goodbye!")
                    shutdown_event.set()
                    break
                
                if user_input.strip():
                    if producer:
                        await send_message(producer, TOPIC_NAME, user_input.strip())
                    else:
                        print("❌ Producer not available")
                
            except EOFError:
                # Handle Ctrl+D
                print("\n👋 Goodbye!")
                shutdown_event.set()
                break
            except Exception as e:
                print(f"❌ Error handling input: {str(e)}")
                
    finally:
        executor.shutdown(wait=False)

def signal_handler(signum, frame):
    """
    Handle shutdown signals gracefully
    """
    print(f"\n🛑 Received signal {signum}. Shutting down gracefully...")
    shutdown_event.set()

def print_banner():
    """
    Print application banner
    """
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                 🚀 Interactive Kafka Pub/Sub                ║
║                                                              ║
║  📤 Type messages to publish to the topic                   ║
║  📥 Incoming messages will appear automatically             ║
║  🔄 Real-time bidirectional communication                   ║
║                                                              ║
║  Commands:                                                   ║
║    • Type any message and press Enter to publish            ║
║    • Type 'quit', 'exit', or 'q' to exit                   ║
║    • Use Ctrl+C or Ctrl+D to exit                          ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)

def print_connection_info():
    """
    Print connection information
    """
    print(f"🔗 Bootstrap Servers: {', '.join(BOOTSTRAP_SERVERS)}")
    print(f"📝 Topic: {TOPIC_NAME}")
    print(f"👥 Consumer Group: {CONSUMER_GROUP}")
    print(f"🌍 Region: {AWS_REGION}")
    print("=" * 70)

async def main():
    """
    Main async function
    """
    global producer, consumer
    
    print_banner()
    print_connection_info()
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Create producer and consumer
        print("🔄 Initializing Kafka producer...")
        producer = await create_producer(BOOTSTRAP_SERVERS)
        if not producer:
            print("❌ Failed to create producer. Exiting.")
            return
        
        print("🔄 Initializing Kafka consumer...")
        consumer = await create_consumer(BOOTSTRAP_SERVERS, TOPIC_NAME, CONSUMER_GROUP)
        if not consumer:
            print("❌ Failed to create consumer. Exiting.")
            return
        
        print("✅ Successfully connected to Kafka!")
        print("🎯 Ready for interactive messaging!\n")
        
        # Start consumer and input handler concurrently
        consumer_task = asyncio.create_task(consume_messages(consumer))
        input_task = asyncio.create_task(handle_user_input())
        
        # Wait for shutdown
        await shutdown_event.wait()
        
        # Cancel tasks
        consumer_task.cancel()
        input_task.cancel()
        
        # Wait for tasks to complete
        try:
            await asyncio.gather(consumer_task, input_task, return_exceptions=True)
        except Exception as e:
            logger.debug(f"Task cancellation error: {e}")
        
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
    finally:
        # Clean up
        print("\n🧹 Cleaning up...")
        
        if producer:
            try:
                await producer.stop()
                print("✅ Producer stopped")
            except Exception as e:
                print(f"⚠️ Error stopping producer: {e}")
        
        if consumer:
            try:
                await consumer.stop()
                print("✅ Consumer stopped")
            except Exception as e:
                print(f"⚠️ Error stopping consumer: {e}")
    
    print("🏁 Interactive Kafka session ended!")

def run_async_main():
    """
    Run the async main function
    """
    try:
        # Set up proper signal handling for Windows compatibility
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Interrupted by user")
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")

if __name__ == "__main__":
    run_async_main()