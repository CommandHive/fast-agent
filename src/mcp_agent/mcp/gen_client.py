from contextlib import asynccontextmanager
from datetime import timedelta
from typing import AsyncGenerator, Callable

from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from mcp import ClientSession

from mcp_agent.logging.logger import get_logger
from mcp_agent.mcp.interfaces import ServerRegistryProtocol
from mcp_agent.mcp.mcp_agent_client_session import MCPAgentClientSession

logger = get_logger(__name__)


@asynccontextmanager
async def gen_client(
    server_name: str,
    server_registry: ServerRegistryProtocol,
    client_session_factory: Callable[
        [MemoryObjectReceiveStream, MemoryObjectSendStream, timedelta | None],
        ClientSession,
    ] = MCPAgentClientSession,
) -> AsyncGenerator[ClientSession, None]:
    """
    Create a client session to the specified server.
    Handles server startup, initialization, and message receive loop setup.
    If required, callers can specify their own message receive loop and ClientSession class constructor to customize further.
    For persistent connections, use connect() or MCPConnectionManager instead.
    """
    print(f"🚀 DEBUG: gen_client() - STARTING connection to server '{server_name}'")
    if not server_registry:
        raise ValueError(
            "Server registry not found in the context. Please specify one either on this method, or in the context."
        )
    print(f"Why is the registration not happeneing!!!! server_registry: file gen_client.py {server_registry}")
    print(f"🔄 DEBUG: gen_client() - About to initialize server '{server_name}' via server registry")
    async with server_registry.initialize_server(
        server_name=server_name,
        client_session_factory=client_session_factory,
    ) as session:
        print(f"✅ DEBUG: gen_client() - Successfully initialized server '{server_name}', yielding session")
        yield session
        print(f"🔗 DEBUG: gen_client() - Session cleanup for server '{server_name}' completed")


async def connect(
    server_name: str,
    server_registry: ServerRegistryProtocol,
    client_session_factory: Callable[
        [MemoryObjectReceiveStream, MemoryObjectSendStream, timedelta | None],
        ClientSession,
    ] = MCPAgentClientSession,
) -> ClientSession:
    """
    Create a persistent client session to the specified server.
    Handles server startup, initialization, and message receive loop setup.
    If required, callers can specify their own message receive loop and ClientSession class constructor to customize further.
    """
    print(f"🚀 DEBUG: connect() - STARTING persistent connection to server '{server_name}'")
    if not server_registry:
        raise ValueError(
            "Server registry not found in the context. Please specify one either on this method, or in the context."
        )

    print(f"🔄 DEBUG: connect() - About to get server connection for '{server_name}' from connection manager")
    server_connection = await server_registry.connection_manager.get_server(
        server_name=server_name,
        client_session_factory=client_session_factory,
    )

    print(f"✅ DEBUG: connect() - Successfully got server connection for '{server_name}', returning session")
    return server_connection.session


async def disconnect(
    server_name: str | None,
    server_registry: ServerRegistryProtocol,
) -> None:
    """
    Disconnect from the specified server. If server_name is None, disconnect from all servers.
    """
    if not server_registry:
        raise ValueError(
            "Server registry not found in the context. Please specify one either on this method, or in the context."
        )

    if server_name:
        await server_registry.connection_manager.disconnect_server(server_name=server_name)
    else:
        await server_registry.connection_manager.disconnect_all_servers()
