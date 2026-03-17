"""
Lightweight asynchronous Nostr publisher and event listener.

Usage pattern:
    from nostr import init_global_publisher, get_publisher

    # Initialize once at startup
    init_global_publisher(privkey_nsec, relays=[...])

    # Publish events
    pub = get_publisher()
    if pub is not None:
        event = ...
        pub.publish_event(event)

    # Consume received events from channel
    event_channel = pub.get_event_channel()
    while True:
        ev = event_channel.get(timeout=5)
        print(ev)

Publishing and listening are done on background threads to avoid blocking hot paths.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
import uuid
from typing import List, Optional

from pynostr.event import Event
from pynostr.filters import Filters, FiltersList
from pynostr.key import PrivateKey
from pynostr.relay_manager import RelayManager
from pynostr.relay import Relay
from pynostr.base_relay import RelayPolicy
from pynostr.message_pool import MessagePool
import tornado.ioloop
from tornado import gen

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from logging_config import get_logger
from nostr.events import (
    TRADE_SIGNAL_KIND,
    COPYTRADE_INTENT_KIND,
    EXECUTION_REPORT_KIND,
)

logger = get_logger("nostr.publisher")


class NostrPublisher:
    """Async Nostr event publisher and listener backed by `RelayManager` and worker threads.

    Responsibilities:
    - Publish events asynchronously via a background thread
    - Subscribe to events and put them into a thread-safe channel for external consumers
    """

    # Default event kinds to subscribe for copy-trade or monitoring
    DEFAULT_LISTEN_KINDS = [
        TRADE_SIGNAL_KIND,
        COPYTRADE_INTENT_KIND,
        EXECUTION_REPORT_KIND,
    ]

    def __init__(
        self,
        *,
        private_key_nsec: str,
        relays: List[str],
        timeout: int = 6,
        sid: str = "moltrade-main",
        role: str = "node",
        listen_kinds: Optional[List[int]] = None,
    ) -> None:
        self._private_key = PrivateKey.from_nsec(private_key_nsec)
        self.public_key = self._private_key.public_key.hex()
        # Separate RelayManager for publishing (used by publisher thread)
        self._publish_relay_manager = RelayManager(timeout=timeout)
        self._sid = sid
        self._role = role
        self._relays = relays
        self._timeout = timeout
        self._listen_kinds = listen_kinds if listen_kinds is not None else self.DEFAULT_LISTEN_KINDS
        
        # Connection state tracking
        self._publish_connected = False
        self._publish_connect_lock = threading.Lock()

        # Add relays to publish manager
        for r in relays:
            try:
                self._publish_relay_manager.add_relay(r, close_on_eose=False)
            except Exception as exc:
                logger.warning(f"Failed to add Nostr relay {r}: {exc}")

        # Outgoing event queue (for publishing)
        self._publish_queue: queue.Queue[Event] = queue.Queue()

        # Incoming event channel (for external consumers)
        self._event_channel: queue.Queue[Event] = queue.Queue(maxsize=1000)

        # Subscription tracking
        self._subscriptions: dict = {}
        
        # Listener state
        self._listen_io_loop: Optional[tornado.ioloop.IOLoop] = None
        self._listen_relays: List[Relay] = []
        self._listen_message_pool: Optional[MessagePool] = None
        self._listen_callback: Optional[tornado.ioloop.PeriodicCallback] = None
        self._listen_cleanup_callback: Optional[tornado.ioloop.PeriodicCallback] = None
        self._seen_event_ids: set = set()
        self._max_seen_events = 10000  # Limit cache size

        # Stop signal for background threads
        self._stop = threading.Event()

        # Start publisher thread
        self._publisher_thread = threading.Thread(
            target=self._publisher_loop,
            name=f"NostrPublisher-{role}",
            daemon=True,
        )
        self._publisher_thread.start()

        # Start event listener thread
        self._listener_thread = threading.Thread(
            target=self._listener_loop,
            name=f"NostrListener-{role}",
            daemon=True,
        )
        self._listener_thread.start()

        logger.info(
            "Initialized NostrPublisher (role=%s, sid=%s, relays=%d)",
            role,
            sid,
            len(relays),
        )

    # -------------------------------------------------------------------------
    # Properties
    # -------------------------------------------------------------------------

    @property
    def sid(self) -> str:
        return self._sid

    @property
    def role(self) -> str:
        return self._role

    # -------------------------------------------------------------------------
    # Event Channel (for external consumers)
    # -------------------------------------------------------------------------

    def get_event_channel(self) -> queue.Queue[Event]:
        """Return the event channel (queue) for external consumers to get nostr events."""
        return self._event_channel

    # -------------------------------------------------------------------------
    # Publishing
    # -------------------------------------------------------------------------

    def publish_event(self, event: Event) -> None:
        """Enqueue an event for async publishing."""
        try:
            self._publish_queue.put_nowait(event)
        except queue.Full:
            logger.warning("NostrPublisher queue full; dropping event")

    def _publisher_loop(self) -> None:
        """Background loop that signs and publishes events."""
        # Connect once at startup
        with self._publish_connect_lock:
            if not self._publish_connected:
                try:
                    self._publish_relay_manager.run_sync()
                    self._publish_connected = True
                    logger.info(f"Publisher relays connected: {len(self._relays)} relays")
                except Exception as exc:
                    logger.warning(f"Failed to connect publisher relays: {exc}")
        
        while not self._stop.is_set():
            event = None
            try:
                event = self._publish_queue.get(timeout=0.5)
            except queue.Empty:
                pass  

            if event is not None:
                try:
                    event.sign(self._private_key.hex())
                    self._publish_relay_manager.publish_event(event)
                    
                    logger.info(
                        "Published Nostr event kind=%s id=%s role=%s",
                        getattr(event, "kind", None),
                        getattr(event, "id", None),
                        self._role,
                    )
                    
                    # Call run_sync to send the event
                    try:
                        self._publish_relay_manager.run_sync()
                        # Give relay more time to process and forward the event
                        time.sleep(0.5)
                    except Exception as exc:
                        logger.debug("PublishRelayManager.run_sync failed: %s", exc)
                    
                    # Drain OK notices
                    self._drain_message_pool()
                except Exception as exc:
                    logger.warning(f"Failed to publish Nostr event: {exc}")
            else:
                # Longer sleep when idle to reduce CPU usage
                time.sleep(0.5)


    # -------------------------------------------------------------------------
    # Subscribing / Listening
    # -------------------------------------------------------------------------

    def _listener_loop(self) -> None:
        """Background thread: use tornado io_loop to continuously listen for events."""
        # Create a new IOLoop for this thread
        self._listen_io_loop = tornado.ioloop.IOLoop()
        self._listen_message_pool = MessagePool(first_response_only=False)
        policy = RelayPolicy()
        

        current_time = int(time.time())
        filters = FiltersList([Filters(kinds=self._listen_kinds, since=current_time)])
        subscription_id = uuid.uuid1().hex
        
        for relay_url in self._relays:
            try:
                relay = Relay(
                    relay_url, 
                    self._listen_message_pool, 
                    self._listen_io_loop, 
                    policy, 
                    timeout=self._timeout, 
                    close_on_eose=False
                )
                relay.add_subscription(subscription_id, filters)
                self._listen_relays.append(relay)
            except Exception as exc:
                logger.warning(f"Failed to create listener relay for {relay_url}: {exc}")
        
        if not self._listen_relays:
            logger.warning("No listener relays available")
            return
        
        logger.info(
            f"Nostr event listener started for kinds={self._listen_kinds}, sub_id={subscription_id}"
        )
        
        def poll_events():
            """Periodically poll message pool for new events."""
            if self._stop.is_set():
                self._listen_io_loop.stop()
                return
            
            try:
                # Check for new events
                while self._listen_message_pool.has_events():
                    event_msg = self._listen_message_pool.get_event()
                    ev = getattr(event_msg, "event", None)
                    
                    if ev is not None and ev.id not in self._seen_event_ids:
                        self._seen_event_ids.add(ev.id)
                        try:
                            self._event_channel.put_nowait(ev)
                            logger.debug(f"Listener received event: kind={ev.kind}, id={ev.id[:16]}...")
                        except queue.Full:
                            logger.warning("Nostr event channel full; dropping event")
                
                # Drain EOSE notices
                while self._listen_message_pool.has_eose_notices():
                    self._listen_message_pool.get_eose_notice()
            except Exception as exc:
                logger.debug(f"Error polling events: {exc}")
        
        @gen.coroutine
        def connect_relays():
            """Connect to all relays once."""
            for relay in self._listen_relays:
                try:
                    yield relay.connect()
                    logger.info(f"Listener connected to relay: {relay.url}")
                except Exception as e:
                    logger.warning(f"Failed to connect to listener relay {relay.url}: {e}")
        
        # Set up periodic callback to poll for events (every 1000ms to reduce load)
        self._listen_callback = tornado.ioloop.PeriodicCallback(poll_events, 1000)
        self._listen_callback.start()
        
        def cleanup_seen_events():
            """Periodically clean up old seen event IDs to prevent memory leak."""
            if len(self._seen_event_ids) > self._max_seen_events:
                # Keep only the most recent half
                old_size = len(self._seen_event_ids)
                self._seen_event_ids = set(list(self._seen_event_ids)[-self._max_seen_events // 2:])
                logger.debug(f"Cleaned up seen_event_ids: {old_size} -> {len(self._seen_event_ids)}")
        
        # Clean up every 5 minutes
        self._listen_cleanup_callback = tornado.ioloop.PeriodicCallback(cleanup_seen_events, 300000)
        self._listen_cleanup_callback.start()
        
        # Spawn the connection in background (non-blocking)
        self._listen_io_loop.spawn_callback(connect_relays)
        
        try:
            # Keep the io_loop running to receive WebSocket messages
            self._listen_io_loop.start()
        except Exception as e:
            logger.warning(f"Listener io_loop error: {e}")
        finally:
            if self._listen_callback:
                self._listen_callback.stop()
            if self._listen_cleanup_callback:
                self._listen_cleanup_callback.stop()
            for relay in self._listen_relays:
                try:
                    relay.close()
                except Exception:
                    pass
            logger.debug("Listener loop stopped")

    # -------------------------------------------------------------------------
    # Internal Helpers
    # -------------------------------------------------------------------------

    def _drain_message_pool(self) -> None:
        """Drain ok notices and events from the relay manager's message pool."""
        mp = getattr(self._publish_relay_manager, "message_pool", None)
        if mp is None:
            return

        # Drain OK notices
        try:
            while getattr(mp, "has_ok_notices", lambda: False)():
                ok_msg = mp.get_ok_notice()
                logger.info(f"Received OK notice: {ok_msg}")
        except Exception as exc:
            logger.debug(f"Error draining OK notices: {exc}")

        # Drain events (for publisher thread, just log them)
        try:
            while getattr(mp, "has_events", lambda: False)():
                event_msg = mp.get_event()
                ev = getattr(event_msg, "event", None)
                if ev is not None:
                    try:
                        logger.debug(f"Publisher received event: kind={ev.kind}, id={ev.id[:16]}...")
                    except Exception:
                        logger.debug("Publisher received event (non-dict): %s", ev)
                else:
                    logger.debug("Publisher received message: %s", event_msg)
        except Exception as exc:
            logger.debug(f"Error draining events: {exc}")

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def shutdown(self) -> None:
        """Stop all background threads and close relay connections."""
        self._stop.set()
        
        # Stop listener io_loop if running
        if self._listen_io_loop is not None:
            try:
                self._listen_io_loop.add_callback(self._listen_io_loop.stop)
            except Exception:
                pass
        
        try:
            self._publisher_thread.join(timeout=2.0)
        except Exception:
            pass
        try:
            self._listener_thread.join(timeout=2.0)
        except Exception:
            pass
        try:
            self._publish_relay_manager.close()
        except Exception:
            pass
        # Listener relays are closed in _listener_loop finally block
        logger.info("NostrPublisher shutdown complete")


# =============================================================================
# Global Publisher Singleton
# =============================================================================

_GLOBAL_PUBLISHER: Optional[NostrPublisher] = None


def init_global_publisher(
    private_key_nsec: str,
    relays: Optional[List[str]] = None,
    *,
    sid: str = "moltrade-main",
    role: str = "node",
    listen_kinds: Optional[List[int]] = None,
) -> None:
    """
    Initialize the process-wide NostrPublisher.

    Safe to call multiple times; later calls are ignored once initialized.
    """
    global _GLOBAL_PUBLISHER
    if _GLOBAL_PUBLISHER is not None:
        return

    relays = relays or []
    if not private_key_nsec:
        logger.warning("Empty Nostr private key provided; publisher not initialized")
        return

    try:
        _GLOBAL_PUBLISHER = NostrPublisher(
            private_key_nsec=private_key_nsec,
            relays=relays,
            sid=sid,
            role=role,
            listen_kinds=listen_kinds,
        )
    except Exception as exc:
        logging.getLogger(__name__).warning(
            f"Failed to init global Nostr publisher: {exc}"
        )


def get_publisher() -> Optional[NostrPublisher]:
    """Return the process-wide NostrPublisher if initialized, else None."""
    return _GLOBAL_PUBLISHER


__all__ = ["NostrPublisher", "init_global_publisher", "get_publisher"]


