import asyncio
import argparse
import aiohttp
import ssl
from typing import Any, Dict

async def main(queue: asyncio.Queue, args: Dict[str, Any]):
    """
    Polls Proxmox API for VMs and LXC containers that transition from running to stopped.
    This detects crashes and unexpected shutdowns (not intentional stops from UI).
    """
    node = args.get("node")
    host = args.get("host")
    user = args.get("user")  # e.g., root@pam
    token_id = args.get("token_id")  # e.g., mytoken
    token_secret = args.get("token_secret")
    verify_ssl = args.get("verify_ssl", False)
    delay = args.get("delay", 60)

    if not all([node, host, user, token_id, token_secret]):
        print("Missing required arguments for proxmox_monitor source")
        return

    # Proxmox API Token Header Format: PVEAPIToken=USER@REALM!TOKENID=UUID
    headers = {
        "Authorization": f"PVEAPIToken={user}!{token_id}={token_secret}"
    }

    urls = {
        "vm": f"https://{host}/api2/json/nodes/{node}/qemu",
        "lxc": f"https://{host}/api2/json/nodes/{node}/lxc"
    }

    # Track previous state of each VM/LXC to detect transitions
    # Format: {(resource_type, vmid): previous_status}
    previous_states: Dict[tuple, str] = {}

    # Configure SSL context
    ssl_context = None
    if not verify_ssl:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    print(f"Starting Proxmox Monitor for node {node} on {host}...")

    while True:
        # Create a new connector and session for each iteration to avoid session closure issues
        connector = aiohttp.TCPConnector(ssl=ssl_context if not verify_ssl else True)
        try:
            async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
                for resource_type, url in urls.items():
                    try:
                        async with session.get(url) as response:
                            if response.status == 200:
                                data = await response.json()
                                items = data.get("data", [])
                                
                                for item in items:
                                    vmid = item.get("vmid")
                                    current_status = item.get("status", "unknown")
                                    state_key = (resource_type, vmid)
                                    
                                    # Get previous status (default to "unknown" for first poll)
                                    previous_status = previous_states.get(state_key, "unknown")
                                    
                                    # Only emit event if VM transitions from "running" to "stopped"
                                    # This catches crashes and unexpected shutdowns
                                    if previous_status == "running" and current_status == "stopped":
                                        event = {
                                            "proxmox": {
                                                "type": resource_type,
                                                "name": item.get("name"),
                                                "vmid": vmid,
                                                "status": current_status,
                                                "previous_status": previous_status,
                                                "node": node,
                                                "uptime": item.get("uptime", 0),  # May be useful for diagnostics
                                            },
                                            "meta": {
                                                "source": "proxmox_monitor",
                                                "event_type": "vm_stopped_unexpectedly"
                                            }
                                        }
                                        await queue.put(event)
                                        print(f"Detected {resource_type} {vmid} ({item.get('name')}) transitioned from running to stopped")
                                    
                                    # Update state tracking
                                    previous_states[state_key] = current_status
                            else:
                                print(f"Failed to fetch {resource_type}: {response.status}")
                    except Exception as e:
                        print(f"Error polling {resource_type}: {e}")
        except Exception as e:
            print(f"Error creating session: {e}")
        finally:
            # Explicitly close connector to ensure clean shutdown
            if not connector.closed:
                await connector.close()
        
        await asyncio.sleep(delay)


async def test_connection(args: Dict[str, Any]) -> bool:
    """
    Test connection to Proxmox API before starting monitoring.
    Returns True if connection is successful, False otherwise.
    """
    node = args.get("node")
    host = args.get("host")
    user = args.get("user")
    token_id = args.get("token_id")
    token_secret = args.get("token_secret")
    verify_ssl = args.get("verify_ssl", False)

    print(f"Testing connection to Proxmox at {host}...")
    
    # Proxmox API Token Header Format: PVEAPIToken=USER@REALM!TOKENID=UUID
    headers = {
        "Authorization": f"PVEAPIToken={user}!{token_id}={token_secret}"
    }

    # Configure SSL context
    ssl_context = None
    if not verify_ssl:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    connector = aiohttp.TCPConnector(ssl=ssl_context if not verify_ssl else True)

    try:
        # Test 1: Check API version (basic connectivity)
        print(f"  [1/3] Testing API connectivity...")
        async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
            version_url = f"https://{host}/api2/json/version"
            async with session.get(version_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    version_data = await response.json()
                    print(f"         ✓ Connected! Proxmox version: {version_data.get('data', {}).get('version', 'unknown')}")
                else:
                    print(f"         ✗ Failed: HTTP {response.status}")
                    return False

            # Test 2: Verify authentication by checking nodes
            print(f"  [2/3] Verifying authentication...")
            nodes_url = f"https://{host}/api2/json/nodes"
            async with session.get(nodes_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    nodes_data = await response.json()
                    available_nodes = [n.get("node") for n in nodes_data.get("data", [])]
                    print(f"         ✓ Authentication successful! Available nodes: {', '.join(available_nodes)}")
                elif response.status == 401:
                    print(f"         ✗ Authentication failed: Invalid credentials")
                    return False
                else:
                    print(f"         ✗ Failed: HTTP {response.status}")
                    return False

            # Test 3: Verify the specified node exists and is accessible
            print(f"  [3/3] Verifying node '{node}' access...")
            node_url = f"https://{host}/api2/json/nodes/{node}/status"
            async with session.get(node_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    node_data = await response.json()
                    node_status = node_data.get("data", {})
                    print(f"         ✓ Node accessible! Status: {node_status.get('status', 'unknown')}")
                    return True
                elif response.status == 404:
                    print(f"         ✗ Node '{node}' not found")
                    return False
                else:
                    print(f"         ✗ Failed to access node: HTTP {response.status}")
                    return False

    except aiohttp.ClientConnectorError as e:
        print(f"  ✗ Connection error: {e}")
        print(f"    Check that Proxmox is reachable at {host}")
        return False
    except asyncio.TimeoutError:
        print(f"  ✗ Connection timeout: Proxmox at {host} did not respond")
        return False
    except Exception as e:
        print(f"  ✗ Unexpected error: {e}")
        return False


if __name__ == "__main__":
    # Mock run for testing purposes only
    # NOTE: When run by EDA, values come from the rulebook vars section
    # Usage: python proxmox_monitor.py --host 192.168.1.100:8006 --node pve --user root@pam --token-id ansible --token-secret YOUR_SECRET
    parser = argparse.ArgumentParser(
        description="Test Proxmox monitor event source",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use defaults (matching rulebook vars)
  python proxmox_monitor.py
  
  # Override specific values
  python proxmox_monitor.py --host 192.168.1.200:8006 --node pve2
  
  # Full example with all parameters
  python proxmox_monitor.py --host 192.168.1.100:8006 --node pve --user root@pam \\
      --token-id ansible --token-secret YOUR_SECRET_UUID --verify-ssl --delay 60
        """
    )
    parser.add_argument("--host", default="192.168.1.100:8006",
                        help="Proxmox host:port (default: 192.168.1.100:8006)")
    parser.add_argument("--node", default="pve",
                        help="Proxmox node name (default: pve)")
    parser.add_argument("--user", default="root@pam",
                        help="Proxmox user (default: root@pam)")
    parser.add_argument("--token-id", default="ansible",
                        help="Proxmox token ID (default: ansible)")
    parser.add_argument("--token-secret", default="YOUR_SECRET_UUID",
                        help="Proxmox token secret (default: YOUR_SECRET_UUID)")
    parser.add_argument("--verify-ssl", action="store_true",
                        help="Verify SSL certificates (default: False)")
    parser.add_argument("--delay", type=int, default=30,
                        help="Poll delay in seconds (default: 30)")
    
    args = parser.parse_args()
    
    class MockQueue:
        async def put(self, event):
            print(f"Event: {event}")
    
    test_args = {
        "host": args.host,
        "node": args.node,
        "user": args.user,
        "token_id": args.token_id,
        "token_secret": args.token_secret,
        "verify_ssl": args.verify_ssl,
        "delay": args.delay
    }
    
    print("Running Proxmox monitor in test mode...")
    print(f"Configuration: {test_args}\n")
    
    # Test connection before starting monitoring
    connection_ok = asyncio.run(test_connection(test_args))
    
    if not connection_ok:
        print("\n❌ Connection test failed. Please check your configuration and try again.")
        print("   Verify:")
        print("   - Proxmox host is reachable")
        print("   - Node name is correct")
        print("   - Token credentials are valid")
        exit(1)
    
    print("\n✅ Connection test passed! Starting monitoring...")
    print("Press Ctrl+C to stop\n")
    
    asyncio.run(main(MockQueue(), test_args))