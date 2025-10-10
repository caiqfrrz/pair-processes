import Pyro5.api
import Pyro5.server
import Pyro5.nameserver
import time
import threading
import signal

@Pyro5.api.expose
class Peer(object):
    RELEASED = 0
    WANTED = 1
    HELD = 2

    def __init__(self, name, ns_host="::1", ns_port=9090):
        self.name = name
        self.ns_host = ns_host
        self.ns_port = ns_port

        self.state = self.RELEASED
        self.replies = set()
        self.deferred = []
        self.timestamp = None
        self.clock = 0

        self.max_cs_time = 12
        self.cs_timer = None

        self.heartbeat_interval = 2
        self.heartbeat_timeout = 6
        self.active_peers = set()
        self.lock = threading.Lock()
        self.last_heartbeat = {}

        self.thread_local = threading.local()
        self.peer_uris = {}

    def _get_proxy(self, peer_name):
        if not hasattr(self.thread_local, 'proxies'):
            self.thread_local.proxies = {}
        
        if peer_name not in self.thread_local.proxies:
            uri = self.peer_uris.get(peer_name)
            if uri:
                proxy = Pyro5.api.Proxy(uri)
                proxy._pyroTimeout = 3
                self.thread_local.proxies[peer_name] = proxy
        return self.thread_local.proxies.get(peer_name)
    
    def _force_release_cs(self):
        print(f"{self.name}: CS TIMEOUT - Forcing release!")
        self.release_cs()

    def discover_peers(self, ns):
        all_registered = ns.list()
        for peer_name, uri in all_registered.items():
            if peer_name != self.name and peer_name != "Pyro.NameServer":
                self.peer_uris[peer_name] = uri
                self.active_peers.add(peer_name)
                self.last_heartbeat[peer_name] = time.time()

    def list_active_peers(self):
        print("\nActive peers:")
        for index, peer in enumerate(self.active_peers, start=1):
            print(f"{index}. {peer}")

        print("\n")

    
    def request_access(self, sender, timestamp):
        self.clock = max(self.clock, timestamp) + 1

        with self.lock:
            if (self.state == self.HELD or (self.state == self.WANTED and (self.timestamp, self.name) < (timestamp, sender))):
                self.deferred.append(sender)
                return False
            else:
                return True
    
    def receive_reply(self, sender):
        with self.lock:
            self.replies.add(sender)
        return True
        
    def heartbeat(self, sender):
        with self.lock:
            self.last_heartbeat[sender] = time.time()

            if sender not in self.active_peers:
                try:
                    ns = Pyro5.api.locate_ns(host=self.ns_host, port=self.ns_port)
                    self.peer_uris[sender] = ns.lookup(sender)
                except Exception as e:
                    print(f"failed to add peer to the peers uri: {e}")
                    return True
                self.active_peers.add(sender)

        return True
    
    def start_sendind_heartbeat(self):
        def send_heartbeats():
            while True:
                with self.lock:
                    peers_to_check = list(self.active_peers)

                for peer_name in peers_to_check:
                    try:
                        proxy = self._get_proxy(peer_name)
                        if proxy:
                            proxy.heartbeat(self.name)
                    except Exception as e:
                        print(f"{self.name}: failed to send heartbeat to {peer_name}, error: {e}")
                time.sleep(self.heartbeat_interval)

        threading.Thread(target=send_heartbeats, daemon=True).start()

    def start_failure_detector(self):
        def detect_failures():
            while True:
                time.sleep(self.heartbeat_timeout)
                current_time = time.time()

                with self.lock:
                    for peer_name in list(self.active_peers):
                        last_hb = self.last_heartbeat.get(peer_name, 0)
                        if current_time - last_hb > self.heartbeat_timeout:
                            print(f"{self.name}: no heartbeat from {peer_name}, removing from active peers")
                            self.active_peers.remove(peer_name)

        threading.Thread(target=detect_failures, daemon=True).start()
    
    def request_cs(self):
        print(f"{self.name} REQUESTING CS")
        with self.lock:
            self.state = self.WANTED
            self.timestamp = self.clock
            self.clock += 1
            self.replies = set()
            peers_to_ask = list(self.active_peers)

        for peer_name in peers_to_ask:
            try:
                proxy = self._get_proxy(peer_name)
                if proxy and proxy.request_access(self.name, self.timestamp):
                    self.replies.add(peer_name)
            except Exception as e:
                print(f"{self.name}: Failed to request from {peer_name}: {e}")
                self.replies.add(peer_name)  # Assume failed peer grants

        request_timeout = self.max_cs_time
        start_time = time.time()

        while len(self.replies) < len(peers_to_ask):
            if time.time() - start_time > request_timeout:
                with self.lock:
                    missing_peers = set(peers_to_ask) - self.replies
                    for peer_name in missing_peers:
                        print(f"{self.name}: no response from {peer_name}, marking as inactive")
                        if peer_name in self.active_peers:
                            self.active_peers.remove(peer_name)
                        self.replies.add(peer_name)
                break
            time.sleep(0.1)

        with self.lock:
              self.state = self.HELD

    def use_resource(self):
        print(f"{self.name} IN CRITICAL SECTION")

        self.cs_timer = threading.Timer(self.max_cs_time, self._force_release_cs)
        self.cs_timer.start()

        with open("shared.log", "a") as f:
            f.write(f"{self.name} @ {time.time()}\n")
            
        # timeout simulation
        if self.name == "guilherme": 
            time.sleep(12)
        else:
            time.sleep(10)

        if self.cs_timer:
            self.cs_timer.cancel()

        self.release_cs()

    def release_cs(self):
        if self.cs_timer:
            self.cs_timer.cancel()
            self.cs_timer = None


        with self.lock:
            if self.state == self.RELEASED:
                  return
            
            self.state = self.RELEASED
            deferred_peers = self.deferred.copy()
            self.deferred = []

        print(f"{self.name} EXITING CS")

        for peer_name in deferred_peers:
            try:
                proxy = self._get_proxy(peer_name)
                if proxy:
                    proxy.receive_reply(self.name)
            except Exception as e:
                print(f"{self.name}: Failed to send reply to {peer_name}: {e}")

    def clean_up(self):
        print(f"\n{self.name}: Shutting down...")

        if self.state == self.WANTED or self.state == self.HELD:
            self.release_cs()

        try:
            ns = Pyro5.api.locate_ns(host="127.0.0.1")
            ns.remove(self.name)
            print(f"{self.name}: Unregistered from nameserver")
        except:
            pass
        exit(0)


def start_nameserver():
    try:
        Pyro5.nameserver.start_ns_loop(host="::1")  # IPv6 localhost
    except:
        pass

def print_menu():
    print("------------MENU------------")
    print("1. Request resource")
    print("2. Free resource")
    print("3. List active peers")
    print("4. Show this menu")
    print("5. Quit")
    print("----------------------------")

def cli_menu(peer):
    print_menu()
    while True:
        option = input("Select your option: ")

        if option == "1":
            peer.request_cs()
            peer.use_resource()
        elif option == "2":
            peer.release_cs()
        elif option == "3":
            peer.list_active_peers()
        elif option == "4":
            print_menu()
        elif option == "5":
            peer.clean_up()
            break
        else:
            print("Select a valid option:")
            print_menu()

def main():
    import sys
    name = sys.argv[1]

    try:
        threading.Thread(target=start_nameserver, daemon=True).start()
        time.sleep(2)
    except OSError:
        print(f"{name}: nameserver already running")
        time.sleep(1)

    ns = Pyro5.api.locate_ns(host="::1")  # IPv6 localhost

    peer = Peer(name, ns_host="::1")  # Ensure Peer uses IPv6
    daemon = Pyro5.server.Daemon(host="::1")  # Daemon on IPv6
    uri = daemon.register(peer)
    ns.register(name, uri)

    time.sleep(3)

    peer.discover_peers(ns)
    peer.start_sendind_heartbeat()
    peer.start_failure_detector()

    threading.Thread(target=daemon.requestLoop, daemon=True).start()

    signal.signal(signal.SIGINT, lambda sig, frame: peer.clean_up())

    cli_menu(peer)

if __name__ == "__main__":
    main()
