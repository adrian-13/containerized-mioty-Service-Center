#!/usr/bin/env python3
"""
TLS Test Server - spusti na PC
Použitie: python3 tls_server.py --cert server.crt --key server.key [--ca ca.crt]
"""

import socket
import ssl
import argparse
import sys
from datetime import datetime
import threading

class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    RESET = '\033[0m'

def log(message, color=Colors.RESET):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{color}[{timestamp}] {message}{Colors.RESET}")

def handle_client(conn, addr, client_id):
    """Spracovanie klientskeho pripojenia"""
    try:
        log(f"Client #{client_id} connected from {addr[0]}:{addr[1]}", Colors.GREEN)
        
        # Získaj informácie o certifikáte klienta (ak je poskytnutý)
        try:
            cert = conn.getpeercert()
            if cert:
                log(f"Client #{client_id} certificate:", Colors.BLUE)
                if 'subject' in cert:
                    for item in cert['subject']:
                        for key, value in item:
                            log(f"  {key}: {value}", Colors.BLUE)
                log(f"  Cipher: {conn.cipher()}", Colors.BLUE)
            else:
                log(f"Client #{client_id} - No client certificate provided", Colors.YELLOW)
        except Exception as e:
            log(f"Client #{client_id} - Certificate info not available: {e}", Colors.YELLOW)
        
        # Pošli uvítaciu správu
        welcome_msg = (
            "TLS Connection Successful!\n"
            f"Server time: {datetime.now().isoformat()}\n"
            f"Your IP: {addr[0]}\n"
            f"Cipher: {conn.cipher()[0]}\n"
            f"TLS Version: {conn.version()}\n"
            "---END---\n"
        )
        conn.sendall(welcome_msg.encode('utf-8'))
        
        # Čítaj dáta od klienta
        while True:
            data = conn.recv(1024)
            if not data:
                break
            
            message = data.decode('utf-8').strip()
            log(f"Client #{client_id} sent: {message}", Colors.BLUE)
            
            # Echo odpoveď
            response = f"Server received: {message}\n"
            conn.sendall(response.encode('utf-8'))
            
            if message.upper() == 'QUIT':
                log(f"Client #{client_id} requested disconnect", Colors.YELLOW)
                break
        
    except ssl.SSLError as e:
        log(f"Client #{client_id} SSL Error: {e}", Colors.RED)
    except Exception as e:
        log(f"Client #{client_id} Error: {e}", Colors.RED)
    finally:
        conn.close()
        log(f"Client #{client_id} disconnected", Colors.YELLOW)

def create_ssl_context(certfile, keyfile, cafile=None, require_client_cert=False):
    """Vytvorenie SSL/TLS kontextu"""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    
    # Načítaj server certifikát a kľúč
    try:
        context.load_cert_chain(certfile=certfile, keyfile=keyfile)
        log(f"✓ Server certificate loaded: {certfile}", Colors.GREEN)
        log(f"✓ Server key loaded: {keyfile}", Colors.GREEN)
    except Exception as e:
        log(f"✗ Failed to load server certificate/key: {e}", Colors.RED)
        sys.exit(1)
    
    # Ak je zadaný CA certifikát pre verifikáciu klienta
    if cafile:
        try:
            context.load_verify_locations(cafile=cafile)
            log(f"✓ CA certificate loaded: {cafile}", Colors.GREEN)
            
            if require_client_cert:
                context.verify_mode = ssl.CERT_REQUIRED
                log("✓ Client certificate verification: REQUIRED", Colors.GREEN)
            else:
                context.verify_mode = ssl.CERT_OPTIONAL
                log("✓ Client certificate verification: OPTIONAL", Colors.YELLOW)
        except Exception as e:
            log(f"✗ Failed to load CA certificate: {e}", Colors.RED)
            sys.exit(1)
    else:
        context.verify_mode = ssl.CERT_NONE
        log("⚠ Client certificate verification: DISABLED", Colors.YELLOW)
    
    # Bezpečnostné nastavenia
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    
    return context

def main():
    parser = argparse.ArgumentParser(description='TLS Test Server')
    parser.add_argument('--cert', required=True, help='Cesta k server certifikátu (server.crt)')
    parser.add_argument('--key', required=True, help='Cesta k server kľúču (server.key)')
    parser.add_argument('--ca', help='Cesta k CA certifikátu pre verifikáciu klientov (ca.crt)')
    parser.add_argument('--require-client-cert', action='store_true', 
                        help='Vyžadovať klientsky certifikát')
    parser.add_argument('--host', default='0.0.0.0', help='IP adresa (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=8443, help='Port (default: 8443)')
    
    args = parser.parse_args()
    
    # Vytvor SSL kontext
    ssl_context = create_ssl_context(
        args.cert, 
        args.key, 
        args.ca, 
        args.require_client_cert
    )
    
    # Vytvor socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        sock.bind((args.host, args.port))
        sock.listen(5)
        log(f"✓ Server listening on {args.host}:{args.port}", Colors.GREEN)
        log("=" * 60, Colors.BLUE)
        log("Waiting for connections...", Colors.BLUE)
        log("Press Ctrl+C to stop", Colors.BLUE)
        log("=" * 60, Colors.BLUE)
        
        client_counter = 0
        
        while True:
            try:
                conn, addr = sock.accept()
                
                # Wrap socket with TLS
                try:
                    tls_conn = ssl_context.wrap_socket(conn, server_side=True)
                    client_counter += 1
                    
                    # Spracuj klienta v novom vlákne
                    client_thread = threading.Thread(
                        target=handle_client, 
                        args=(tls_conn, addr, client_counter)
                    )
                    client_thread.daemon = True
                    client_thread.start()
                    
                except ssl.SSLError as e:
                    log(f"TLS handshake failed from {addr[0]}:{addr[1]} - {e}", Colors.RED)
                    conn.close()
                    
            except KeyboardInterrupt:
                log("\nShutting down server...", Colors.YELLOW)
                break
                
    except Exception as e:
        log(f"Server error: {e}", Colors.RED)
    finally:
        sock.close()
        log("Server stopped", Colors.YELLOW)

if __name__ == '__main__':
    main()