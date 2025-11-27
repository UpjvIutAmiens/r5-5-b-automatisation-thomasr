import socket
import threading

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 5000                 # port this script listens on
CHUNK_SIZE_LINES = 200             # how many CSV lines per chunk

BDD_HOST = "bdd"                   # Docker service name (Docker DNS)
BDD_PORT = 6000                    # C# service port
# ----------------------------------------------------------------------


def listen_for_csv():
    """Main TCP listener receiving a CSV file."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((LISTEN_HOST, LISTEN_PORT))
    server.listen(5)

    print(f"[LISTENING] Waiting for CSV sender on {LISTEN_HOST}:{LISTEN_PORT}")

    while True:
        client_socket, addr = server.accept()
        print(f"[CONNECTED] Sender connected from {addr}")

        threading.Thread(
            target=handle_csv_sender,
            args=(client_socket,)
        ).start()


def handle_csv_sender(client_socket: socket.socket):
    """Receives CSV data, splits into chunks, sends to bdd service."""
    csv_data = b""

    try:
        while True:
            buf = client_socket.recv(4096)
            if not buf:
                break
            csv_data += buf
    finally:
        client_socket.close()

    csv_text = csv_data.decode("utf-8")

    print("[INFO] CSV received. Length:", len(csv_text))

    chunks = split_into_chunks(csv_text)

    print(f"[INFO] Split into {len(chunks)} chunks")

    # Send chunks sequentially
    for idx, chunk in enumerate(chunks, start=1):
        print(f"[INFO] Sending chunk {idx}/{len(chunks)}...")

        result = send_chunk_to_bdd(chunk)

        if result != "SUCCESS":
            print(f"[ERROR] Chunk {idx} failed with response:", result)
            return

        print(f"[INFO] Chunk {idx} success")

    print("[FINAL] All chunks sent successfully!")


def split_into_chunks(csv_text: str):
    """Splits CSV text into chunks by number of lines."""
    lines = csv_text.splitlines()
    chunks = []

    for i in range(0, len(lines), CHUNK_SIZE_LINES):
        chunk_lines = lines[i:i + CHUNK_SIZE_LINES]
        chunks.append("\n".join(chunk_lines))

    return chunks


def send_chunk_to_bdd(chunk: str):
    """Sends a single chunk to the bdd service and waits for a response."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((BDD_HOST, BDD_PORT))

        # Send length header then the chunk
        chunk_bytes = chunk.encode("utf-8")

        header = f"{len(chunk_bytes)}\n".encode("utf-8")
        sock.sendall(header + chunk_bytes)

        # Wait for response (SUCCESS or ERROR)
        response = sock.recv(1024).decode("utf-8").strip()
        sock.close()

        return response

    except Exception as e:
        return f"ERROR: {e}"


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------

if __name__ == "__main__":
    listen_for_csv()
