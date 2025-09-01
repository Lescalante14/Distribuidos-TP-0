import sys

def validate_args(args):
    if len(args) != 2:
        print("Usage: python generar-compose.py <output_file> <clients_count>")
        sys.exit(1)

    output_file = args[0]

    clients_count = args[1]

    if not clients_count.isdigit():
        print("Error: La cantidad de clientes debe ser un número entero")
        sys.exit(1)

    return output_file, clients_count

def write_services(f, clients_count):
    f.write("services:\n")
    write_server(f)
    write_clients(f, clients_count)

def write_server(f):
    f.write("  server:\n")
    f.write("    container_name: server\n")
    f.write("    image: server:latest\n")
    f.write("    entrypoint: python3 /main.py\n")
    f.write("    environment:\n")
    f.write("      - PYTHONUNBUFFERED=1\n")
    f.write("      - LOGGING_LEVEL=DEBUG\n")
    f.write("    networks:\n")
    f.write("      - testing_net\n")
    f.write("    volumes:\n")
    f.write("      - ./server/config.ini:/config.ini\n")

def write_clients(f, clients_count):
    for i in range(1, int(clients_count) + 1):
        f.write(f"  client{i}:\n")
        f.write(f"    container_name: client{i}\n")
        f.write(f"    image: client:latest\n")
        f.write("    entrypoint: /client\n")
        f.write("    environment:\n")
        f.write(f"      - CLI_ID={i}\n")
        f.write("      - CLI_LOG_LEVEL=DEBUG\n")
        f.write("    networks:\n")
        f.write("      - testing_net\n")
        f.write("    depends_on:\n")
        f.write("      - server\n")
        f.write("    volumes:\n")
        f.write("      - ./client/config.yaml:/config.yaml\n")

def write_networks(f):
    f.write("networks:\n")
    f.write("  testing_net:\n")
    f.write("    ipam:\n")
    f.write("      driver: default\n")
    f.write("      config:\n")
    f.write("        - subnet: 172.25.125.0/24\n")


def main(args):
    output_file, clients_count = validate_args(args)

    with open(output_file, "w") as f:
        f.write("name: tp0\n")
        write_services(f, clients_count)
        write_networks(f)

    print(output_file, clients_count)

if __name__ == "__main__":
    main(sys.argv[1:])