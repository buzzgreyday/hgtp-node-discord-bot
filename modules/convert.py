import re
from hashlib import sha256
import base58
node_data = {
        "name": "test",
        "contact": "contact",
        "host": "157.245.121.245",
        "layer": 0,
        "publicPort": 9000,
        "p2pPort": None,
        "id": 	"05b84749ed796e85ec43852f8d87af489e6d5f6f481fe2c720be1898e85b27a8b582de12e47f16bf3332889c5318e601d5dce30841d7ee29128f692597b1217c"
}
def id_to_address():
    # pkcs prefix + 04 required bytes
    pkcs_prefix = "3056301006072a8648ce3d020106052b8104000a034200" + "04"

    output_nodeid = f"{node_data['id'][0:8]}...{node_data['id'][-8:]}"

    if len(node_data["id"]) == 128:
        node_id = f"{pkcs_prefix}{node_data['id']}"
    else:
        node_id = None

    node_id = sha256(bytes.fromhex(node_id)).hexdigest()
    node_id = base58.b58encode(bytes.fromhex(node_id)).decode()
    node_id = node_id[len(node_id)-36:]

    check_digits = re.sub('[^0-9]+', '', node_id)
    check_digit = 0
    for n in check_digits:
        check_digit += int(n)
        if check_digit > 9:
            check_digit = check_digit % 9

    wallet_address = f"DAG{check_digit}{node_id}"
    print(wallet_address)

