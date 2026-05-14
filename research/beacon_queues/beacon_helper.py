from web3 import Web3

def resolve_el_block_hash(rpc_url: str, block_hash_hex: str):
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    blk = w3.eth.get_block(block_hash_hex)
    return int(blk.number), int(blk.timestamp)
