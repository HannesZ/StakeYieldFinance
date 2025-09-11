# https://eth2book.info/capella/part2/incentives/issuance/#the-base-reward-per-increment

EFFECTIVE_BALANCE_INCREMENT = 1_000_000_000  # 1 ETH in Gwei
BASE_REWARD_FACTOR = 64
EPOCH =   32  # slots per epoch
SLOT =    12  # seconds per slot

EPOCHS_PER_YEAR = 365.25 * 24 * 60 * 60 / (EPOCH * SLOT)


def Gwei(value: int) -> int:
    return value * 10**9

def integer_squareroot(value: int) -> int:
    return int(value**0.5)  


def get_base_reward_per_increment(state: BeaconState) -> Gwei:
    return Gwei(EFFECTIVE_BALANCE_INCREMENT * BASE_REWARD_FACTOR // integer_squareroot(get_total_active_balance(state)))

def APR (state: BeaconState) -> float:
    base_reward = get_base_reward_per_increment(state)
    return EPOCHS_PER_YEAR * base_reward