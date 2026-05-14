"""
Generic binary search for finding state changes across a range of slots.

Works by comparing the first and last slot in a range:
- If they match, all slots in between are assumed to have the same value.
- If they differ, the range is split in half and searched recursively.

The algorithm is parameterized by a `fetch_fn(slot) -> (count, ...)` callable,
so the same logic can be reused for exit queue, pending deposits, etc.
"""


def _find_changes_recursive(slots, cache, fetch_fn, label, debug, depth=0):
    indent = "    " * (depth + 1)

    if debug:
        if slots:
            print(f"{indent}[depth={depth}] Processing range: slots {slots[0]} to {slots[-1]} ({len(slots)} slots)")
        else:
            print(f"{indent}[depth={depth}] Empty slots list")

    if len(slots) <= 1:
        if slots and slots[0] not in cache:
            data = fetch_fn(slots[0])
            cache[slots[0]] = data
            if debug:
                if data[0] is not None:
                    print(f"{indent}  -> QUERY slot {slots[0]}: count={data[0]}, eth={data[1]:.3f} (single slot)")
                else:
                    print(f"{indent}  -> QUERY slot {slots[0]}: FAILED (single slot)")
        elif slots and debug:
            print(f"{indent}  -> CACHED slot {slots[0]}: count={cache[slots[0]][0]} (already queried)")
        return

    first_slot = slots[0]
    last_slot = slots[-1]

    # Get data for first and last slots (use cache if available)
    if first_slot not in cache:
        data = fetch_fn(first_slot)
        cache[first_slot] = data
        if debug:
            if data[0] is not None:
                print(f"{indent}  -> QUERY first slot {first_slot}: count={data[0]}, eth={data[1]:.3f}")
            else:
                print(f"{indent}  -> QUERY first slot {first_slot}: FAILED")
    elif debug:
        print(f"{indent}  -> CACHED first slot {first_slot}: count={cache[first_slot][0]}")

    if last_slot not in cache:
        data = fetch_fn(last_slot)
        cache[last_slot] = data
        if debug:
            if data[0] is not None:
                print(f"{indent}  -> QUERY last slot {last_slot}: count={data[0]}, eth={data[1]:.3f}")
            else:
                print(f"{indent}  -> QUERY last slot {last_slot}: FAILED")
    elif debug:
        print(f"{indent}  -> CACHED last slot {last_slot}: count={cache[last_slot][0]}")

    first_data = cache[first_slot]
    last_data = cache[last_slot]

    # Handle failed queries - mark all slots in range as failed
    if first_data[0] is None or last_data[0] is None:
        if debug:
            print(f"{indent}  => Query failed, marking all {len(slots)} slots in range as failed")
        for s in slots:
            if s not in cache:
                cache[s] = (None, None)
        return

    first_count = first_data[0]
    last_count = last_data[0]

    # If same count, no changes in this range (don't recurse)
    if first_count == last_count:
        if debug:
            print(f"{indent}  => SAME count ({first_count}) at both ends - no changes in range, skipping recursion")
        return

    # If adjacent slots with different values, we've found the boundary
    if len(slots) == 2:
        if debug:
            print(f"{indent}  => BOUNDARY FOUND between slot {first_slot} (count={first_count}) and {last_slot} (count={last_count})")
        return

    # Different and not adjacent - query middle and recurse both halves
    mid_idx = len(slots) // 2
    mid_slot = slots[mid_idx]

    if debug:
        print(f"{indent}  => DIFFERENT counts (first={first_count}, last={last_count}) - splitting at middle slot {mid_slot}")

    if mid_slot not in cache:
        data = fetch_fn(mid_slot)
        cache[mid_slot] = data
        if debug:
            if data[0] is not None:
                print(f"{indent}  -> QUERY middle slot {mid_slot}: count={data[0]}, eth={data[1]:.3f}")
            else:
                print(f"{indent}  -> QUERY middle slot {mid_slot}: FAILED")
    elif debug:
        print(f"{indent}  -> CACHED middle slot {mid_slot}: count={cache[mid_slot][0]}")

    # Recurse on left half [first, mid] and right half [mid, last]
    if debug:
        print(f"{indent}  => Recursing LEFT: slots {first_slot} to {mid_slot}")
    _find_changes_recursive(slots[:mid_idx + 1], cache, fetch_fn, label, debug, depth + 1)

    if debug:
        print(f"{indent}  => Recursing RIGHT: slots {mid_slot} to {last_slot}")
    _find_changes_recursive(slots[mid_idx:], cache, fetch_fn, label, debug, depth + 1)


def find_changes(slots, fetch_fn, label="", debug=False):
    """
    Use binary search to find state changes within a list of slots.

    Args:
        slots: List of slot numbers (must be sorted ascending)
        fetch_fn: Callable(slot) -> (count, value) where count is used
                  to detect changes. Return (None, None) to signal failure.
        label: Label for debug logging (e.g. "Exit Queue", "Pending Deposits")
        debug: Enable detailed logging

    Returns:
        dict mapping slot -> (count, value) for every slot in the input list.
    """
    if not slots:
        return {}

    if debug:
        print(f"\n  === {label} Binary Search Start ===")
        print(f"  Input: {len(slots)} slots from {slots[0]} to {slots[-1]}")

    cache = {}

    # Find all change points via binary search
    _find_changes_recursive(slots, cache, fetch_fn, label, debug, depth=0)

    if debug:
        print(f"\n  === Filling in gaps ===")
        print(f"  Queried {len(cache)} slots: {sorted(cache.keys())}")

    # Fill in all slots by propagating values forward from queried slots
    result = {}
    queried_slots = sorted(cache.keys())
    current_data = cache[queried_slots[0]]

    q_idx = 0
    for slot in slots:
        while q_idx < len(queried_slots) - 1 and queried_slots[q_idx + 1] <= slot:
            q_idx += 1
            current_data = cache[queried_slots[q_idx]]
            if debug:
                print(f"    Slot {slot}: switching to queried value from slot {queried_slots[q_idx]} (count={current_data[0]})")
        result[slot] = current_data

    if debug:
        # Show the final mapping summary
        unique_values = {}
        for s, data in result.items():
            count = data[0] if data[0] is not None else "FAILED"
            if count not in unique_values:
                unique_values[count] = []
            unique_values[count].append(s)

        print(f"\n  === Result Summary ===")
        for count, slot_list in sorted(unique_values.items(), key=lambda x: (isinstance(x[0], str), x[0] or 0)):
            if len(slot_list) <= 6:
                print(f"    count={count}: slots {slot_list}")
            else:
                print(f"    count={count}: slots {slot_list[0]}-{slot_list[-1]} ({len(slot_list)} slots)")
        print(f"  === {label} Binary Search End ===\n")

    return result
