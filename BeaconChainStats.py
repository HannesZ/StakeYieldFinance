import requests
import csv
import os
from dotenv import load_dotenv

load_dotenv()


# CSV file to store the results
CSV_FILENAME = "beacon_chain_queues_.csv"
# Access variables
BEACON_NODE_URL = os.getenv('BEACON_NODE_URL')

def get_validators_by_status(state_id="head", status="pending_queued"):
    # Query the Beacon chain API for validators with the specified status
    url = f"{BEACON_NODE_URL}/eth/v1/beacon/states/{state_id}/validators"
    params = {
        'status': status
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()["data"]
    else:
        print(f"Error fetching data for state {state_id} with status {status}: {response.status_code}")
        return []

def query_epochs(start_slot, end_slot, interval=1):
    CSV_FILENAME = f"beacon_chain_queues_{start_slot}_{end_slot}.csv"
        # Open the CSV file for writing
    with open(CSV_FILENAME, mode='w', newline='') as file:
        writer = csv.writer(file)
        
        # Write the header of the CSV file
        writer.writerow(["epoch", "slot", "active", "entry_queue", "exit_queue"])

        # Initialize variables to store the number of validators in each queue
        nr_exit_queue = 0
        initialized_exit_queue = False
        for slot in range(start_slot, end_slot + 1, interval):
            #print(f"\nQuerying slot {slot}...")

            if slot % 32 == 0 or not initialized_exit_queue:
                print(f"\n**************************** Epoch {int(slot/32)} ****************************")
                # Get validators that are in the entry queue (pending_queued)
                entry_queue = get_validators_by_status(state_id=slot, status="pending_queued")
                nr_entry_queue = len(entry_queue)

                # Get validators that are active
                active_ongoing = get_validators_by_status(state_id=slot, status="active_ongoing")
                nr_active_ongoing = len(active_ongoing)

                # Get validators that are in the exit queue (active_exiting, active_slashed)
                exit_queue = get_validators_by_status(state_id=slot, status="active_exiting,active_slashed")
                nr_exit_queue = len(exit_queue)
                initialized_exit_queue = True
            else:
                # Get validators that are in the exit queue (active_exiting, active_slashed)
                exit_queue = get_validators_by_status(state_id=slot, status="active_exiting,active_slashed")
                if len(exit_queue) != nr_exit_queue:
                    nr_active_ongoing = nr_active_ongoing + (nr_exit_queue - len(exit_queue))
                    nr_exit_queue = len(exit_queue)

            # Write the result to the CSV file
            writer.writerow([int(slot/32), slot, nr_active_ongoing, nr_entry_queue, nr_exit_queue])

            # Print the result for each slot
            print(f"Slot {slot} - Entry Queue: {nr_entry_queue} validators, Exit Queue: {nr_exit_queue} validators")

if __name__ == "__main__":
    # Example: Query epochs 100000 to 100010
    end_slot =  12464282
    start_slot = 12364282  #  end_slot-1000
    query_epochs(start_slot, end_slot)

    print(f"Results have been saved to {CSV_FILENAME}")