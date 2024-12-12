from datetime import datetime, timedelta
import sys
import time

# ========================================================
# for adding parameterized execution
def get_arg_value(arg):
        try:
            idx = sys.argv.index(arg)
            return sys.argv[idx + 1]
        except (ValueError, IndexError):
            return None

# ========================================================
# generate custom datetime string for batch operation
def generate_custom_datetime_format():
    """Generate a custom datetime format."""
    now = datetime.now()
    three_hours_ago = now - timedelta(days=1)
    custom_format = three_hours_ago.strftime("%Y%m%d%H") + "05"
    return custom_format


# ========================================================
# Return the current time to use for the next stage
def log_time(previous_time, label=""):
    current_time = time.time()
    elapsed_time = current_time - previous_time
    print(f"{label} - Time taken: {elapsed_time:.2f} seconds")
    return current_time  

# add this at the staring of the code
# script_start_time = time.time()

# can use multiple times for different stages
# process_time = log_time(script_start_time, "PROCESS_NAME-process completed")



# ========================================================