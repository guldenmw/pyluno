from pprint import pprint
import luno

# Instantiate streaming object.
lw = luno.classes.Stream()

# Start the connection.
lw.start()

# Create your old_ticker variable for storing the previous ticker value
# and start a while loop for continuous streaming.
old_ticker = {}
while True:
    # Get the latest ticker
    new_ticker = lw.ticker()

    # If you only want to see changes in price, compare the new value to the previous value.
    if new_ticker == old_ticker:
        continue
    pprint(new_ticker)

    # Set your previous value at the end of the loop.
    old_ticker = new_ticker
