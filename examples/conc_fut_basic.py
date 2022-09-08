import time
import concurrent.futures
import functools


def add_two(number):
    """Arbitrary function to add two to a number"""
    time.sleep(2)
    print(number + 2)


def divider(number, div_number):
    """Arbitrary function to divide number by div_number"""
    time.sleep(2)
    number_divided = int(number / div_number)
    print(number_divided)


if __name__ == "__main__":

    NUMBERS = [2, 4, 6, 8, 10]

    # Sequential for comparison
    start_seq = time.perf_counter()

    for number in NUMBERS:
        add_two(number)

    end_seq = time.perf_counter()
    print(f"sequential executed in {end_seq - start_seq} seconds")

    #  Concurrent futures
    start_conc = time.perf_counter()

    with concurrent.futures.ProcessPoolExecutor() as executor:
        numb_add_results = executor.map(add_two, NUMBERS)

    end_conc = time.perf_counter()
    print(f" parallel processs executed in {end_conc - start_conc} seconds")

    # Holiding a value constant when apply map to a list is also useful
    start_conc_par = time.perf_counter()

    with concurrent.futures.ProcessPoolExecutor() as executor:
        num_mult_results = executor.map(
            functools.partial(divider, div_number=2), NUMBERS
        )

    end_conc_par = time.perf_counter()
    print(
        f" parallel multiplication executed in {end_conc_par - start_conc_par} seconds"
    )

    # Can also use threading instead of process as an execution methof
    thread_conc_start = time.perf_counter()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        num_mult_results_thr = executor.map(
            functools.partial(divider, div_number=1), NUMBERS
        )

    thread_conc_end = time.perf_counter()
    print(
        f"parallel threading executed in {thread_conc_end - thread_conc_start} seconds"
    )
